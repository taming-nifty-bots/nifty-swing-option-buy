import math
import os
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
from dotenv import find_dotenv, load_dotenv
from pymongo import MongoClient
from slack_sdk import WebClient
from tamingnifty import connect_dhan as dhan
from tamingnifty import ta
from tamingnifty import utils as util

VERSION = "premium-renko-levels-1pct-40-20-2026-10-28"


def now_ist():
    return datetime.now(ZoneInfo("Asia/Kolkata")).replace(tzinfo=None)


def settings():
    for key in ("CONNECTION_STRING", "user_name", "slack_token", "DHAN_CLIENT_ID",
                "DHAN_PIN", "DHAN_TOTP"):
        if not os.environ.get(key, "").strip():
            raise ValueError("Missing environment variable: " + key)
    if os.environ.get("instrument_name", "NIFTY") != "NIFTY":
        raise ValueError("This strategy supports NIFTY only")
    if os.environ.get("live_trading", "false").strip().lower() != "false":
        raise ValueError("This strategy is paper-only; live_trading must be false")
    return dict(user=os.environ["user_name"], mongo=os.environ["CONNECTION_STRING"],
                slack=os.environ["slack_token"])


def notify(client, message):
    print(message, flush=True)
    try:
        util.notify(message=message, slack_client=client, slack_channel="niftyswing")
    except Exception as error:
        print("Slack notification failed: " + util.exception_detail(error), flush=True)


def signal_id(user, day):
    return f"NIFTY_premium_renko_{user}_{day}"


def locked_contracts(master, index, day):
    frame = index.copy()
    frame["datetime"] = pd.to_datetime(frame["datetime"])
    spot = frame.loc[frame["datetime"] == pd.Timestamp(day + " 09:30")]
    if len(spot) != 1:
        raise ValueError("Need the exact Nifty 09:30 minute open to lock ATM")
    price = float(spot.iloc[0]["open"])
    if not math.isfinite(price) or price <= 0:
        raise ValueError("Invalid Nifty 09:30 price")
    strike = int(util.round_to_nearest(price, base=50))
    options = master.loc[(master["UNDERLYING_SYMBOL"] == "NIFTY")
                         & (master["INSTRUMENT"] == "OPTIDX")
                         & (master["EXCH_ID"] == "NSE")
                         & (master["STRIKE_PRICE"] == strike)].copy()
    options["expiry_date"] = pd.to_datetime(options["SM_EXPIRY_DATE"]).dt.normalize()
    options = options.loc[options["expiry_date"] > pd.Timestamp(day) + pd.Timedelta(days=6)]
    if options.empty:
        raise ValueError("No ATM contracts strictly beyond six calendar days")
    expiry = options["expiry_date"].min()
    contracts = {}
    for kind in ("CE", "PE"):
        matches = options.loc[(options["expiry_date"] == expiry) & (options["OPTION_TYPE"] == kind)]
        if len(matches) != 1:
            raise ValueError("Expected one exact dated ATM " + kind + " contract")
        row = matches.iloc[0]
        contracts[kind] = dict(security_id=str(int(row["SECURITY_ID"])),
                               symbol=str(row["DISPLAY_NAME"]), lot_size=int(row["LOT_SIZE"]))
    if contracts["CE"]["lot_size"] != contracts["PE"]["lot_size"]:
        raise ValueError("CE/PE lot sizes disagree")
    return dict(strike=strike, spot_0930_open=price, expiry=str(expiry.date()),
                dte=(expiry.date() - pd.Timestamp(day).date()).days, contracts=contracts)


def session_candles(candles, day, now):
    frame = candles.copy()
    required = ["datetime", "open", "high", "low", "close", "volume"]
    if not set(required).issubset(frame.columns) or frame.empty:
        raise ValueError("Missing contract OHLCV history")
    frame["datetime"] = pd.to_datetime(frame["datetime"])
    frame = frame.loc[(frame["datetime"] >= pd.Timestamp(day) - pd.Timedelta(days=14))
                      & (frame["datetime"] < pd.Timestamp(now).floor("min"))
                      & (frame["datetime"].dt.time >= pd.Timestamp("09:15").time())
                      & (frame["datetime"].dt.time <= pd.Timestamp("15:29").time())]
    frame = frame.sort_values("datetime").reset_index(drop=True)
    if frame.empty or frame["datetime"].duplicated().any():
        raise ValueError("Empty or duplicate completed normal-session candles")
    if frame[required].isna().any().any():
        raise ValueError("Missing OHLCV values")
    for field in required[1:]:
        if not frame[field].map(math.isfinite).all():
            raise ValueError("Non-finite " + field)
    if (frame[["open", "high", "low", "close"]] <= 0).any().any() or (frame["volume"] < 0).any():
        raise ValueError("Nonpositive OHLC or negative volume")
    if ((frame["high"] < frame[["open", "close", "low"]].max(axis=1)).any()
            or (frame["low"] > frame[["open", "close", "high"]].min(axis=1)).any()):
        raise ValueError("Invalid OHLC ordering")
    # A rounded zero step would hang the shared percentage-Renko builder.
    if (frame[["open", "close"]] < 1).any().any():
        raise ValueError("Option premium too small for safe percentage-Renko rounding")
    return frame


def option_levels(candles, day, now):
    bars = session_candles(candles, day, now)
    today = bars.loc[bars["datetime"].dt.strftime("%Y-%m-%d") == day]
    if today.empty:
        raise ValueError("No completed option candles today")
    data_asof = today.iloc[-1]["datetime"]
    if pd.Timestamp(now).floor("min") - data_asof > pd.Timedelta(minutes=2):
        raise ValueError("Stale option candles; no fresh levels can be published")
    bricks = ta.convert_to_renko(brick_size=1, df=bars.copy())
    if bricks.empty:
        raise ValueError("Not enough history to form Renko")
    bricks["high40"] = bricks["high"].shift(1).rolling(40).max()
    bricks["low20"] = bricks["low"].shift(1).rolling(20).min()
    snapshots = bricks.drop_duplicates("datetime", keep="last")
    observations = []
    latest = None
    for row in snapshots.itertuples():
        latest = dict(brick_time=str(row.datetime), close=float(row.close),
                      high40=float(row.high40) if pd.notna(row.high40) else None,
                      low20=float(row.low20) if pd.notna(row.low20) else None)
        if str(row.datetime)[:10] == day:
            observations.append(latest)
    # Level history lets the executor recover a missed poll without moving decisions here.
    return dict(**latest, data_asof=str(data_asof), history_bars=len(bars),
                observations=observations)


def publish(conn, collection, master, user, now):
    day = str(now.date())
    key = signal_id(user, day)
    doc = collection.find_one({"_id": key})
    if doc is None:
        index = dhan.fetch_candles(conn, 13, "IDX_I", "INDEX",
                                   now.replace(hour=0, minute=0, second=0, microsecond=0), now)
        locked = locked_contracts(master, index, day)
        collection.update_one({"_id": key}, {"$setOnInsert": dict(
            day=day, strategy_version=VERSION, **locked)}, upsert=True)
        doc = collection.find_one({"_id": key})
    if doc["strategy_version"] != VERSION:
        raise ValueError("Incompatible daily signal; do not overwrite its contract lock")
    options = {}
    start = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=14)
    for kind in ("CE", "PE"):
        security = doc["contracts"][kind]["security_id"]
        bars = dhan.fetch_candles(conn, security, "NSE_FNO", "OPTIDX", start, now)
        options[kind] = option_levels(bars, day, now)
    collection.update_one({"_id": key}, {"$set": dict(
        options=options, updated_at=str(now), broker="DHAN")})
    return collection.find_one({"_id": key})


def main():
    load_dotenv(find_dotenv())
    config = settings()
    slack = WebClient(token=config["slack"])
    client = MongoClient(config["mongo"])
    collection = client["Bots"]["supertrend"]
    master = None
    master_day = None
    last_minute = None
    last_error = None
    last_alert = now_ist()
    heartbeat = now_ist()
    notify(slack, f"Nifty premium Renko signal started: {VERSION}; Dhan, paper-only")
    try:
        while True:
            now = now_ist()
            try:
                if now.strftime("%H:%M") >= "15:16":
                    break
                minute = now.replace(second=0, microsecond=0)
                if now.weekday() < 5 and now.strftime("%H:%M") >= "09:31" and minute != last_minute:
                    conn = dhan.login_to_dhan(slack_channel="niftyswing")
                    if master_day != now.date():
                        master = dhan.load_instruments()
                        master_day = now.date()
                    doc = publish(conn, collection, master, config["user"], now)
                    print(f"{doc['day']} ATM {doc['strike']} expiry {doc['expiry']}", flush=True)
                    for kind in ("CE", "PE"):
                        levels = doc["options"][kind]
                        print(f"{kind}: close={levels['close']} high40={levels['high40']} "
                              f"low20={levels['low20']} asof={levels['data_asof']}", flush=True)
                    last_minute = minute
                if last_error is not None:
                    notify(slack, "Nifty premium signal recovered")
                    last_error = None
                if now - heartbeat >= timedelta(hours=1):
                    notify(slack, "Nifty premium signal alive; waiting/publishing completed minutes")
                    heartbeat = now
            except Exception as error:
                message = util.exception_detail(error)
                print(message, flush=True)
                if message != last_error or now - last_alert >= timedelta(minutes=15):
                    notify(slack, "Nifty premium signal error: " + message)
                    last_error, last_alert = message, now
            time.sleep(10)
    finally:
        client.close()


if __name__ == "__main__":
    main()
