import os
import math
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
from dotenv import find_dotenv, load_dotenv
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from slack_sdk import WebClient
from tamingnifty import connect_dhan as dhan
from tamingnifty import utils as util
from tamingnifty.charges import money, order_charges, sum_charges

VERSION = "premium-renko-levels-1pct-40-20-2026-10-28"
SIGNAL_ID = "nifty_options_renko"
ACTIVE_STATES = ["pending", "active", "partial", "closing"]


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


def trade_id(user, day):
    return f"{VERSION}:{user}:{day}"


def valid_price(price):
    value = float(price)
    if not math.isfinite(value) or value <= 0:
        raise ValueError("Expected a finite positive market price")
    return value


def validate_quantity(quantity, lot_size):
    qty = int(quantity)
    lot = int(lot_size)
    if qty != float(quantity) or lot <= 0 or qty <= 0 or qty % lot:
        raise ValueError(f"quantity must be a positive multiple of contract lot size {lot}")
    return qty


def fresh_signal(doc, user, now):
    if (doc is None or doc.get("_id") != SIGNAL_ID
            or doc.get("strategy_version") != VERSION):
        raise ValueError("Missing or incompatible daily signal")
    if pd.Timestamp(doc["updated_at"]).date() != now.date():
        raise ValueError("Signal is not from today")
    age = (now - pd.Timestamp(doc["updated_at"]).to_pydatetime()).total_seconds()
    if not 0 <= age <= 90:
        raise ValueError("Signal publisher is stale or future-dated")
    for kind in ("CE", "PE"):
        age = (pd.Timestamp(now).floor("min") - pd.Timestamp(doc[kind + "_data_asof"])).total_seconds()
        if not 60 <= age <= 120:
            raise ValueError("Signal option candles are stale or incomplete")
    return doc


def first_decision(doc):
    found = []
    for kind in ("CE", "PE"):
        brick = pd.Timestamp(doc[kind + "_brick_time"])
        high = doc[kind + "_last_40_high"]
        if (brick >= pd.Timestamp(doc["updated_at"]).normalize() + pd.Timedelta(hours=9, minutes=30)
                and high is not None and doc[kind + "_close"] > high):
            found.append((doc[kind + "_data_asof"], kind))
    if not found:
        return None
    found.sort()
    if len(found) == 2:
        return dict(status="skip", reason="both_latest_levels_above_high40", signal_time=found[0][0])
    return dict(status="entry", option_type=found[0][1], signal_time=found[0][0])


def entry_is_current(decision, now):
    if decision is None or decision["status"] != "entry":
        return False
    signal = pd.Timestamp(decision["signal_time"])
    earliest = signal + pd.Timedelta(minutes=1)
    return (signal.date() == now.date()
            and pd.Timestamp(str(now.date()) + " 09:30") <= signal
            < pd.Timestamp(str(now.date()) + " 15:14")
            and earliest <= pd.Timestamp(now) < earliest + pd.Timedelta(minutes=1)
            and pd.Timestamp(now) < pd.Timestamp(str(now.date()) + " 15:15"))


def exit_reason(trade, doc, now):
    if now.strftime("%H:%M") >= "15:15" or trade["entry_date"] < str(now.date()):
        return "15:15 EOD" if trade["entry_date"] == str(now.date()) else "Overdue intraday position"
    if doc is None:
        raise ValueError("Need a fresh contract signal before evaluating channel exit")
    kind = trade["option_type"]
    if (trade["security_id"] != doc[kind + "_security_id"]
            or trade["expiry"] != doc["expiry"]):
        raise ValueError("Active trade and signal contracts disagree")
    if (pd.Timestamp(trade["entry_timestamp"]).floor("min") <= pd.Timestamp(doc[kind + "_brick_time"])
            < pd.Timestamp(now).floor("min")
            and doc[kind + "_last_20_low"] is not None
            and doc[kind + "_close"] < doc[kind + "_last_20_low"]):
        return "20 brick low breakout"
    return None


def paper_order(conn, orders, key, contract, quantity, side, now):
    existing = orders.find_one({"_id": key})
    if existing is not None:
        return existing
    price = valid_price(dhan.get_option_ltp(conn, contract["security_id"]))
    filled_at = now_ist()
    if side == "BUY":
        deadline = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
        if not now <= filled_at < deadline or filled_at.strftime("%H:%M") >= "15:15":
            raise ValueError("Quote arrived outside the entry minute; no paper buy recorded")
    order = dict(_id=key, orderId=key, orderStatus="TRADED", transactionType=side,
                 securityId=contract["security_id"], tradingSymbol=contract["symbol"],
                 exchangeSegment="NSE_FNO", quantity=quantity, filledQty=quantity,
                 averageTradedPrice=price, createTime=str(filled_at), simulated=True,
                 strategy_version=VERSION)
    try:
        orders.insert_one(order.copy())
    except DuplicateKeyError:
        order = orders.find_one({"_id": key})
    return order


def claim_entry(strategies, doc, user, quantity, now):
    decision = first_decision(doc)
    if not entry_is_current(decision, now):
        return None
    # Include historical records: never place a second trade because its era differs.
    if strategies.find_one({"entry_date": str(now.date())}) is not None:
        return None
    kind = decision["option_type"]
    contract = dict(security_id=doc[kind + "_security_id"], symbol=doc[kind + "_symbol"],
                    lot_size=doc[kind + "_lot_size"])
    qty = validate_quantity(quantity, contract["lot_size"])
    trade = dict(_id=trade_id(user, str(now.date())), strategy_version=VERSION,
                 instrument_name="NIFTY", broker="DHAN", simulated=True,
                 strategy_state="pending", entry_date=str(now.date()), entry_signal=decision["signal_time"],
                 option_type=kind, trend="Bullish" if kind == "CE" else "Bearish",
                 strike=doc["STRIKE"], expiry=doc["expiry"], dte=doc["dte"],
                 security_id=contract["security_id"], long_option_symbol=contract["symbol"],
                 lot_size=contract["lot_size"], quantity=qty, signal_doc_id=doc["_id"],
                 entry_timestamp=str(now), entry_time=now.strftime("%H:%M:%S"))
    try:
        strategies.insert_one(trade.copy())
    except DuplicateKeyError:
        return None
    return trade


def open_pending(conn, strategies, orders, trade, now):
    key = trade["_id"] + ":BUY"
    existing = orders.find_one({"_id": key})
    decision = dict(status="entry", signal_time=trade["entry_signal"])
    if existing is None and not entry_is_current(decision, now):
        strategies.update_one({"_id": trade["_id"], "strategy_state": "pending"},
                              {"$set": dict(strategy_state="aborted",
                                             exit_reason="Missed next-minute entry window")})
        raise ValueError("Pending paper entry expired; daily slot remains consumed")
    contract = dict(security_id=trade["security_id"], symbol=trade["long_option_symbol"])
    order = paper_order(conn, orders, key, contract, trade["quantity"], "BUY", now)
    price = valid_price(order["averageTradedPrice"])
    entry_fees = order_charges("options", "BUY", price, trade["quantity"])
    strategies.update_one({"_id": trade["_id"], "strategy_state": "pending"}, {"$set": dict(
        strategy_state="active", long_option_cost=price, entry_order_id=key,
        entry_timestamp=order["createTime"], entry_time=order["createTime"][11:19],
        total_investment=money(price * trade["quantity"]), charges=sum_charges(entry_fees),
        pnl=0.0, net_pnl=-entry_fees["total"], running_pnl=0.0)})


def close_trade(conn, strategies, orders, trade, reason, now):
    contract = dict(security_id=trade["security_id"], symbol=trade["long_option_symbol"])
    key = trade["_id"] + ":SELL"
    order = paper_order(conn, orders, key, contract, trade["quantity"], "SELL", now)
    price = valid_price(order["averageTradedPrice"])
    gross = money(trade["quantity"] * (price - trade["long_option_cost"]))
    fees = sum_charges(
        order_charges("options", "BUY", trade["long_option_cost"], trade["quantity"]),
        order_charges("options", "SELL", price, trade["quantity"]))
    strategies.update_one({"_id": trade["_id"], "strategy_state": {"$in": ["active", "closing"]}},
                          {"$set": dict(strategy_state="closed", long_exit_price=price,
                                        exit_date=order["createTime"][:10],
                                        exit_time=order["createTime"][11:19],
                                        exit_timestamp=order["createTime"], exit_order_id=key,
                                        exit_reason=reason, pnl=gross, running_pnl=gross,
                                        charges=fees, net_pnl=money(gross - fees["total"]))})


def step(conn, strategies, orders, signals, user, quantity, now):
    active = list(strategies.find({"strategy_state": {"$in": ACTIVE_STATES}}))
    if len(active) > 1:
        raise ValueError("Multiple open/reserved swing positions; manual reconciliation required")
    if active:
        trade = active[0]
        if trade.get("strategy_version") != VERSION or trade.get("simulated") is not True:
            raise ValueError("Legacy or non-paper open position: reconcile before running the new strategy")
        if trade["strategy_state"] == "pending":
            open_pending(conn, strategies, orders, trade, now)
            return "Pending entry reconciled"
        reason = exit_reason(trade, None, now) if (
            now.strftime("%H:%M") >= "15:15" or trade["entry_date"] < str(now.date())) else None
        if reason is None:
            doc = signals.find_one({"_id": SIGNAL_ID})
            doc = fresh_signal(doc, user, now)
            reason = exit_reason(trade, doc, now)
        if reason:
            close_trade(conn, strategies, orders, trade, reason, now)
            return "Position closed: " + reason
        price = valid_price(dhan.get_option_ltp(conn, trade["security_id"]))
        running = money(trade["quantity"] * (price - trade["long_option_cost"]))
        strategies.update_one({"_id": trade["_id"], "strategy_state": "active"},
                              {"$set": {"running_pnl": running, "last_mark": str(now)}})
        return None
    if now.weekday() >= 5 or now.strftime("%H:%M") < "09:31" or now.strftime("%H:%M") >= "15:15":
        return None
    if strategies.find_one({"entry_date": str(now.date())}) is not None:
        return None
    doc = signals.find_one({"_id": SIGNAL_ID})
    doc = fresh_signal(doc, user, now)
    if doc.get("skip_day"):
        return None
    decision = first_decision(doc)
    if decision is not None and decision["status"] == "skip":
        signals.update_one({"_id": doc["_id"], "updated_at": doc["updated_at"]},
                           {"$set": {"skip_day": True}})
        return "Both latest option levels exceed high40; skip today"
    trade = claim_entry(strategies, doc, user, quantity, now)
    if trade is not None:
        open_pending(conn, strategies, orders, trade, now)
        return f"Paper buy {trade['long_option_symbol']}, quantity {trade['quantity']}"
    return None


def main():
    load_dotenv(find_dotenv())
    config = settings()
    quantity = os.environ.get("quantity")
    if quantity is None:
        raise ValueError("Missing quantity; configure the existing executor job explicitly")
    validate_quantity(quantity, 1)
    slack = WebClient(token=config["slack"])
    client = MongoClient(config["mongo"])
    strategies = client["Bots"][f"nifty_swing_{config['user']}"]
    orders = client["Bots"][f"orders_nifty_swing_{config['user']}"]
    signals = client["Bots"]["supertrend"]
    last_error = None
    last_alert = now_ist()
    heartbeat = now_ist()
    notify(slack, f"Nifty premium executor started: {VERSION}; PAPER ONLY")
    try:
        while True:
            now = now_ist()
            try:
                if now.strftime("%H:%M") >= "15:16" and strategies.count_documents(
                        {"strategy_state": {"$in": ACTIVE_STATES}}) == 0:
                    break
                if now.strftime("%H:%M") >= "09:15":
                    conn = dhan.login_to_dhan(slack_channel="niftyswing")
                    message = step(conn, strategies, orders, signals, config["user"], quantity, now_ist())
                    if message:
                        notify(slack, message)
                if last_error is not None:
                    notify(slack, "Nifty premium executor recovered")
                    last_error = None
                if now - heartbeat >= timedelta(hours=1):
                    notify(slack, "Nifty premium executor alive; paper mode, one trade/day")
                    heartbeat = now
            except Exception as error:
                message = util.exception_detail(error)
                print(message, flush=True)
                if message != last_error or now - last_alert >= timedelta(minutes=15):
                    notify(slack, "Nifty premium executor error: " + message)
                    last_error, last_alert = message, now
            time.sleep(10)
    finally:
        client.close()


if __name__ == "__main__":
    main()
