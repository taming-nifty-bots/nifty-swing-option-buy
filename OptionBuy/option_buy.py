from pymongo import MongoClient
import os
import sys
from tamingnifty import connect_definedge as edge
from tamingnifty import utils as util
import requests
import time
import zipfile
from retry import retry
import io
import datetime 
from datetime import timedelta
from dateutil import parser
import pandas as pd
from slack_sdk import WebClient
pd.set_option('display.max_rows', None)
from dotenv import (  # pip install python-dotenv
    find_dotenv,
    load_dotenv,
)


dotenv_file: str = find_dotenv()
load_dotenv(dotenv_file)

slack_channel = "niftyswing"
CONNECTION_STRING = os.environ.get('CONNECTION_STRING')  # Mongo Connection
user_name = os.environ.get('user_name')
trade_start_time = parser.parse("09:16:00").time()
trade_end_time = parser.parse("15:28:00").time()
slack_client = WebClient(token=os.environ.get('slack_token'))
quantity = os.environ.get('quantity')
instrument_name = os.environ.get('instrument_name')
lot_size = 65
total_lots = int(quantity) // lot_size
# --- Fixed-stop design borrowed from BNF Intraday (replaces the VIX-scaled SL/RR + trailing) ---
STOP_LOSS_PER_LOT = 750        # flat rupee stop per lot (no VIX scaling)
REWARD_RISK_RATIO = 4.0        # runner target = RR x |stop|; with 1-lot runner this yields Rs.3000 at 4R
HALF_BOOK_AT_R = 1             # book HALF the position at +1R, then move the runner stop to breakeven (cost)
DAILY_MAX_LOSS_PER_LOT = -1200  # daily loss ceiling per lot; for 2 lots this is -2400 (~1.92% of Rs.1.25L capital)
# Partial booking splits the position into two equal halves, so an EVEN lot count (>= 2) is required.
if int(quantity) % lot_size != 0 or total_lots < 2 or total_lots % 2 != 0:
    print(f"[config] quantity={quantity} resolves to {total_lots} lot(s) of {lot_size}; "
          f"this strategy books half the position and needs an EVEN lot count (>= 2). Exiting.")
    raise SystemExit(1)
half_quantity = (total_lots // 2) * lot_size   # lot-aligned half booked at +1R
mongo_client = MongoClient(CONNECTION_STRING)

supertrend_collection = mongo_client['Bots']["supertrend"]

                                                   

strategies_collection_name = instrument_name.lower() + "_swing_" + user_name
orders_collection_name = "orders_" + instrument_name.lower() + "_swing_" + user_name

# trades collection
strategies = mongo_client['Bots'][strategies_collection_name]
orders = mongo_client['Bots'][orders_collection_name]  # orders collection

# @retry(tries=5, delay=5, backoff=2)
def place_buy_order(symbol, qty):
    #conn = edge.login_to_integrate(True)
    # io = edge.IntegrateOrders(conn)
    # # Get today's date in the required format (e.g., '16-05-2024')
    # order = io.place_order(
    #     exchange=conn.EXCHANGE_TYPE_NFO,
    #     order_type=conn.ORDER_TYPE_BUY,
    #     price=0,
    #     price_type=conn.PRICE_TYPE_MARKET,
    #     product_type=conn.PRODUCT_TYPE_NORMAL,
    #     quantity=qty,
    #     tradingsymbol=symbol,
    # )
    # order_id = order['order_id']
    # order = get_order_by_order_id(conn, order_id)
    # print(f"Order Status: {order['order_status']}")
    # if order['order_status'] != "COMPLETE":
    #     time.sleep(2)
    #     order = get_order_by_order_id(conn, order_id)
    #     print(f"Order Status after retry: {order['order_status']}")
    # if order['order_status'] != "COMPLETE":
    #     util.notify(f"Order Message: {order['message']}",slack_client=slack_client, slack_channel=slack_channel)
    #     util.notify(f"Order Failed: {order}",slack_client=slack_client, slack_channel=slack_channel)
    #     orders.insert_one(order)
    #     raise Exception("Error in placing order - " +
    #                 str(order['message']))
    order = {
        "order_id": "25052900010716",
        "last_fill_qty": qty,
        "tradingsymbol": symbol,
        "token": "40470",
        "quantity": qty,
        "price_type": "MARKET",
        "product_type": "NORMAL",
        "order_entry_time": datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S'),
        "order_status": "COMPLETE",
        "order_type": "BUY",
        "exchange_orderid": "1100000127824637",
        "message": " ",
        "pending_qty": "0",
        "price": "0.00",
        "exchange_time": "29-05-2025 13:08:22",
        "average_traded_price": edge.get_option_price('NFO', symbol, (datetime.datetime.now() - timedelta(days=7)), datetime.datetime.today(), 'min'),
        "exchange": "NFO",
        "filled_qty": qty,
        "disclosed_quantity": "0",
        "validity": "DAY",
        "ordersource": "TRTP"
    }
    print(f"Order placed: {order}")
    util.notify(f"Order placed: {order}",slack_client=slack_client, slack_channel=slack_channel)
    orders.insert_one(order)
    return order


# @retry(tries=5, delay=5, backoff=2)
def place_sell_order(symbol, qty):
    #conn = edge.login_to_integrate(True)
    # io = edge.IntegrateOrders(conn)
    # order = io.place_order(
    #     exchange=conn.EXCHANGE_TYPE_NFO,
    #     order_type=conn.ORDER_TYPE_SELL,
    #     price=0,
    #     price_type=conn.PRICE_TYPE_MARKET,
    #     product_type=conn.PRODUCT_TYPE_NORMAL,
    #     quantity=qty,
    #     tradingsymbol=symbol,
    # )
    # order_id = order['order_id']
    # order = get_order_by_order_id(conn, order_id)
    # print(f"Order Status: {order['order_status']}")
    # if order['order_status'] != "COMPLETE":
    #     time.sleep(2)
    #     order = get_order_by_order_id(conn, order_id)
    #     print(f"Order Status after retry: {order['order_status']}")
    # if order['order_status'] != "COMPLETE":
    #     util.notify(f"Order Message: {order['message']}",slack_client=slack_client, slack_channel=slack_channel)
    #     util.notify(f"Order Failed: {order}",slack_client=slack_client, slack_channel=slack_channel)
    #     orders.insert_one(order)
    #     raise Exception("Error in placing order - " +
    #                 str(order['message']))
    order = {
        "order_id": "25052900010716",
        "last_fill_qty": qty,
        "tradingsymbol": symbol,
        "token": "40470",
        "quantity": qty,
        "price_type": "MARKET",
        "product_type": "NORMAL",
        "order_entry_time": datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S'),
        "order_status": "COMPLETE",
        "order_type": "SELL",
        "exchange_orderid": "1100000127824637",
        "message": " ",
        "pending_qty": "0",
        "price": "0.00",
        "exchange_time": "29-05-2025 13:08:22",
        "average_traded_price": edge.get_option_price('NFO', symbol, (datetime.datetime.now() - timedelta(days=7)), datetime.datetime.today(), 'min'),
        "exchange": "NFO",
        "filled_qty": qty,
        "disclosed_quantity": "0",
        "validity": "DAY",
        "ordersource": "TRTP"
    }
    util.notify(f"Order placed: {order}",slack_client=slack_client, slack_channel=slack_channel)
    orders.insert_one(order)
    return order


# @retry(tries=5, delay=5, backoff=2)
def get_order_by_order_id(conn: edge.ConnectToIntegrate, order_id):
    io = edge.IntegrateOrders(conn)
    print(f"Getting order by order ID: {order_id}")
    order = io.order(order_id)
    print(order)
    return order



# @retry(tries=5, delay=5, backoff=2)
def load_csv_from_zip(url='https://app.definedgesecurities.com/public/allmaster.zip'):
    column_names = ['SEGMENT', 'TOKEN', 'SYMBOL', 'TRADINGSYM', 'INSTRUMENT TYPE', 'EXPIRY', 'TICKSIZE', 'LOTSIZE', 'OPTIONTYPE', 'STRIKE', 'PRICEPREC', 'MULTIPLIER', 'ISIN', 'PRICEMULT', 'UnKnown']
    # Send a GET request to download the zip file
    response = requests.get(url)
    response.raise_for_status()  # This will raise an exception for HTTP errors
    # Open the zip file from the bytes-like object
    with zipfile.ZipFile(io.BytesIO(response.content)) as thezip:
        # Extract the name of the first CSV file in the zip archive
        csv_name = thezip.namelist()[0]
        # Extract and read the CSV file into a pandas DataFrame
        with thezip.open(csv_name) as csv_file:
            df = pd.read_csv(csv_file, header=None, names=column_names, low_memory=False, on_bad_lines='skip')
    df = df[(df['SEGMENT'] == 'NFO') & (df['INSTRUMENT TYPE'] == 'OPTIDX')]
    df = df[(df['SYMBOL'].str.startswith('NIFTY'))]
    df = df[df['SYMBOL'] == 'NIFTY']
    df['EXPIRY'] = df['EXPIRY'].astype(str).apply(lambda x: x.zfill(8))
    df['EXPIRY'] = pd.to_datetime(df['EXPIRY'], format='%d%m%Y', errors='coerce')
    df = df.sort_values(by='EXPIRY', ascending=True)
    # Return the loaded DataFrame
    return df



# @retry(tries=5, delay=5, backoff=2)
def get_option_symbol(strike=19950, option_type = "PE" ):
    df = load_csv_from_zip()
    df = df[df['TRADINGSYM'].str.contains(str(strike))]
    df = df[df['OPTIONTYPE'].str.match(option_type)]
    # Get the current date
    current_date = datetime.datetime.now()

    df= df[(df['EXPIRY'] > (current_date + timedelta(days=3)))]
    df = df.head(1)
    print("Getting options Symbol...")
    print(f"Symbol: {df['TRADINGSYM'].values[0]} , Expiry: {df['EXPIRY'].values[0]}")
    return df['TRADINGSYM'].values[0], df['EXPIRY'].values[0]

def buy_put(strike=19950, pcr=None):
    option_type = "PE"
    util.notify(f"Buy Strike: {strike}",slack_client=slack_client, slack_channel=slack_channel)
    buy_strike_symbol, expiry = get_option_symbol(strike, option_type)
    print(expiry)
    expiry = str(expiry)
    expiry = parser.parse(expiry).date()
    print(expiry)
    buy_order = place_buy_order(buy_strike_symbol, quantity)
    if buy_order['order_status'] == "COMPLETE":
         util.notify("Buy order placed successfully!",slack_client=slack_client, slack_channel=slack_channel)
    else:
        util.notify(f"Buy order failed: {buy_order['message']}",slack_client=slack_client, slack_channel=slack_channel)
        raise Exception("Error in placing buy order - " + str(buy_order['message']))
    long_option_cost = buy_order['average_traded_price']
    util.notify("PUT Option Long successfull!",slack_client=slack_client, slack_channel=slack_channel)
    record_details_in_mongo(buy_strike_symbol, "Bearish", expiry, long_option_cost, pcr)

def buy_call(strike=19950, pcr=None):
    option_type = "CE"
    util.notify(f"Buy Strike: {strike}",slack_client=slack_client, slack_channel=slack_channel)
    buy_strike_symbol, expiry = get_option_symbol(strike, option_type)
    print(expiry)
    expiry = str(expiry)
    expiry = parser.parse(expiry).date()
    print(expiry)
    buy_order = place_buy_order(buy_strike_symbol, quantity)
    if buy_order['order_status'] == "COMPLETE":
         util.notify("Buy order placed successfully!",slack_client=slack_client, slack_channel=slack_channel)
    else:
        util.notify(f"Buy order failed: {buy_order['message']}",slack_client=slack_client, slack_channel=slack_channel)
        raise Exception("Error in placing buy order - " + str(buy_order['message']))
    long_option_cost = buy_order['average_traded_price']
    util.notify("CALL Option Long successfull!",slack_client=slack_client, slack_channel=slack_channel)
    record_details_in_mongo(buy_strike_symbol, "Bullish", expiry, long_option_cost, pcr)

def record_details_in_mongo(buy_strike_symbol, trend, expiry, long_option_cost, pcr=None):
    conn = edge.login_to_integrate()
    vix = edge.fetch_ltp(conn, 'NSE', 'India VIX')  # logged for analysis only (no longer drives the SL)
    # Fixed-stop design borrowed from BNF Intraday (no VIX scaling, no trailing):
    #   stop = -1500/lot, book half at +0.5R, runner stop -> breakeven (cost), runner target = 2R (Rs.3000).
    stop_loss = -STOP_LOSS_PER_LOT * total_lots
    target = round(abs(stop_loss) * REWARD_RISK_RATIO, 2)
    half_book_trigger = round(HALF_BOOK_AT_R * abs(stop_loss), 2)
    util.notify(
        f"Risk (fixed): SL={round(stop_loss, 2)}, Target={target}, RR={REWARD_RISK_RATIO}, "
        f"HalfBook>= {half_book_trigger} ({HALF_BOOK_AT_R}R)",
        slack_client=slack_client, slack_channel=slack_channel)

    if instrument_name == "NIFTY":
        trading_symbol = "Nifty 50"
    elif instrument_name == "BANKNIFTY":
        trading_symbol = "Nifty Bank"
    strategy = {
        'instrument_name': instrument_name,
        'India Vix': vix,
        'pcr': pcr,
        'quantity': int(quantity),
        'lot_size': lot_size,
        'long_exit_price': 0,
        'strategy_state': 'active',
        'entry_date': str(datetime.datetime.now().date()),
        'exit_date': '',
        'entry_day_of_week': datetime.datetime.now().strftime('%A'),
        'exit_day_of_week': '',
        'trend': trend,
        'long_option_symbol': buy_strike_symbol,
        'long_option_cost': long_option_cost,
        'stop_loss': round(stop_loss, 2),
        'target': round(target, 2),
        'rr': REWARD_RISK_RATIO,
        # --- half-booking tracking (book half at +0.5R, then run the remainder at breakeven) ---
        'half_book_at_r': HALF_BOOK_AT_R,
        'half_book_trigger': half_book_trigger,
        'half_booked': False,
        'half_booked_pnl': 0,
        'half_book_price': 0,
        'half_book_time': '',
        'half_book_date': '',
        'runner_quantity': int(quantity),
        'total_investment': round(long_option_cost * int(quantity), 2),
        'entry_time': datetime.datetime.now().strftime('%H:%M'),
        'exit_time': '',
        'instrument_close': edge.fetch_ltp(conn, 'NSE', trading_symbol),
        'expiry': str(expiry),
        'running_pnl': 0,
        'exit_reason': '',
        'pnl': '',
        'net_pnl': '',
        'max_pnl_reached': 0,
        'min_pnl_reached': 0
    }
    strategies.insert_one(strategy)

def calculate_pnl(quantity, entry, exit):
    pnl = float(quantity) * (float(exit) - float(entry))
    print(f"Realized Gains: {round(pnl, 2)}")
    return round(pnl, 2)

@retry(tries=5, delay=5, backoff=2)
def close_active_positions(reason, ltp=None):
    print(f"Closing active positions {instrument_name}")
    util.notify(f"Closing active positions {instrument_name}",slack_client=slack_client, slack_channel=slack_channel)
    active_strategies = strategies.find({'strategy_state': {'$in': ['active', 'partial']}})
    for strategy in active_strategies:
        runner_quantity = int(strategy.get('runner_quantity', strategy['quantity']))
        sell_order = place_sell_order(strategy['long_option_symbol'], runner_quantity)
        util.notify("Long option leg closed",slack_client=slack_client, slack_channel=slack_channel)
        exit_price = ltp if ltp else sell_order['average_traded_price']
        half_booked_pnl = float(strategy.get('half_booked_pnl', 0) or 0)
        runner_pnl = calculate_pnl(runner_quantity, strategy['long_option_cost'], exit_price)
        gross_pnl = round(half_booked_pnl + runner_pnl, 2)
        net_pnl = round(gross_pnl - (105 * total_lots), 2)  # Deducting brokerage and taxes (flat, conservative)
        strategies.update_one({'_id': strategy['_id']}, {'$set': {
            'strategy_state': 'closed',
            'exit_date': str(datetime.datetime.now().date()),
            'exit_time': datetime.datetime.now().strftime('%H:%M'),
            'exit_day_of_week': datetime.datetime.now().strftime('%A'),
            'long_exit_price': exit_price,
            'exit_reason': reason,
            'pnl': gross_pnl,
            'net_pnl': net_pnl,
        }})
        util.notify(f"Exit [{reason}] half={half_booked_pnl} runner={runner_pnl} gross={gross_pnl} net={net_pnl}",slack_client=slack_client, slack_channel=slack_channel)
        print(f"Exit [{reason}] gross={gross_pnl} net={net_pnl}")
        time.sleep(120)  # Wait before looking for the next entry signal
    return

@retry(tries=5, delay=5, backoff=2)
def get_current_price(strategy, start=None):
    if start is None:
        days_ago = datetime.datetime.now() - timedelta(days=7)
        start = days_ago.replace(hour=9, minute=15, second=0, microsecond=0)
    return edge.get_option_price('NFO', strategy['long_option_symbol'], start, datetime.datetime.today(), 'min')


@retry(tries=5, delay=5, backoff=2)
def book_half(strategy, option_price=None):
    """Book the lot-aligned half of the position at >= +0.5R; the runner then sits at breakeven (cost).
    Records the half-book leg so the DB tracks partial-exit -> full-exit for the trade."""
    full_quantity = int(strategy['quantity'])
    sell_order = place_sell_order(strategy['long_option_symbol'], half_quantity)
    half_book_price = option_price if option_price else sell_order['average_traded_price']
    half_booked_pnl = calculate_pnl(half_quantity, strategy['long_option_cost'], half_book_price)
    runner_quantity = full_quantity - half_quantity
    strategies.update_one({'_id': strategy['_id']}, {'$set': {
        'half_booked': True,
        'strategy_state': 'partial',
        'half_booked_pnl': round(half_booked_pnl, 2),
        'half_book_price': half_book_price,
        'half_book_time': datetime.datetime.now().strftime('%H:%M'),
        'half_book_date': str(datetime.datetime.now().date()),
        'runner_quantity': runner_quantity,
    }})
    util.notify(f"HALF BOOK: sold {half_quantity} @ {half_book_price}, half_booked_pnl={round(half_booked_pnl, 2)}; "
                f"runner {runner_quantity} now at breakeven (cost {strategy['long_option_cost']})",
                slack_client=slack_client, slack_channel=slack_channel)
    print(f"HALF BOOK {half_quantity}@{half_book_price} half_booked_pnl={round(half_booked_pnl, 2)} runner={runner_quantity}")
    return

# @retry(tries=5, delay=5, backoff=2)
def main():
    print(str(datetime.time(hour=9, minute=31)))
    util.notify(f"{instrument_name} Option Buy bot kicked off",slack_client=slack_client, slack_channel=slack_channel)
    print(f"{instrument_name} Positional bot kicked off")
    days_ago = datetime.datetime.now() - timedelta(days=7)
    start = days_ago.replace(hour=9, minute=15, second=0, microsecond=0)
    
    # Track the time when the last notification was sent
    last_notification_time = datetime.datetime.now()
    while True:
        try:
            current_time = datetime.datetime.now().time()
            notification_time = datetime.datetime.now()

            # Calculate elapsed time since the last notification
            elapsed_time = notification_time - last_notification_time
            print(f"elapsed time: {elapsed_time}")
            if elapsed_time >= timedelta(hours=1):
                util.notify(message=f"{instrument_name} Weekly option Selling bot is Alive!", slack_client=slack_client, slack_channel=slack_channel)
                util.notify(message=f"current time from {instrument_name}WeeklyOptionSelling: {current_time}", slack_client=slack_client, slack_channel=slack_channel)
                # Update the last notification time
                last_notification_time = notification_time
                
            print(f"current time: {current_time}")
            if current_time > trade_start_time:
                print("Trading Window is active.")
                if strategies.count_documents({'strategy_state': {'$in': ['active', 'partial']}}) > 0:
                    active_strategies = strategies.find(
                        {'strategy_state': {'$in': ['active', 'partial']}})
                    for strategy in active_strategies:
                        option_price = get_current_price(strategy, start)
                        option_cost = float(strategy['long_option_cost'])
                        full_quantity = int(strategy['quantity'])

                        # Positional exits kept intact (apply in BOTH phases): Friday weekend + 2-day time SL
                        is_friday_exit = (datetime.datetime.now().strftime('%A') == "Friday"
                                          and current_time >= datetime.time(hour=15, minute=15))
                        is_time_exit = False
                        entry_date = strategy.get('entry_date')
                        if entry_date:
                            try:
                                entry_date_obj = parser.parse(entry_date).date()
                                days_active = (datetime.datetime.now().date() - entry_date_obj).days
                                if days_active >= 2 and current_time >= datetime.time(hour=14, minute=30):
                                    is_time_exit = True
                            except Exception as e:
                                print(f"Error parsing entry_date: {e}")

                        if not strategy.get('half_booked', False):
                            # Phase 1: full position on the fixed stop; book half at +0.5R
                            position_pnl = calculate_pnl(full_quantity, option_cost, option_price)
                            strategies.update_one({'_id': strategy['_id']}, {'$set': {'running_pnl': position_pnl}})
                            if position_pnl > float(strategy.get('max_pnl_reached', 0) or 0):
                                strategies.update_one({'_id': strategy['_id']}, {'$set': {'max_pnl_reached': position_pnl}})
                            if position_pnl < float(strategy.get('min_pnl_reached', 0) or 0):
                                strategies.update_one({'_id': strategy['_id']}, {'$set': {'min_pnl_reached': position_pnl}})

                            if position_pnl <= strategy['stop_loss']:
                                util.notify(f"SL HIT! Current PnL: {position_pnl}", slack_client=slack_client, slack_channel=slack_channel)
                                close_active_positions("SL HIT")
                                break
                            elif position_pnl >= strategy['half_book_trigger']:
                                util.notify(f"Half-book level reached ({strategy.get('half_book_at_r', HALF_BOOK_AT_R)}R). PnL: {position_pnl}", slack_client=slack_client, slack_channel=slack_channel)
                                book_half(strategy)
                            elif is_friday_exit:
                                util.notify("Friday Exit Time Reached", slack_client=slack_client, slack_channel=slack_channel)
                                close_active_positions("Exit Before Weekend")
                                break
                            elif is_time_exit:
                                util.notify("Time based Stop Loss Hit (2 days active)", slack_client=slack_client, slack_channel=slack_channel)
                                close_active_positions("Time based Stop Loss (2 days active)")
                                break
                        else:
                            # Phase 2: runner (half) with stop at breakeven (cost) and a fixed 2R target
                            runner_quantity = int(strategy['runner_quantity'])
                            runner_pnl = calculate_pnl(runner_quantity, option_cost, option_price)
                            half_booked_pnl = float(strategy.get('half_booked_pnl', 0) or 0)
                            strategies.update_one({'_id': strategy['_id']}, {'$set': {'running_pnl': round(half_booked_pnl + runner_pnl, 2)}})
                            runner_target_price = option_cost + float(strategy['target']) / full_quantity

                            if option_price <= option_cost:
                                util.notify(f"Runner breakeven stop hit @ {option_price} (cost {option_cost}). Booking remainder.", slack_client=slack_client, slack_channel=slack_channel)
                                close_active_positions("Partial+BE")
                                break
                            elif option_price >= runner_target_price:
                                util.notify(f"Runner TARGET hit @ {option_price} (>= {round(runner_target_price, 2)}).", slack_client=slack_client, slack_channel=slack_channel)
                                close_active_positions("Partial+Target")
                                break
                            elif is_friday_exit:
                                util.notify("Friday Exit Time Reached (runner). Closing remainder.", slack_client=slack_client, slack_channel=slack_channel)
                                close_active_positions("Partial+Weekend")
                                break
                            elif is_time_exit:
                                util.notify("Time based Stop Loss Hit (runner, 2 days active). Closing remainder.", slack_client=slack_client, slack_channel=slack_channel)
                                close_active_positions("Partial+Time")
                                break
                else:
                    # Daily-loss ceiling: enforce "no more than 2% loss in a single day" before any new entry
                    today = str(datetime.datetime.now().date())
                    daily_max_loss_limit = DAILY_MAX_LOSS_PER_LOT * total_lots
                    realized_today = 0.0
                    for s in strategies.find({'exit_date': today, 'strategy_state': 'closed'}):
                        try:
                            realized_today += float(s.get('net_pnl', 0) or 0)
                        except (TypeError, ValueError):
                            continue
                    if realized_today <= daily_max_loss_limit:
                        util.notify(f"Daily kill switch active. Realized net PnL today: {realized_today}, limit: {daily_max_loss_limit}. No new entries today.", slack_client=slack_client, slack_channel=slack_channel)
                        return
                    if (realized_today + (-STOP_LOSS_PER_LOT * total_lots)) < daily_max_loss_limit:
                        util.notify(f"No further entries today (would breach daily loss limit). Realized: {realized_today}, limit: {daily_max_loss_limit}.", slack_client=slack_client, slack_channel=slack_channel)
                        return

                    atm_next_straddle = supertrend_collection.find_one({"_id": "atm_next_straddle"})
                    atm_prev_straddle = supertrend_collection.find_one({"_id": "atm_prev_straddle"})

                    if atm_next_straddle['AFT'] == True and current_time > datetime.time(hour=9, minute=31):
                        buy_call(strike=atm_next_straddle['strike'], pcr=atm_next_straddle['pcr'])
                    elif atm_prev_straddle['AFT'] == True and current_time > datetime.time(hour=9, minute=31):
                        buy_put(strike=atm_prev_straddle['strike'], pcr=atm_prev_straddle['pcr'])
                    else:
                        print("Waiting for the entry signal...")
        except Exception as e:
            util.notify(f"Exception occurred: {str(e)}", slack_client=slack_client, slack_channel=slack_channel)
        
        if current_time > trade_end_time:
            util.notify("Closing Bell, Bot will exit now",slack_client=slack_client, slack_channel=slack_channel)
            return   
        time.sleep(10)
if __name__ == "__main__":
    main()
