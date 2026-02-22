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
total_lots = int(quantity) / lot_size
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
    vix = edge.fetch_ltp(conn, 'NSE', 'India VIX')
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
    'trend' : trend,
    'long_option_symbol' : buy_strike_symbol,
    'long_option_cost' : long_option_cost,
    'stop_loss' : max((-1250 * total_lots), round(-0.5 * long_option_cost * int(quantity), 2)),
    'trailing_stop_loss' : max((-1250 * total_lots), round(-0.5 * long_option_cost * int(quantity), 2)),
    'target' : min((5000 * total_lots), round(2 * long_option_cost * int(quantity), 2)),
    'total_investment' : round(long_option_cost * int(quantity), 2),
    'entry_time' : datetime.datetime.now().strftime('%H:%M'),
    'exit_time' : '',
    'instrument_close' : edge.fetch_ltp(conn, 'NSE', trading_symbol),
    'expiry' : str(expiry),
    'running_pnl' : 0,
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
def close_active_positions(reason):
    print(f"Closing active positions {instrument_name}")
    util.notify(f"Closing active positions {instrument_name}",slack_client=slack_client, slack_channel=slack_channel)
    active_strategies = strategies.find({'strategy_state': 'active'})
    for strategy in active_strategies:
        sell_order = place_sell_order(strategy['long_option_symbol'], strategy['quantity'])
        util.notify("Long option leg closed",slack_client=slack_client, slack_channel=slack_channel)
        strategies.update_one({'_id': strategy['_id']}, {'$set': {'strategy_state': 'closed'}})
        strategies.update_one({'_id': strategy['_id']}, {'$set': {'exit_date': str(datetime.datetime.now().date())}})
        strategies.update_one({'_id': strategy['_id']}, {'$set': {'exit_time': datetime.datetime.now().strftime('%H:%M')}})
        strategies.update_one({'_id': strategy['_id']}, {'$set': {'exit_day_of_week': datetime.datetime.now().strftime('%A')}})
        strategies.update_one({'_id': strategy['_id']}, {'$set': {'long_exit_price': sell_order['average_traded_price']}})
        strategies.update_one({'_id': strategy['_id']}, {'$set': {'exit_reason': reason}})
        pnl = calculate_pnl(strategy['quantity'], strategy['long_option_cost'], sell_order['average_traded_price'])
        util.notify(f"Realized Gains: {round(pnl, 2)}",slack_client=slack_client, slack_channel=slack_channel)
        strategies.update_one({'_id': strategy['_id']}, {'$set': {'pnl': pnl}})
        strategies.update_one({'_id': strategy['_id']}, {'$set': {'net_pnl': (pnl-(105*total_lots))}})  # Deducting brokerage and taxes
        util.notify(f"Net PnL after brokerage and taxes: {round((pnl-(105*total_lots)), 2)}",slack_client=slack_client, slack_channel=slack_channel)
        print(f"Realized Gains: {round(pnl, 2)}")
        print(f"Net PnL after brokerage and taxes: {round((pnl-(105*total_lots)), 2)}")
        time.sleep(120)  # Wait for 10 seconds before looking for next entry signal
    return

@retry(tries=5, delay=5, backoff=2)
def get_pnl(strategy, start=None):
    if start is None:
        days_ago = datetime.datetime.now() - timedelta(days=7)
        start = days_ago.replace(hour=9, minute=15, second=0, microsecond=0)
    long_option_cost = edge.get_option_price('NFO', strategy['long_option_symbol'], start, datetime.datetime.today(), 'min')
    current_pnl = calculate_pnl(strategy['quantity'], strategy['long_option_cost'], long_option_cost)
    strategies.update_one({'_id': strategy['_id']}, {'$set': {'running_pnl': current_pnl}})
    return current_pnl

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
                if strategies.count_documents({'strategy_state': 'active'}) > 0:
                    active_strategies = strategies.find(
                        {'strategy_state': 'active'})
                    for strategy in active_strategies:
                        pnl = get_pnl(strategy, start)
                        if strategy['max_pnl_reached'] < pnl:                         
                            strategies.update_one({'_id': strategy['_id']}, {'$set': {'max_pnl_reached': pnl}})
                            strategies.update_one({'_id': strategy['_id']}, {'$set': {'trailing_stop_loss': strategy['stop_loss'] + pnl}})
                            util.notify(f"New Max PnL reached: {pnl}, Updated Trailing SL: {strategy['stop_loss'] + pnl}",slack_client=slack_client, slack_channel=slack_channel)
                        
                        if strategy['min_pnl_reached'] > pnl:
                            strategies.update_one({'_id': strategy['_id']}, {'$set': {'min_pnl_reached': pnl}})

                        if pnl <= strategy['trailing_stop_loss']:
                            util.notify(f"SL HIT! Current PnL: {pnl}",slack_client=slack_client, slack_channel=slack_channel)
                            close_active_positions("SL HIT")
                            break

                        if pnl >= strategy['target']:
                            util.notify(f"Target HIT! Current PnL: {pnl}",slack_client=slack_client, slack_channel=slack_channel)
                            close_active_positions("Target HIT")
                            break

                        if datetime.datetime.now().strftime('%A') == "Friday" and current_time >= datetime.time(hour=15, minute=15):
                            util.notify("Friday Exit Time Reached", slack_client=slack_client, slack_channel=slack_channel)
                            close_active_positions("Exit Before Weekend")
                            break

                        print(str(datetime.datetime.now().date()))
                        # Time-based stop loss: exit if position active for 2 days
                        entry_date = strategy.get('entry_date')
                        if entry_date:
                            try:
                                entry_date_obj = parser.parse(entry_date).date()
                                days_active = (datetime.datetime.now().date() - entry_date_obj).days
                                if days_active >= 2 and current_time >= datetime.time(hour=14, minute=30):
                                    util.notify("Time based Stop Loss Hit (2 days active)", slack_client=slack_client, slack_channel=slack_channel)
                                    close_active_positions("Time based Stop Loss (2 days active)")
                                    break
                            except Exception as e:
                                print(f"Error parsing entry_date: {e}")
                else:
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
