# package import statement

from SmartApi import SmartConnect
import pyotp
from logzero import logger
import os
from dotenv import load_dotenv
import pandas as pd
import requests
import time as time
from datetime import datetime
import yfinance as yf
import datetime as dt
import concurrent.futures
from dateutil.relativedelta import relativedelta
import numpy as np
import pytz
from flask import Flask, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)



load_dotenv()

# Credentials for login
api_key = os.getenv("API_KEY")
username = os.getenv("CLIENT_CODE")
pwd = os.getenv("PASSCODE")
# print(pwd)
smartApi = SmartConnect(api_key)
try:
    token = os.getenv("TOKEN")
    totp = pyotp.TOTP(token).now()
except Exception as e:
    logger.error("Invalid Token: The provided token is not valid.")
    raise e

correlation_id = "abcde"
data = smartApi.generateSession(username, pwd, totp)

if data['status'] == False:
    logger.error(data)

else:
    # login api call
    # logger.info(f"You Credentials: {data}")
    authToken = data['data']['jwtToken']
    refreshToken = data['data']['refreshToken']
    # fetch the feedtoken
    feedToken = smartApi.getfeedToken()
    # fetch User Profile
    res = smartApi.getProfile(refreshToken)
    smartApi.generateToken(refreshToken)
    res=res['data']['exchanges']


# Fetch all the scripts that is tradable
url = 'https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json'
d = requests.get(url).json()
token_df = pd.DataFrame.from_dict(d)

stock_symbols = token_df[(token_df['exch_seg'] == 'NFO') & (token_df['instrumenttype'] == 'OPTSTK')].loc[:,'name'].unique().tolist()


tickers = [f"{stock}.NS" for stock in stock_symbols]  # Convert to Yahoo format
data = yf.Tickers(" ".join(tickers))  # Fetch all at once

sector_market_cap = {}
for stock in stock_symbols:
    ticker_data = data.tickers[f"{stock}.NS"]
    info = ticker_data.info
    equity_sector = info.get('sector', 'Sector info not available')
    equity_marketcap = info.get('marketCap', 0) / 1e7  # Market cap in crores
    sector_market_cap[stock] = [equity_sector, equity_marketcap]


equity_data = token_df[(token_df['exch_seg'] == "NSE") & (token_df['instrumenttype'] == '')]
equity_data.set_index('name', inplace=True)
nfo_data = token_df[(token_df['exch_seg']=="NFO") & (token_df['instrumenttype'] == 'OPTSTK')].drop(['tick_size','instrumenttype','exch_seg'],axis=1).sort_values(by='name')


def get_monthly_expiry(df, equity_data, names):

    # Process dates once outside the loop
    df['expiry'] = pd.to_datetime(df['expiry'], format='%d%b%Y', errors='coerce').dt.date

    exp = sorted(df['expiry'].unique())
    td = dt.date.today()

    exp = [pd.Timestamp(x).date() for x in exp]


    # Find the closest expiry (zrdexp)
    zrdexp = min(exp, key=lambda x: abs((x - td).days))

    # Check if today is in the last week of the month
    last_day_of_month = dt.date(td.year, td.month + 1, 1) - dt.timedelta(days=1)
    last_week_start = last_day_of_month - dt.timedelta(days=7)

    include_next_month = td >= last_week_start

    # Get next month's expiry if applicable
    next_month_expiry = None
    if include_next_month:
        next_month = td + relativedelta(months=1)
        next_month_expiries = [e for e in exp if e.month == next_month.month and e.year == next_month.year]
        if next_month_expiries:
            next_month_expiry = min(next_month_expiries)

    # Pre-filter data by expiry dates
    if next_month_expiry:
        df_filtered = df[df['expiry'].isin([zrdexp, next_month_expiry])].copy()

    else:
        df_filtered = df[df['expiry'] == zrdexp].copy()

    # Convert strike to numeric once
    df_filtered['strike'] = pd.to_numeric(df_filtered['strike'], errors='coerce')

    # Pre-filter CE and PE options
    ce_options = df_filtered[df_filtered['symbol'].str.endswith('CE')]
    print(f"Length of CE is : {len(ce_options)}")
    pe_options = df_filtered[df_filtered['symbol'].str.endswith('PE')]
    print(f"Length of PE is : {len(pe_options)}")

    # Create equity token mapping once
    equity_token_mapping = {}

    # Precompute LTPs to avoid redundant API calls
    ltp_dict = {}

    # Function to process a single name
    def process_name(name):
        try:
            token = equity_data.loc[name, 'token']
            if isinstance(token, pd.Series):
                token = token.iloc[0]

            equity_token_mapping[name] = token

            # Cache LTP values to reduce API calls
            if token not in ltp_dict:
                time.sleep(1)
                ltp_data = smartApi.ltpData('NSE', name, token)
                # time.sleep(3)
                ltp_dict[token] = ltp_data['data']['ltp'] * 100

            ltp = ltp_dict[token]

            # Selecting CE options for current month
            ce_near_curr = ce_options[(ce_options['name'] == name) & (ce_options['expiry'] == zrdexp) & (ce_options['strike'] <= ltp)].sort_values(by='strike', ascending=False).head(1)
            ce_gt_curr = ce_options[(ce_options['name'] == name) & (ce_options['expiry'] == zrdexp) & (ce_options['strike'] > ltp)].sort_values(by='strike').head(10)

            # Selecting PE options for current month
            pe_near_curr = pe_options[(pe_options['name'] == name) & (pe_options['expiry'] == zrdexp) & (pe_options['strike'] >= ltp)].sort_values(by='strike').head(1)
            pe_lt_curr = pe_options[(pe_options['name'] == name) & (pe_options['expiry'] == zrdexp) & (pe_options['strike'] < ltp)].sort_values(by='strike', ascending=False).head(10)

            # If next month expiry is available, fetch its options
            if next_month_expiry:
                ce_near_next = ce_options[(ce_options['name'] == name) & (ce_options['expiry'] == next_month_expiry) & (ce_options['strike'] <= ltp)].sort_values(by='strike', ascending=False).head(1)
                ce_gt_next = ce_options[(ce_options['name'] == name) & (ce_options['expiry'] == next_month_expiry) & (ce_options['strike'] > ltp)].sort_values(by='strike').head(10)

                pe_near_next = pe_options[(pe_options['name'] == name) & (pe_options['expiry'] == next_month_expiry) & (pe_options['strike'] >= ltp)].sort_values(by='strike').head(1)
                pe_lt_next = pe_options[(pe_options['name'] == name) & (pe_options['expiry'] == next_month_expiry) & (pe_options['strike'] < ltp)].sort_values(by='strike', ascending=False).head(10)

                # Merge both expiry sets
                ce = pd.concat([ce_near_curr, ce_gt_curr, ce_near_next, ce_gt_next])
                pe = pd.concat([pe_near_curr, pe_lt_curr, pe_near_next, pe_lt_next])
            else:
                # Only current month expiry data
                ce = pd.concat([ce_near_curr, ce_gt_curr])
                pe = pd.concat([pe_near_curr, pe_lt_curr])

            # Combine results
            return pd.concat([ce, pe])
        except Exception as e:
            print(f"Error processing {name}: {e}")
            return pd.DataFrame()

    # Use multithreading for API calls (the bottleneck)
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(process_name, name): name for name in names}
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if not result.empty:
                results.append(result)

    # Combine all results
    combined_df = pd.concat(results, ignore_index=True).sort_values(by=['name'])

    return combined_df, zrdexp, equity_token_mapping

options, exp, equity_token_mapping = get_monthly_expiry(nfo_data,equity_data,stock_symbols)


def get_change(token):
    """Fetches the last year's data in one API call and extracts required dates."""

    today = dt.date.today()
    one_year_ago = today - relativedelta(months=14)

    # Fetch entire year's data in a single call
    try:
        historic_data = smartApi.getCandleData({
            "exchange": "NSE",
            "symboltoken": token,
            "interval": "ONE_DAY",
            "fromdate": one_year_ago.strftime("%Y-%m-%d 09:00"),
            "todate": today.strftime("%Y-%m-%d 09:00"),
        })
        time.sleep(1)  # Sleep to avoid hitting rate limits
    except Exception as e:
        print(f"Error fetching data for token {token}: {e}")
        return None

    # Check if valid data was returned
    if "data" not in historic_data or not historic_data["data"]:
        return None

    # Convert API response to a dictionary {date: closing_price}
    price_data = {entry[0][:10]: entry[4] for entry in historic_data["data"]}  # Extract closing price

    # Required historical dates
    offsets = {"one_month": 1, "three_months": 3, "six_months": 6, "twelve_months": 12}
    historical_prices = {}

    for label, months_ago in offsets.items():
        target_date = today - relativedelta(months=months_ago)

        # Adjust to previous business day if necessary
        while target_date.strftime("%Y-%m-%d") not in price_data:
          target_date -= dt.timedelta(days=1)  # Move back one day until a valid date is found


        historical_prices[label] = price_data.get(target_date.strftime("%Y-%m-%d"), None)

    return [
        historical_prices["one_month"],
        historical_prices["three_months"],
        historical_prices["six_months"],
        historical_prices["twelve_months"],
    ]

change_data = {}
for token in equity_token_mapping.values():
  change_data[token] = get_change(token)

# Map the underlying symbol to the equity token
options["equity_token"] = options["name"].map(equity_token_mapping)

# Group options by their corresponding equity token
equity_to_option_map = options.groupby("equity_token")["token"].apply(list).to_dict()


merged_data = {
    token: {
        "static": options.loc[options["token"] == token].to_dict(orient="records")[0],
        "equity": None
    }
    for token in options["token"].tolist()
}

option_data_final = {
}

test_data = pd.DataFrame()

pars = {
    "datatype":"PercPriceGainers",
    "expirytype":"NEAR"
}
test = smartApi.gainersLosers(pars)['data']

df = pd.DataFrame(test)

# Convert time to Indian Standard Time
IST = pytz.timezone('Asia/Kolkata')

# Define the column headers with unique names
HEADERS = [
    "Timestamp", "Name", "Expiry", "Call_option", "CMP", "Strike", "LTP", "Lot_size",
    "Premium_Rate", "Cover", "Volume", "High_price", "Low_price",
    "OI_change_percent", "High_52_week", "Low_52_week", "Open_price", "High_price_day_eq", "Low_price_day_eq",
    "Price_change_percent", "Sector", "Market_cap",
    "One_month_change_%", "Three_month_change_%",
    "Six_month_change_%", "Twelve_month_change_%", "Close_price"
]


@app.route("/data", methods=["GET"])
def fetch_all_data():
    
    rows = []  # Local buffer for this batch

    for token, details in merged_data.items():
        try:
            static_data = details.get('static', {})
            equity_data = details.get('equity', {})

            option_info = option_data_final.get(token, {})

            sector_info = sector_market_cap.get(static_data.get('name'), ["Unknown", 0])
            change_values = change_data.get(equity_data.get('token'), [None, None, None, None])

            last_traded_price = equity_data.get('last_traded_price', 0) / 100
            closed_price = equity_data.get('closed_price', 1) / 100

            row = [
                datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"),
                static_data.get('name', 'Unknown'),
                static_data.get('expiry', datetime.now(IST)).strftime("%Y-%m-%d"),
                static_data.get('symbol', 'XX')[-2:],
                last_traded_price,
                static_data.get('strike', 0) / 100,
                option_info.get('last_traded_price', 0) / 100,
                static_data.get('lotsize', 0),
                round(((option_info.get('last_traded_price', 0) / 100) / (static_data.get('strike', 1) / 100)) * 100, 1),
                100 - round(((static_data.get('strike', 1) / 100) / (last_traded_price or 1)) * 100, 1),
                (option_info.get('volume_trade_for_the_day', 0) * 2) / 1000,
                option_info.get('high_price_of_the_day', 0) / 100,
                option_info.get('low_price_of_the_day', 0) / 100,
                option_info.get('open_interest_change_percentage', 0) / 1e7,
                equity_data.get('52_week_high_price', 0) / 100,
                equity_data.get('52_week_low_price', 0) / 100,
                equity_data.get('open_price_of_the_day', 0) / 100,
                equity_data.get('high_price_of_the_day', 0) / 100,
                equity_data.get('low_price_of_the_day', 0) / 100,
                round(((last_traded_price - closed_price) / (closed_price or 1)) * 100, 1),
                sector_info[0],
                str(sector_info[1]),
                round((((last_traded_price - (change_values[0] or 0)) / ((change_values[0] or 1))) * 100), 1),
                round((((last_traded_price - (change_values[1] or 0)) / ((change_values[1] or 1))) * 100), 1),
                round((((last_traded_price - (change_values[2] or 0)) / ((change_values[2] or 1))) * 100), 1),
                round((((last_traded_price - (change_values[3] or 0)) / ((change_values[3] or 1))) * 100), 1),
                closed_price,
            ]
            rows.append(row)

        except Exception as e:
            print(f"⚠️ Error processing token {token}: {e}")
            continue

    if not rows:
        print("⚠️ No data to write!")
        return

    df = pd.DataFrame(rows, columns=HEADERS)
    df = df.sort_values(by=["Name", "Strike","Expiry","Call_option"])
    return jsonify(df.to_dict(orient='records'))
   

# Imports for web socket
from SmartApi.smartWebSocketV2 import SmartWebSocketV2
from logzero import logger
import threading
import json
import math
import itertools

AUTH_TOKEN = authToken
API_KEY = os.getenv("API_KEY")
CLIENT_CODE = os.getenv("CLIENT_CODE")
FEED_TOKEN = feedToken

correlation_id_1 = "ws1"
correlation_id_2 = "ws2"
correlation_id_3 = "ws3"

# Subscription Modes
mode_1 = 3  # Mode for the first WebSocket
mode_2 = 3  # Mode for the second WebSocket
mode_3 = 3  # Mode for the second WebSocket


# Token Data (4614 entries)
option_tokens = options['token'].tolist()
equity_tokens = list(equity_token_mapping.values())

import itertools
import threading
import time

merged_data_lock = threading.Lock()  # Lock for shared `merged_data`

# Locks for independent WebSocket completion tracking
ws2_lock = threading.Lock()
ws3_lock = threading.Lock()

from concurrent.futures import ThreadPoolExecutor
import queue
import logging

logging.basicConfig(level=logging.DEBUG)

# Create a queue to hold incoming updates
update_queue = queue.Queue()
option_data_lock = threading.Lock()


BATCH_SIZE = 990

# Create option token batches
def create_batches(option_tokens):
    return [option_tokens[i:i + BATCH_SIZE] for i in range(0, len(option_tokens), BATCH_SIZE)]

ws2_batches = create_batches(option_tokens[:len(option_tokens)])  # First half
ws3_batches = create_batches(option_tokens[len(option_tokens)//2:])  # Second half

# Track completed cycles
ws2_completed = False
ws3_completed = False

from datetime import datetime
import pytz

def daily_sheet_writer():
    """Checks the time (IST) and writes data to a new Google Sheet at 3:45 PM IST."""
    india_tz = pytz.timezone("Asia/Kolkata")

    while True:
        now = datetime.now(india_tz)  # Get current time in IST
        if now.hour == 15 and now.minute == 45:  # 3:45 PM IST
            sheet_name = now.strftime("%Y-%m-%d")  # Create sheet name as YYYY-MM-DD
            logger.info(f"Creating new sheet: {sheet_name} and writing data.")
            write_to_google_sheet()  # Pass sheet name to function
            time.sleep(60)  # Wait a minute to prevent multiple writes in the same minute
        time.sleep(10)  # Check time every 10 seconds

import copy

prev_batches = {}

def cycle_subscriptions(ws, ws_id, batch_list):
    """Continuously cycle through the batches until stopped, and write to Google Sheets after completion."""
    global ws2_completed, ws3_completed,prev_batches

    while True:
      for i, batch in enumerate(ws2_batches):
              logger.info(f"{ws_id}: Subscribing batch {i+1}/{len(ws2_batches)} with {len(batch)} tokens")
            #   logger.debug(f"{ws_id}: batch content: {batch}")
            #   logger.debug(f"{ws_id}: Previous batch tokens before unsubscribing: {prev_batches}")

              if prev_batches:  # Unsubscribe from the previous batch of the **last** cycle
                  ws.unsubscribe(ws_id, mode_2, [{"exchangeType": 2, "tokens": list(prev_batches)}])
                #   logger.info(f"{ws_id}: Unsubscribing batch {i+1}/{len(ws2_batches)} with {len(prev_batches)} tokens")
                  time.sleep(1)  # Allow time for unsubscription to process


              prev_batches = tuple(batch)  # Store the current batch
              ws.subscribe(ws_id, mode_2, [{"exchangeType": 2, "tokens": list(batch)}])
              time.sleep(1)
    #   write_to_google_sheet()



# Second WebSocket tokens (Equity Data)
equity_list = [
    {
        "exchangeType": 1,
        "tokens": list(equity_token_mapping.values())
    }
]


def update_equity_data(ws_equity_data):
    equity_token = ws_equity_data["token"]
    # with merged_data_lock:
    #   if equity_token in equity_to_option_map:
    #       for option_token in equity_to_option_map[equity_token]:
    #           if option_token in merged_data:
    #               merged_data[option_token]["equity"] = ws_equity_data
    if equity_token in equity_to_option_map:
        for option_token in equity_to_option_map[equity_token]:
            if option_token in merged_data:
                merged_data[option_token]["equity"] = ws_equity_data



def on_data_1(wsapp, message):
    """Callback for WebSocket 1 (equity data)."""
    try:
        # logger.info(f"Equity Tick: {message}")
        equity_data = message
        update_equity_data(equity_data)
        # Add equity data to the queue
        # queue.append({"equity": equity_data})

    except Exception as e:
        logger.error(f"Error in Equity WebSocket on_data: {e}")


def on_data_2(wsapp, message):
    """Callback for WebSocket 2 (tick data)."""
    try:
        # logger.info(f"Options Tick: {message}")
        # options_data = message
        token = message["token"]
        option_data_final[token] = message
    except Exception as e:
        logger.error(f"Error in Option WebSocket on_data: {e}")


def on_open_1(wsapp):
    """Keep WebSocket 1 always subscribed for equity data."""
    logger.info("Equity WebSocket Opened")
    sws1.subscribe(correlation_id_1, mode_1, equity_list)

def on_open_2(wsapp):
    """WebSocket 2 - Process first half of option tokens in order."""
    logger.info("Options WebSocket 2 Opened")
    threading.Thread(target=cycle_subscriptions, args=(sws2, correlation_id_2, copy.deepcopy(ws2_batches)), daemon=True).start()


def on_error(wsapp, error):
    logger.error(f"WebSocket Error: {error}")


def on_close(wsapp):
    logger.info("WebSocket Closed")


# Initialize WebSocket connections
sws1 = SmartWebSocketV2(AUTH_TOKEN, API_KEY, CLIENT_CODE, FEED_TOKEN)
sws1.on_open = on_open_1
sws1.on_data = on_data_1
sws1.on_error = on_error
sws1.on_close = on_close

sws2 = SmartWebSocketV2(AUTH_TOKEN, API_KEY, CLIENT_CODE, FEED_TOKEN)
sws2.on_open = on_open_2
sws2.on_data = on_data_2
sws2.on_error = on_error
sws2.on_close = on_close


# Run WebSockets in separate threads
def run_ws1():
    sws1.connect()

def run_ws2():
    sws2.connect()


def close_websockets():
    try:
        # Close WebSocket 1
        sws1.close_connection()
        logger.info("WebSocket 1 disconnected successfully.")

        # Close WebSocket 2
        sws2.close_connection()
        logger.info("WebSocket 2 disconnected successfully.")

        # # Close WebSocket 3
        # sws3.close_connection()
        # logger.info("WebSocket 3 disconnected successfully.")
    except Exception as e:
        logger.error(f"Error while closing WebSockets: {e}")



if __name__ == "__main__":
    # Run WebSockets in separate threads
    threading.Thread(target=run_ws1, daemon=True).start()
    time.sleep(5)
    threading.Thread(target=run_ws2, daemon=True).start()
    threading.Thread(target=daily_sheet_writer, daemon=True).start()

    # Use the PORT from the environment if available (important for Render)
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)


# Keep script running
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Interrupt received. Closing WebSockets...")
        close_websockets()

