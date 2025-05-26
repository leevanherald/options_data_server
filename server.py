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


# def get_change(token):
#     """Fetches the last year's data in one API call and extracts required dates."""

#     today = dt.date.today()
#     one_year_ago = today - relativedelta(months=14)

#     # Fetch entire year's data in a single call
#     try:
#         historic_data = smartApi.getCandleData({
#             "exchange": "NSE",
#             "symboltoken": token,
#             "interval": "ONE_DAY",
#             "fromdate": one_year_ago.strftime("%Y-%m-%d 09:00"),
#             "todate": today.strftime("%Y-%m-%d 09:00"),
#         })
#         time.sleep(1)  # Sleep to avoid hitting rate limits
#     except Exception as e:
#         print(f"Error fetching data for token {token}: {e}")
#         return None

#     # Check if valid data was returned
#     if "data" not in historic_data or not historic_data["data"]:
#         return None

#     # Convert API response to a dictionary {date: closing_price}
#     price_data = {entry[0][:10]: entry[4] for entry in historic_data["data"]}  # Extract closing price

#     # Required historical dates
#     offsets = {"one_month": 1, "three_months": 3, "six_months": 6, "twelve_months": 12}
#     historical_prices = {}

#     for label, months_ago in offsets.items():
#         target_date = today - relativedelta(months=months_ago)

#         # Adjust to previous business day if necessary
#         while target_date.strftime("%Y-%m-%d") not in price_data:
#           target_date -= dt.timedelta(days=1)  # Move back one day until a valid date is found


#         historical_prices[label] = price_data.get(target_date.strftime("%Y-%m-%d"), None)

#     return [
#         historical_prices["one_month"],
#         historical_prices["three_months"],
#         historical_prices["six_months"],
#         historical_prices["twelve_months"],
#     ]

# change_data = {}
# for token in equity_token_mapping.values():
#   change_data[token] = get_change(token)


change_data = {'20374': [380.65, 380.35, 491.85, 447.4],
 '4963': [1214.55, 1263.75, 1236.6, 1087.15],
 '9683': [1305.4, 1428.3, 1714.0, 1493.55],
 '11543': [7707.25, 9558.0, 7321.95, 5682.2],
 '21770': [1686.95, 1859.2, 2100.15, 1710.7],
 '19234': [570.85, 611.1, 439.25, 462.45],
 '18652': [549.55, 652.95, 742.6, 628.7],
 '15141': [2474.15, 2789.75, 3743.9, 2696.85],
 '1997': [532.4, 582.85, 615.25, 646.8],
 '4749': [656.65, 756.75, 891.7, 911.35],
 '9480': [763.15, 868.9, 964.4, 996.7],
 '3220': [1113.0, 1350.35, 1166.9, 1194.4],
 '17094': [344.45, 369.5, 430.2, 290.2],
 '11483': [3244.7, 3596.35, 3532.4, 3807.85],
 '14366': [7.55, 7.99, 9.5, 13.15],
 '11184': [57.34, 62.65, 73.13, 82.5],
 '1901': [2899.05, 3163.9, 3708.3, 2988.2],
 '220': [163.93, 173.88, 204.59, 145.5],
 '24948': [141.71, 143.4, 171.99, 168.85],
 '5748': [1269.6, 1799.15, 1832.15, 2102.65],
 '17818': [4721.95, 5881.85, 6376.8, 4897.4],
 '11262': [188.49, 203.73, 266.48, 237.68],
 '10440': [2030.0, 2252.15, 2216.95, 1612.0],
 '11809': [322.1, 434.6, 458.55, 413.76],
 '772': [495.8, 513.75, 567.35, 503.05],
 '2031': [2727.85, 3086.1, 3165.85, 2078.1],
 '1512': [744.3, 834.9, 684.65, 605.1],
 '20050': [2130.0, 2105.0, 2052.0, 1989.91],
 '14309': [530.15, 507.5, 528.9, 537.4],
 '8075': [1696.35, 1800.1, 1879.95, 2010.9],
 '19061': [208.39, 179.98, 182.92, 194.8],
 '19943': [1989.0, 2436.65, 2790.65, 2206.8],
 '9599': [255.45, 334.75, 416.9, 455.0],
 '11195': [4657.55, 4262.15, 4602.95, 3599.8],
 '4067': [609.65, 638.4, 697.9, 519.0],
 '10999': [11664.15, 11822.0, 12531.95, 12865.1],
 '10940': [5578.75, 5868.4, 5547.05, 3796.0],
 '22377': [980.75, 1203.2, 955.85, 820.55],
 '21690': [13911.25, 16905.85, 14519.0, 7635.3],
 '5258': [936.75, 981.05, 1359.55, 1568.35],
 '14732': [667.05, 802.6, 840.0, 918.25],
 '31181': [4664.05, 5938.5, 6002.45, 3597.0],
 '19913': [3593.6, 3843.0, 4465.45, 4672.8],
 '29135': [327.4, 330.35, 370.0, 327.8],
 '2142': [1036.6, 1088.9, 1154.1, 1036.65],
 '881': [1133.25, 1370.75, 1331.21, 1234.91],
 '17534': [1284.0, 1252.55, 1828.8, 1483.1],
 '1594': [1686.0, 1933.15, 1948.55, 1476.7],
 '25510': None,
 '7852': [170.75, 168.99, 214.62, 138.31],
 '4503': [2302.25, 2924.85, 2901.1, 2454.2],
 '910': [5100.7, 5163.2, 4693.45, 4205.75],
 '1624': [124.84, 134.6, 164.24, 170.3],
 '2277': [107202.95, 119869.3, 132140.2, 132708.45],
 '958': [3011.1, 3340.55, 3855.4, 3072.25],
 '676': [355.85, 405.45, 493.8, 377.15],
 '23650': [2179.45, 2179.2, 1884.05, 1674.2],
 '1023': [180.51, 195.5, 187.76, 154.4],
 '6364': [195.17, 204.38, 212.79, 178.0],
 '13751': [6833.85, 8237.1, 8270.05, 6225.4],
 '13611': [700.65, 769.9, 875.1, 1014.8],
 '15313': [44.51, 57.27, 58.91, 70.0],
 '4717': [158.14, 190.58, 224.75, 200.75],
 '31415': [80.76, 89.83, 115.29, 90.07],
 '2319': [184.74, 265.2, 300.6, 266.15],
 '20261': [149.59, 223.38, 224.39, 174.3],
 '7406': [1404.4, 1627.9, 1734.55, 1039.1],
 '2029': [123.42, 143.77, 151.82, 147.5],
 '17963': [2238.45, 2219.9, 2581.75, 2498.05],
 '13528': [72.82, 76.81, 87.2, 85.7],
 '1660': [403.9, 449.55, 477.47, 396.25],
 '6733': [908.95, 942.55, 982.9, 911.55],
 '17400': [77.03, 78.99, 90.64, 92.35],
 '15332': [67.13, 66.0, 73.0, 74.7],
 '10099': [1045.3, 1156.25, 1304.55, 1246.2],
 '18143': [221.81, 298.6, 345.6, 369.25],
 '17875': [2008.35, 2564.25, 2951.9, 2588.6],
 '11630': [329.55, 325.4, 420.95, 363.5],
 '11872': [493.15, 608.4, 556.5, 439.1],
 '1232': [2407.25, 2435.75, 2738.2, 2280.8],
 '11236': [654.6, 645.45, 751.75, 703.2],
 '6545': [164.79, 169.52, 193.59, 179.3],
 '17869': [491.7, 579.35, 722.55, 627.5],
 '2303': [3452.05, 4109.8, 4368.3, 3567.15],
 '20242': [1543.15, 2282.4, 1759.6, 1532.4],
 '11723': [1011.15, 902.3, 998.2, 876.45],
 '10738': [7569.1, 11979.85, 11199.1, 8563.1],
 '9819': [1460.55, 1643.7, 1948.45, 1530.15],
 '18096': [602.8, 750.4, 615.4, 452.7],
 '17438': [369.75, 491.7, 565.6, 416.4],
 '2475': [232.89, 271.33, 293.45, 270.4],
 '1922': [1935.4, 1768.65, 1803.4, 1788.4],
 '2955': [431.7, 706.4, 709.95, 433.45],
 '7229': [1557.95, 1932.25, 1789.45, 1540.05],
 '13310': [3223.85, 4264.5, 4201.75, 3764.0],
 '14413': [40067.15, 48063.3, 42267.1, 35024.6],
 '4244': [3834.2, 4062.45, 4189.45, 3660.35],
 '1333': [1689.25, 1694.3, 1651.05, 1546.6],
 '17029': [1749.65, 1855.6, 1690.1, 1368.2],
 '6705': [684.85, 902.05, 750.6, 412.2],
 '1348': [3652.5, 4138.1, 5529.85, 4595.1],
 '2412': [918.55, 1047.85, 1024.0, 864.9],
 '21951': [83.87, 107.23, 140.67, 97.0],
 '18365': [5278.8, 6230.2, 5308.65, 3903.8],
 '11351': [285.9, 331.0, 353.4, 281.0],
 '1363': [691.35, 586.65, 721.8, 576.8],
 '14299': [401.1, 434.9, 465.85, 410.35],
 '17939': [223.85, 239.73, 314.3, 331.35],
 '14552': [1560.15, 1692.25, 1656.4, 1448.8],
 '1406': [332.45, 391.35, 390.25, 308.6],
 '2664': [2751.2, 2908.0, 3185.25, 3044.2],
 '1394': [2204.55, 2401.0, 2818.8, 2268.95],
 '20825': [182.27, 238.44, 228.88, 209.1],
 '24184': [3219.1, 3630.45, 4570.7, 3959.7],
 '10666': [91.17, 101.97, 102.49, 132.9],
 '6656': [1397.8, 2002.85, 1661.55, 1285.25],
 '9590': [5063.45, 6904.6, 7262.75, 5281.4],
 '11403': [288.45, 321.8, 374.25, 488.7],
 '14977': [263.3, 306.65, 327.15, 281.75],
 '20302': [1144.85, 1568.6, 1816.45, 1275.45],
 '2043': [871.65, 943.5, 857.25, 848.9],
 '18391': [163.79, 161.66, 196.01, 255.5],
 '15355': [406.2, 503.6, 524.25, 453.5],
 '2885': [1249.8, 1265.5, 1397.35, 1485.98],
 '2963': [110.91, 109.76, 131.37, 146.3],
 '17971': [834.8, 737.25, 732.25, 723.3],
 '21808': [1411.6, 1463.15, 1728.05, 1518.8],
 '3045': [732.75, 771.15, 781.45, 768.3],
 '3103': [27985.2, 26027.45, 25582.9, 25955.2],
 '4306': [630.85, 579.75, 665.86, 513.84],
 '467': [625.2, 618.0, 710.1, 634.6],
 '3150': [5106.1, 6296.65, 7372.15, 5668.0],
 '18883': [87.76, 103.01, 120.54, 135.1],
 '13332': [9486.5, 9621.05, 11273.4, 8725.0],
 '4684': [517.6, 573.15, 673.45, 645.8],
 '3273': [2935.35, 2350.95, 2328.4, 2607.2],
 '3351': [1609.3, 1837.75, 1917.15, 1599.7],
 '3363': [3390.45, 4607.1, 5437.35, 4309.55],
 '10243': [679.65, 871.05, 876.95, 729.0],
 '3405': [815.3, 1003.7, 1085.7, 1137.55],
 '3721': [1383.85, 1704.7, 2004.15, 2048.25],
 '3432': [962.05, 961.6, 1119.05, 1137.35],
 '3411': [5624.15, 6473.5, 7550.1, 7922.45],
 '3456': [648.3, 794.95, 919.8, 1013.2],
 '3426': [351.45, 374.05, 456.9, 416.35],
 '3499': [151.56, 132.64, 159.52, 165.2],
 '18908': [818.2, 873.9, 939.1, 763.9],
 '20293': [671.55, 886.3, 1044.0, 1101.9],
 '11536': [3611.2, 4108.4, 4253.25, 3972.55],
 '13538': [1492.35, 1663.75, 1629.1, 1265.15],
 '312': [2701.8, 3521.95, 4055.75, 3549.75],
 '15414': [759.0, 1108.9, 1111.1, 956.05],
 '3506': [3079.35, 3484.05, 3493.35, 3748.9],
 '3518': [3073.8, 3324.75, 3517.45, 2597.3],
 '13786': [1332.0, 1434.45, 1816.65, 1568.4],
 '1964': [4999.85, 6699.1, 8041.95, 3931.85],
 '8479': [2332.9, 2361.8, 2740.55, 2132.95],
 '11532': [10582.4, 11403.55, 11389.8, 9863.75],
 '10753': [117.78, 111.12, 114.56, 155.0],
 '10447': [1327.85, 1575.8, 1519.0, 1140.75],
 '11287': [629.85, 539.55, 553.29, 469.12],
 '18921': [487.75, 606.95, 590.0, 580.04],
 '3063': [445.45, 446.4, 497.45, 323.3],
 '3718': [1405.45, 1764.45, 1800.9, 1306.85],
 '3787': [284.8, 297.55, 263.48, 239.95],
 '11915': [16.88, 18.77, 21.54, 24.75],
 '5097': [216.83, 250.05, 278.7, 191.8],
 '7929': [900.8, 981.45, 1054.65, 1000.65],
 '21174': [1152.9, 1722.4, 1362.1, 938.63],
 '335': [2559.6, 2773.9, 3004.35, 2441.75],
 '4745': [97.41, 100.11, 105.94, 144.2],
 '7': [406.3, 397.85, 537.85, 704.8],
 '11373': [333.7, 371.2, 342.7, 273.8],
 '637': [571.2, 500.5, 495.7, 377.3],
 '422': [1087.8, 1248.55, 1481.35, 1173.9],
 '10217': [747.6, 770.2, 1007.3, 1075.85],
 '13': [5326.55, 6657.0, 8154.65, 6699.75],
 '21614': [160.85, 178.91, 223.83, 203.15],
 '30108': [242.31, 267.35, 333.65, 239.4],
 '22': [1885.75, 2008.0, 2385.8, 2582.45],
 '25': [2247.5, 2515.25, 3160.7, 3224.55],
 '3563': [837.2, 988.95, 1813.85, 1919.1],
 '15083': [1144.5, 1152.3, 1418.55, 1348.65],
 '11703': [4738.05, 5561.7, 6250.85, 4888.35],
 '1270': [499.8, 538.05, 606.4, 630.55],
 '324': [2098.0, 2653.65, 2597.15, 2945.15],
 '25780': [1458.25, 1538.6, 1544.8, 1588.3],
 '157': [6216.1, 7142.65, 6924.3, 6307.75],
 '163': [411.7, 482.9, 508.95, 469.9],
 '212': [209.78, 222.73, 222.47, 177.2],
 '236': [2270.0, 2334.35, 3088.05, 2892.0],
 '14418': [1332.65, 1553.25, 1899.1, 2024.8],
 '6066': [607.2, 704.95, 759.95, 966.8],
 '21238': [544.3, 566.45, 727.35, 633.9],
 '760': [630.05, 694.6, 780.4, 504.6],
 '6994': [434.45, 544.05, 570.2, 732.0],
 '275': [1097.4, 1281.65, 1491.25, 1120.25],
 '5900': [1037.65, 1074.95, 1153.3, 1076.05],
 '16669': [7574.75, 8642.25, 11889.1, 9030.5],
 '16675': [1845.3, 1697.0, 1838.15, 1689.15],
 '317': [8404.5, 7355.4, 7186.95, 7178.55],
 '2263': [149.93, 151.72, 187.66, 184.95],
 '4668': [205.9, 232.95, 247.24, 267.6],
 '383': [276.99, 282.1, 280.25, 223.55],
 '404': [509.35, 461.95, 573.1, 560.0],
 '10604': [1630.7, 1599.2, 1657.45, 1204.7],
 '438': [196.95, 221.37, 265.5, 256.45],
 '2181': [27033.4, 33014.75, 37967.65, 31087.35],
 '526': [261.26, 286.8, 338.0, 295.98],
 '547': [4748.25, 4859.9, 6204.4, 4826.8],
 '19585': [4178.3, 5400.4, 4242.65, 2801.7],
 '342': [3365.7, 4707.95, 4410.35, 3069.4],
 '10794': [85.02, 97.14, 104.95, 122.49],
 '628': [140.94, 174.42, 190.14, 140.85],
 '19257': None,
 '694': [1458.45, 1494.9, 1640.7, 1462.45],
 '1424': [429.35, 443.8, 510.8, 344.4]}

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

# def daily_sheet_writer():
#     """Checks the time (IST) and writes data to a new Google Sheet at 3:45 PM IST."""
#     india_tz = pytz.timezone("Asia/Kolkata")

#     while True:
#         now = datetime.now(india_tz)  # Get current time in IST
#         if now.hour == 15 and now.minute == 45:  # 3:45 PM IST
#             sheet_name = now.strftime("%Y-%m-%d")  # Create sheet name as YYYY-MM-DD
#             logger.info(f"Creating new sheet: {sheet_name} and writing data.")
#             write_to_google_sheet()  # Pass sheet name to function
#             time.sleep(60)  # Wait a minute to prevent multiple writes in the same minute
#         time.sleep(10)  # Check time every 10 seconds

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

def start_background_services():
    threading.Thread(target=run_ws1, daemon=True).start()
    threading.Thread(target=run_ws2, daemon=True).start()

@app.before_first_request
def before_first_request():
    start_background_services()



# # if __name__ == "__main__":
# #     # Use the PORT from the environment if available (important for Render)
# port = int(os.environ.get("PORT", 5000))
# app.run(host="0.0.0.0", port=port)


# # Keep script running
#     try:
#         while True:
#             time.sleep(1)
#     except KeyboardInterrupt:
#         logger.info("Interrupt received. Closing WebSockets...")
#         close_websockets()

