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
import signal
import atexit
import sys




app = Flask(__name__)
CORS(app)

@app.route("/")
def home():
    return "WebSocket app running!"
    
@app.route("/shutdown", methods=["POST"])
def shutdown():
    logger.info("Shutdown endpoint called.")
    close_websockets()
    os._exit(0)

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "websockets": "running"})



load_dotenv()

def graceful_exit():
    logger.info("Cleaning up before shutdown...")
    close_websockets()

atexit.register(graceful_exit)
signal.signal(signal.SIGINT, lambda s, f: sys.exit(0))
signal.signal(signal.SIGTERM, lambda s, f: sys.exit(0))

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

# print(authToken)

# Fetch all the scripts that is tradable
url = 'https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json'
d = requests.get(url).json()
token_df = pd.DataFrame.from_dict(d)
# print(token_df)

stock_symbols = token_df[(token_df['exch_seg'] == 'NFO') & (token_df['instrumenttype'] == 'OPTSTK')].loc[:,'name'].unique().tolist()


tickers = [f"{stock}.NS" for stock in stock_symbols]  # Convert to Yahoo format
data = yf.Tickers(" ".join(tickers))  # Fetch all at once

# sector_market_cap = {}
# for stock in stock_symbols:
#     ticker_data = data.tickers[f"{stock}.NS"]
#     info = ticker_data.info
#     equity_sector = info.get('sector', 'Sector info not available')
#     equity_marketcap = info.get('marketCap', 0) / 1e7  # Market cap in crores
#     sector_market_cap[stock] = [equity_sector, equity_marketcap]

sector_market_cap = {'IDFCFIRSTB': ['Financial Services', 53007.4959872],
 'HEROMOTOCO': ['Consumer Cyclical', 86994.9571072],
 'HFCL': ['Technology', 13265.6939008],
 'ICICIBANK': ['Financial Services', 1023434.1883904],
 'IEX': ['Financial Services', 18623.9434752],
 'ICICIGI': ['Financial Services', 100267.098112],
 'HINDALCO': ['Basic Materials', 145568.3403776],
 'IGL': ['Utilities', 30040.1688576],
 'HINDCOPPER': ['Basic Materials', 25033.3495296],
 'HINDPETRO': ['Energy', 86910.8088832],
 'IIFL': ['Financial Services', 20709.801984],
 'INDHOTEL': ['Consumer Cyclical', 111084.5816832],
 'HINDUNILVR': ['Consumer Defensive', 562303.860736],
 'ICICIPRULI': ['Financial Services', 92185.8998272],
 'HINDZINC': ['Basic Materials', 221829.2936704],
 'INDIANB': ['Financial Services', 87802.01984],
 'INDIGO': ['Industrials', 220082.7068416],
 'IDEA': ['Communication Services', 76381.814784],
 'HUDCO': ['Financial Services', 49514.9940736],
 'INDUSINDBK': ['Financial Services', 65179.7200896],
 'INDUSTOWER': ['Communication Services', 104369.0422272],
 'INFY': ['Technology', 651897.864192],
 'INOXWIND': ['Industrials', 24021.0264064],
 'IOC': ['Energy', 202385.0377216],
 'IRB': ['Industrials', 32103.3240576],
 'IRCTC': ['Industrials', 63032.000512],
 'IREDA': ['Financial Services', 49250.5137152],
 'IRFC': ['Financial Services', 192054.689792],
 'ITC': ['Consumer Defensive', 529408.98304],
 'JINDALSTEL': ['Basic Materials', 98603.0145536],
 'JIOFIN': ['Financial Services', 193294.2901248],
 'JSL': ['Basic Materials', 58986.7646976],
 'JSWENERGY': ['Utilities', 93353.4302208],
 'JSWSTEEL': ['Basic Materials', 246000.386048],
 'JUBLFOOD': ['Consumer Cyclical', 45984.8450048],
 'KALYANKJIL': ['Consumer Cyclical', 56934.694912],
 'KEI': ['Industrials', 36432.2947072],
 'KOTAKBANK': ['Financial Services', 425127.6017664],
 'KPITTECH': ['Technology', 36772.151296],
 'LAURUSLABS': ['Healthcare', 36008.2014208],
 'LICHSGFIN': ['Financial Services', 34222.1684736],
 'LICI': ['Financial Services', 609034.2793216],
 'LODHA': ['Real Estate', 150839.0862848],
 'LT': ['Industrials', 505951.7612032],
 'LTF': ['Financial Services', 48214.6844672],
 'LTIM': ['Technology', 155780.3433984],
 'KAYNES': ['Industrials', 36218.8439552],
 'LUPIN': ['Healthcare', 91092.385792],
 'M&M': ['Consumer Cyclical', 370376.8612864],
 'MANAPPURAM': ['Financial Services', 22415.5336704],
 'MARICO': ['Consumer Defensive', 91171.06176],
 'MARUTI': ['Consumer Cyclical', 397311.082496],
 'M&MFIN': ['Financial Services', 35999.154176],
 'MAXHEALTH': ['Healthcare', 116157.5268352],
 'MCX': ['Financial Services', 40495.7233152],
 'MFSL': ['Financial Services', 52644.184064],
 'MGL': ['Utilities', 13989.9011072],
 'MOTHERSON': ['Consumer Cyclical', 113284.4285952],
 'MPHASIS': ['Technology', 49508.5387776],
 'MUTHOOTFIN': ['Financial Services', 101988.663296],
 'NATIONALUM': ['Basic Materials', 34389.0624512],
 'NAUKRI': ['Communication Services', 98350.2749696],
 'NBCC': ['Industrials', 34748.9992704],
 'NCC': ['Industrials', 15063.9312896],
 'NESTLEIND': ['Consumer Defensive', 232872.8436736],
 'NHPC': ['Utilities', 91339.1812608],
 'MANKIND': ['Healthcare', 97274.9144064],
 'NMDC': ['Basic Materials', 65059.4680832],
 'NTPC': ['Utilities', 326874.7460608],
 'NYKAA': ['Consumer Cyclical', 57765.8290176],
 'OBEROIRLTY': ['Real Estate', 69197.0899968],
 'OFSS': ['Technology', 79682.240512],
 'MAZDOCK': ['Industrials', 135902.7634176],
 'OIL': ['Energy', 70652.1595904],
 'ONGC': ['Energy', 305449.6907264],
 'PAGEIND': ['Consumer Cyclical', 51718.1571072],
 'PATANJALI': ['Consumer Defensive', 61665.509376],
 'PAYTM': ['Technology', 61870.6436096],
 'PEL': ['Financial Services', 26848.3280896],
 'PERSISTENT': ['Technology', 89497.9670016],
 'PETRONET': ['Energy', 46597.4984704],
 'PFC': ['Financial Services', 142370.4981504],
 'PHOENIXLTD': ['Real Estate', 57768.5684224],
 'PIDILITIND': ['Basic Materials', 155432.9632768],
 'PIIND': ['Basic Materials', 59707.2674816],
 'PNB': ['Financial Services', 128950.337536],
 'PNBHOUSING': ['Financial Services', 29284.2307584],
 'POLICYBZR': ['Financial Services', 87220.1781248],
 'POLYCAB': ['Industrials', 92422.3537152],
 'POONAWALLA': ['Financial Services', 33346.2339584],
 'POWERGRID': ['Utilities', 279530.4910848],
 'PRESTIGE': ['Real Estate', 73695.461376],
 'RBLBANK': ['Financial Services', 14022.6576384],
 'RECLTD': ['Financial Services', 112309.2529152],
 'RELIANCE': ['Energy', 1960588.607488],
 'SAIL': ['Basic Materials', 55282.8993536],
 'SBICARD': ['Financial Services', 96770.9032448],
 'SBILIFE': ['Financial Services', 179506.2235136],
 'SBIN': ['Financial Services', 731863.4233856],
 'SHRIRAMFIN': ['Financial Services', 131260.9927168],
 'SIEMENS': ['Industrials', 120486.4155648],
 'SJVN': ['Utilities', 40622.342144],
 'SOLARINDS': ['Basic Materials', 152267.6793344],
 'SONACOMS': ['Consumer Cyclical', 32413.2667392],
 'SRF': ['Industrials', 92671.34464],
 'SUNPHARMA': ['Healthcare', 406542.483456],
 'SUPREMEIND': ['Industrials', 55899.5013632],
 'SYNGENE': ['Healthcare', 26374.0604416],
 'TATACHEM': ['Basic Materials', 24468.0409088],
 'SHREECEM': ['Basic Materials', 107610.6887168],
 'TATACOMM': ['Communication Services', 49721.09824],
 'TATACONSUM': ['Consumer Defensive', 110947.8866944],
 'TATAELXSI': ['Technology', 41304.031232],
 'TATAMOTORS': ['Consumer Cyclical', 264255.2029184],
 'TATAPOWER': ['Utilities', 129794.7164672],
 'TATASTEEL': ['Basic Materials', 196206.3691776],
 'TATATECH': ['Technology', 31471.7995008],
 'TCS': ['Technology', 1238074.130432],
 'PPLPHARMA': ['Healthcare', 27964.4274688],
 'TECHM': ['Technology', 139767.0805504],
 'TIINDIA': ['Industrials', 58835.4158592],
 'TITAGARH': ['Industrials', 12847.8994432],
 'TITAN': ['Consumer Cyclical', 313472.8175616],
 'RVNL': ['Industrials', 90041.8961408],
 'TORNTPHARM': ['Healthcare', 107649.1976704],
 'TORNTPOWER': ['Utilities', 70926.958592],
 'TRENT': ['Consumer Cyclical', 210217.2450816],
 'TVSMOTOR': ['Consumer Cyclical', 130229.5740416],
 'ULTRACEMCO': ['Basic Materials', 331210.6602496],
 'UNIONBANK': ['Financial Services', 118634.053632],
 'UNITDSPR': ['Consumer Defensive', 113094.1251584],
 'UPL': ['Basic Materials', 51859.0005248],
 'VBL': ['Consumer Defensive', 161182.3054848],
 'VEDL': ['Basic Materials', 178823.6791808],
 'VOLTAS': ['Consumer Cyclical', 42568.3550208],
 'WIPRO': ['Technology', 263093.0661376],
 'YESBANK': ['Financial Services', 65924.3925504],
 'ZYDUSLIFE': ['Healthcare', 97971.5883008],
 'UNOMINDA': ['Consumer Cyclical', 62756.1201664],
 'ETERNAL': ['Consumer Cyclical', 233107.9868416],
 'ANGELONE': ['Financial Services', 28973.5442432],
 'CDSL': ['Financial Services', 37484.150784],
 'CYIENT': ['Industrials', 14585.7134592],
 'GMRAIRPORT': ['Industrials', 91240.4119552],
 'HAL': ['Industrials', 334962.6486784],
 'ABCAPITAL': ['Financial Services', 62677.450752],
 'ASTRAL': ['Industrials', 41232.7870464],
 'HDFCAMC': ['Financial Services', 111789.473792],
 'ABFRL': ['Consumer Cyclical', 9406.2026752],
 'CANBK': ['Financial Services', 106752.4882432],
 'CONCOR': ['Industrials', 48807.4903552],
 'GODREJCP': ['Consumer Defensive', 124597.0038784],
 'ADANIENSOL': ['Utilities', 108302.3163392],
 'BEL': ['Industrials', 286872.3318784],
 'BANKBARODA': ['Financial Services', 128322.568192],
 'COFORGE': ['Technology', 59996.0190976],
 'DLF': ['Real Estate', 217480.7310336],
 'BSE': ['Financial Services', 121611.2623616],
 'DELHIVERY': ['Industrials', 27743.5809792],
 'BHEL': ['Industrials', 89889.3807616],
 'CHAMBLFERT': ['Basic Materials', 22113.9877888],
 'ADANIPORTS': ['Industrials', 316914.139136],
 'AARTIIND': ['Basic Materials', 17533.280256],
 'AUBANK': ['Financial Services', 57488.211968],
 'ASHOKLEY': ['Industrials', 71442.8776448],
 'APLAPOLLO': ['Basic Materials', 52266.0544512],
 'DALBHARAT': ['Basic Materials', 39793.7901568],
 'ABB': ['Industrials', 131762.9722624],
 'ACC': ['Basic Materials', 35784.6908928],
 'ADANIENT': ['Energy', 298116.3696128],
 'ALKEM': ['Healthcare', 58270.0040192],
 'AMBUJACEM': ['Basic Materials', 138133.5302144],
 'ADANIGREEN': ['Utilities', 167825.5194112],
 'GODREJPROP': ['Real Estate', 74311.3752576],
 'DIXON': ['Technology', 89843.5776512],
 'APOLLOHOSP': ['Healthcare', 99671.7559808],
 'ASIANPAINT': ['Basic Materials', 215539.8823936],
 'ATGL': ['Utilities', 75974.8755456],
 'AUROPHARMA': ['Healthcare', 67436.9175552],
 'AXISBANK': ['Financial Services', 378417.8663424],
 'BAJAJ-AUTO': ['Consumer Cyclical', 241331.9159808],
 'BAJAJFINSV': ['Financial Services', 321773.2665344],
 'BAJFINANCE': ['Financial Services', 596544.1138688],
 'BALKRISIND': ['Consumer Cyclical', 48381.444096],
 'BANDHANBNK': ['Financial Services', 30011.4608128],
 'BANKINDIA': ['Financial Services', 58870.5759232],
 'BHARATFORG': ['Consumer Cyclical', 64283.8429696],
 'BHARTIARTL': ['Communication Services', 1117733.8863616],
 'BIOCON': ['Healthcare', 40518.483968],
 'BOSCHLTD': ['Consumer Cyclical', 93493.7362432],
 'BPCL': ['Energy', 139005.853696],
 'BRITANNIA': ['Consumer Defensive', 137222.5011712],
 'BSOFT': ['Technology', 11607.7043712],
 'CAMS': ['Technology', 21112.9499648],
 'CESC': ['Utilities', 22554.5732096],
 'CGPOWER': ['Industrials', 106214.0706816],
 'CHOLAFIN': ['Financial Services', 138327.6347392],
 'BDL': ['Industrials', 71428.2696704],
 'CIPLA': ['Healthcare', 121686.5763328],
 'COALINDIA': ['Energy', 247125.4818816],
 'BLUESTARCO': ['Industrials', 32269.2186112],
 'COLPAL': ['Consumer Defensive', 66750.0412928],
 'CROMPTON': ['Consumer Cyclical', 22814.9313536],
 'CUMMINSIND': ['Industrials', 94575.099904],
 'DABUR': ['Consumer Defensive', 86130.3865344],
 'DIVISLAB': ['Healthcare', 176085.5851008],
 'DMART': ['Consumer Defensive', 271947.8185984],
 'DRREDDY': ['Healthcare', 109757.1631104],
 'EICHERMOT': ['Consumer Cyclical', 147849.2553216],
 'EXIDEIND': ['Consumer Cyclical', 34072.33024],
 'FEDERALBNK': ['Financial Services', 52220.6183424],
 'GAIL': ['Utilities', 127319.8411776],
 'GLENMARK': ['Healthcare', 45129.580544],
 'GRASIM': ['Basic Materials', 176964.2917888],
 'HAVELLS': ['Industrials', 96787.3134592],
 'GRANULES': ['Healthcare', 13014.9105664],
 'HCLTECH': ['Technology', 446509.6654848],
 'HDFCBANK': ['Financial Services', 1516003.6032512],
 'FORTIS': ['Healthcare', 57950.7585024],
 'HDFCLIFE': ['Financial Services', 163801.5565824]}

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


change_data = {'11184': [66.21, 57.34, 65.22, 77.7],
 '1348': [3853.9, 3652.5, 4595.95, 5581.85],
 '21951': [74.14, 83.87, 130.93, 97.45],
 '4963': [1388.9, 1214.55, 1322.3, 1121.7],
 '220': [189.38, 163.93, 184.13, 163.5],
 '21770': [1782.2, 1686.95, 1962.2, 1655.15],
 '1363': [627.15, 691.35, 670.9, 680.35],
 '11262': [199.71, 188.49, 192.78, 233.5],
 '17939': [205.32, 223.85, 292.5, 340.3],
 '1406': [386.25, 332.45, 399.7, 349.23],
 '11809': [372.5, 322.1, 432.6, 470.5],
 '1512': [718.75, 744.3, 837.2, 585.1],
 '1394': [2332.9, 2204.55, 2400.75, 2577.8],
 '18652': [581.2, 549.55, 676.2, 571.4],
 '1424': [408.35, 429.35, 506.5, 688.15],
 '14309': [552.05, 530.15, 597.7, 534.7],
 '11195': [5101.0, 4657.55, 4489.45, 4373.2],
 '14366': [6.7, 7.55, 8.09, 15.85],
 '20825': [208.47, 182.27, 247.59, 248.6],
 '5258': [818.2, 936.75, 982.6, 1492.1],
 '29135': [388.2, 327.4, 362.15, 347.15],
 '1594': [1507.6, 1686.0, 1923.65, 1533.6],
 '7852': [156.82, 170.75, 208.67, 150.2],
 '1624': [139.99, 124.84, 142.33, 164.2],
 '15313': [45.18, 44.51, 59.4, 76.85],
 '13611': [722.55, 700.65, 833.7, 977.75],
 '20261': [156.48, 149.59, 221.81, 180.85],
 '2029': [116.42, 123.42, 159.2, 173.6],
 '1660': [423.55, 403.9, 437.05, 412.8],
 '6733': [855.35, 908.95, 967.2, 1026.95],
 '18143': [248.35, 221.81, 336.85, 353.15],
 '11236': [579.6, 654.6, 747.05, 794.9],
 '17869': [459.85, 491.7, 675.2, 625.95],
 '11723': [957.0, 1011.15, 1011.9, 912.15],
 '18096': [670.2, 602.8, 694.7, 518.5],
 '2955': [519.15, 431.7, 775.45, 407.85],
 '13310': [3232.5, 3223.85, 4487.8, 4220.9],
 '1922': [2103.1, 1935.4, 1786.25, 1753.7],
 '9683': [1244.2, 1305.4, 1534.1, 1524.7],
 '19234': [588.8, 570.85, 571.9, 437.75],
 '1997': [574.75, 532.4, 630.15, 662.1],
 '9480': [786.95, 763.15, 986.35, 994.2],
 '3220': [1234.3, 1113.0, 1376.5, 1433.35],
 '11483': [3443.9, 3244.7, 3947.3, 3532.5],
 '24948': [161.53, 141.71, 148.23, 168.45],
 '17818': [4621.6, 4721.95, 6389.05, 4977.2],
 '12092': [5665.5, 4457.05, 6294.6, 3334.25],
 '10440': [2037.4, 2030.0, 2104.7, 1629.55],
 '2031': [2982.1, 2727.85, 3051.25, 2857.45],
 '19061': [228.62, 208.39, 170.29, 178.95],
 '4067': [723.3, 609.65, 607.75, 653.0],
 '10999': [12250.0, 11664.15, 11279.8, 12810.9],
 '20050': [2279.99, 2130.0, 2070.0, 2000.0],
 '22377': [1127.7, 980.75, 1116.95, 827.1],
 '31181': [5670.5, 4664.05, 6849.4, 3631.8],
 '2142': [1268.2, 1036.6, 1186.15, 936.85],
 '17534': [1362.5, 1284.0, 1279.35, 1381.05],
 '25510': None,
 '4503': [2387.3, 2302.25, 3104.6, 2471.95],
 '23650': [2229.6, 2179.45, 1991.3, 1773.15],
 '6364': [156.84, 195.17, 250.56, 185.3],
 '13751': [1375.0, 1366.77, 1683.77, 1245.96],
 '31415': [92.22, 80.76, 103.33, 96.07],
 '2319': [206.46, 184.74, 312.9, 324.35],
 '17963': [2325.4, 2238.45, 2228.85, 2502.45],
 '17400': [77.97, 77.03, 86.77, 103.2],
 '15380': [2408.2, 2316.05, 2639.35, 2127.5],
 '15332': [64.29, 67.13, 80.51, 86.17],
 '11630': [334.75, 329.55, 369.85, 360.6],
 '6545': [193.03, 164.79, 166.04, 169.9],
 '20242': [1534.2, 1543.15, 2139.9, 1889.5],
 '10738': [7993.5, 7569.1, 12585.0, 8406.3],
 '509': [2922.0, 2338.2, 2444.1, 1576.58],
 '17438': [403.3, 369.75, 467.85, 410.1],
 '2475': [234.96, 232.89, 258.9, 260.4],
 '14413': [45470.0, 40067.15, 46121.0, 38349.45],
 '17029': [1759.9, 1749.65, 1855.45, 1401.2],
 '6705': [832.65, 684.85, 971.95, 381.3],
 '2412': [1011.95, 918.55, 1241.1, 818.05],
 '18365': [5443.0, 5278.8, 6234.25, 3850.25],
 '11351': [307.95, 285.9, 335.1, 301.25],
 '14299': [385.9, 401.1, 515.35, 483.55],
 '14552': [1478.9, 1560.15, 1797.15, 1613.18],
 '2664': [2980.0, 2751.2, 3160.75, 3123.7],
 '24184': [3639.7, 3219.1, 4060.6, 3642.6],
 '10666': [91.97, 91.17, 108.77, 125.1],
 '18908': [1031.1, 818.2, 959.3, 746.5],
 '6656': [1619.9, 1397.8, 2131.15, 1292.1],
 '9590': [5766.5, 5063.45, 7438.4, 6846.45],
 '11403': [372.0, 288.45, 359.85, 465.5],
 '14977': [299.3, 263.3, 329.1, 309.35],
 '20302': [1281.6, 1144.85, 1742.1, 1761.0],
 '18391': [196.82, 163.79, 171.36, 251.25],
 '15355': [384.25, 406.2, 563.7, 496.8],
 '2885': [1377.2, 1249.8, 1295.15, 1469.95],
 '2963': [109.01, 110.91, 126.13, 152.75],
 '17971': [874.2, 834.8, 719.65, 715.55],
 '21808': [1699.8, 1411.6, 1469.3, 1425.85],
 '3045': [779.25, 732.75, 858.05, 829.95],
 '4306': [601.45, 630.85, 621.38, 499.59],
 '3150': [2857.2, 2538.47, 3898.63, 3417.91],
 '18883': [88.91, 87.76, 122.27, 133.35],
 '13332': [13487.0, 9486.5, 10962.9, 9295.0],
 '4684': [500.5, 517.6, 659.65, 664.45],
 '3273': [3004.3, 2935.35, 2302.35, 2312.3],
 '3351': [1744.8, 1609.3, 1806.65, 1506.85],
 '10243': [614.1, 679.65, 867.9, 695.2],
 '3363': [3483.8, 3390.45, 5012.65, 5748.55],
 '3405': [817.3, 815.3, 1106.05, 1056.15],
 '3103': [29230.0, 27985.2, 26711.25, 26077.5],
 '3721': [1515.8, 1383.85, 1785.35, 1812.75],
 '3432': [1113.7, 962.05, 933.95, 1135.65],
 '3411': [5738.0, 5624.15, 7356.0, 7120.0],
 '3456': [708.5, 648.3, 798.75, 970.5],
 '3426': [371.0, 351.45, 440.75, 443.55],
 '3499': [142.78, 151.56, 149.88, 178.9],
 '20293': [665.95, 671.55, 947.95, 1061.4],
 '11536': [3440.3, 3611.2, 4452.15, 3893.95],
 '11571': [207.77, 201.46, 255.6, 150.0],
 '13538': [1493.7, 1492.35, 1777.85, 1377.6],
 '312': [2860.3, 2701.8, 3710.15, 3930.75],
 '15414': [687.25, 759.0, 1213.85, 1208.65],
 '3506': [3510.3, 3079.35, 3468.2, 3444.05],
 '9552': [323.1, 341.5, 470.55, 374.55],
 '3518': [3141.2, 3073.8, 3335.7, 2844.95],
 '13786': [1371.7, 1332.0, 1645.95, 1498.0],
 '1964': [5113.0, 4999.85, 6949.7, 4964.6],
 '8479': [2663.7, 2332.9, 2488.85, 2407.65],
 '11532': [11364.0, 10582.4, 11814.8, 10463.15],
 '10753': [122.9, 117.78, 129.26, 146.45],
 '10447': [1531.2, 1327.85, 1506.25, 1307.55],
 '11287': [674.75, 629.85, 555.4, 517.75],
 '18921': [496.25, 487.75, 641.6, 607.44],
 '3063': [407.9, 445.45, 497.05, 460.65],
 '3718': [1235.6, 1405.45, 1764.35, 1451.5],
 '3787': [242.01, 284.8, 303.75, 242.28],
 '11915': [20.02, 16.88, 21.84, 23.15],
 '7929': [878.65, 900.8, 982.45, 1055.8],
 '14154': [911.85, 870.65, 1078.05, 963.4],
 '5097': [227.14, 216.83, 295.3, 184.0],
 '324': [2326.5, 2098.0, 3425.35, 2590.05],
 '21174': [1218.6, 1152.9, 1904.95, 1040.9],
 '5748': [1179.3, 1269.6, 2002.15, 1924.8],
 '13528': [84.2, 72.82, 86.2, 86.7],
 '2303': [4501.2, 3452.05, 4618.95, 4745.15],
 '21614': [193.17, 160.85, 196.83, 231.9],
 '14418': [1273.4, 1332.65, 1850.55, 2158.8],
 '4244': [4271.6, 3834.2, 4469.15, 3823.2],
 '30108': [92.52, 88.29, 112.34, 118.13],
 '10794': [97.64, 85.02, 108.99, 118.9],
 '4749': [648.65, 656.65, 858.1, 1054.8],
 '10099': [1242.1, 1045.3, 1127.85, 1427.9],
 '10217': [827.1, 747.6, 803.45, 1020.05],
 '383': [315.85, 276.99, 314.6, 283.2],
 '4668': [220.09, 205.9, 262.93, 270.8],
 '11543': [1536.2, 1541.45, 1783.72, 1086.22],
 '14732': [631.7, 667.05, 862.7, 843.45],
 '19585': [2190.67, 1392.77, 1822.37, 893.93],
 '9599': [300.2, 255.45, 371.1, 385.5],
 '438': [216.63, 196.95, 249.55, 285.5],
 '637': [693.9, 571.2, 528.15, 407.9],
 '15083': [1306.3, 1144.5, 1266.85, 1378.85],
 '7': [451.6, 406.3, 448.4, 634.55],
 '21238': [687.0, 544.3, 581.2, 669.0],
 '212': [221.62, 209.78, 228.66, 231.45],
 '25780': [1655.7, 1458.25, 1616.6, 1625.05],
 '8075': [1916.8, 1696.35, 1897.75, 1809.95],
 '13': [5440.5, 5326.55, 7569.75, 8073.25],
 '22': [1811.6, 1885.75, 2260.5, 2499.1],
 '25': [2251.0, 2247.5, 2495.85, 3219.55],
 '11703': [4946.5, 4738.05, 5484.6, 4956.1],
 '1270': [527.45, 499.8, 571.3, 621.3],
 '3563': [879.45, 837.2, 1216.95, 1864.25],
 '17875': [2000.5, 2008.35, 2842.95, 2873.55],
 '21690': [15190.0, 13911.25, 17345.9, 9985.1],
 '157': [6722.0, 6216.1, 7193.85, 6014.55],
 '236': [2299.6, 2270.0, 2391.85, 2927.7],
 '6066': [601.1, 607.2, 729.5, 978.1],
 '275': [1164.4, 1097.4, 1239.5, 1265.15],
 '5900': [1151.1, 1037.65, 1163.25, 1186.8],
 '16669': [7682.5, 7574.75, 9077.45, 9725.55],
 '16675': [1970.0, 1845.3, 1637.05, 1567.7],
 '317': [8641.0, 8404.5, 6868.35, 7191.4],
 '335': [2741.8, 2559.6, 2857.8, 3201.9],
 '2263': [156.26, 149.93, 174.55, 196.55],
 '4745': [110.22, 97.41, 115.88, 121.05],
 '422': [1165.6, 1087.8, 1363.45, 1583.2],
 '10604': [1848.4, 1630.7, 1602.55, 1425.25],
 '11373': [329.9, 333.7, 369.6, 336.0],
 '2181': [30000.0, 27033.4, 35895.15, 30596.6],
 '526': [306.85, 261.26, 303.45, 299.9],
 '547': [5436.0, 4748.25, 4793.0, 5463.55],
 '6994': [384.45, 434.45, 608.65, 678.15],
 '342': [3479.6, 3365.7, 5269.5, 3408.1],
 '628': [157.71, 140.94, 199.66, 147.0],
 '760': [609.6, 630.05, 793.5, 663.55],
 '19257': None,
 '2144': [1531.8, 1135.2, 1216.6, 1430.95],
 '694': [1480.0, 1458.45, 1469.0, 1497.25],
 '20374': [382.4, 380.65, 414.0, 479.15],
 '8311': [1594.0, 2100.75, 2106.3, 1565.9],
 '15141': [2550.5, 2474.15, 2804.25, 2963.5],
 '17094': [321.85, 344.45, 414.1, 397.35],
 '1901': [2736.3, 2899.05, 3509.15, 3606.55],
 '772': [462.75, 495.8, 506.85, 613.1],
 '10940': [6025.5, 5578.75, 5959.65, 4524.05],
 '19913': [3972.4, 3593.6, 3829.05, 4747.25],
 '881': [1155.9, 1133.25, 1255.15, 1212.26],
 '910': [5324.0, 5100.7, 4842.1, 4762.0],
 '676': [360.95, 355.85, 467.0, 527.05],
 '1023': [187.38, 180.51, 213.77, 165.05],
 '4717': [181.6, 158.14, 208.67, 212.7],
 '7406': [1398.2, 1404.4, 1514.15, 1194.15],
 '1232': [2634.8, 2407.25, 2681.4, 2379.85],
 '9819': [1548.3, 1460.55, 1704.7, 1855.35],
 '11872': [440.9, 493.15, 580.1, 478.8],
 '7229': [1569.4, 1557.95, 1909.9, 1431.5],
 '1333': [1889.7, 1689.25, 1870.0, 1573.35],
 '14592': [664.7, 629.5, 719.6, 465.8],
 '467': [713.6, 625.2, 641.8, 562.95]}


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
                round(((last_traded_price - closed_price) / (closed_price or 1)) * 100, 2),
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
    global ws2_completed, ws3_completed, prev_batches

    while True:
        for i, batch in enumerate(batch_list):
            logger.info(f"{ws_id}: Subscribing batch {i+1}/{len(batch_list)} with {len(batch)} tokens")

            if not ws.wsapp or not ws.wsapp.sock or not ws.wsapp.sock.connected:
                logger.error(f"{ws_id}: WebSocket disconnected, exiting cycle.")
                return

            try:
                if prev_batches:
                    ws.unsubscribe(ws_id, mode_2, [{"exchangeType": 2, "tokens": list(prev_batches)}])
                    time.sleep(2)

                prev_batches = tuple(batch)
                ws.subscribe(ws_id, mode_2, [{"exchangeType": 2, "tokens": list(batch)}])
                time.sleep(2)

            except Exception as e:
                logger.error(f"{ws_id}: Exception during subscribe/unsubscribe: {e}")
                return



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
        logger.info(f"Equity data Updating")
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


def fetch_equity_data():
    sws1.subscribe(correlation_id_1, mode_1, equity_list)

def on_open_1(wsapp):
    """Keep WebSocket 1 always subscribed for equity data."""
    logger.info("Equity WebSocket Opened") 
    fetch_equity_data()
    # threading.Thread(target=fetch_equity_data, daemon=True).start()

def on_open_2(wsapp):
    """WebSocket 2 - Process first half of option tokens in order."""
    logger.info("Options WebSocket 2 Opened")
    threading.Thread(target=cycle_subscriptions, args=(sws2, correlation_id_2, copy.deepcopy(ws2_batches)), daemon=True).start()
    # cycle_subscriptions(sws2, correlation_id_2, copy.deepcopy(ws2_batches))

def on_error(wsapp, error):
    print(f"Error: {wsapp} - {error}")
    close_websockets()


def on_close(wsapp):
    logger.warning(f"WebSocket Closed: Code={wsapp}")


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

def safe_connect(sws, name, retries=2):
    for attempt in range(retries):
        try:
            sws.connect()
            logger.info(f"{name} WebSocket connected.")
            return
        except Exception as e:
            logger.error(f"{name} WebSocket failed to connect: {e}. Retrying...")
            time.sleep(5)
    logger.critical(f"{name} WebSocket failed after {retries} attempts. Exiting.")



def close_websockets():
    try:
        sws1.close_connection()
        sws2.close_connection()
        logger.info("WebSockets disconnected successfully.")
    except Exception as e:
        logger.error(f"Error while closing WebSockets: {e}")


# if __name__ == "__main__":
    # Use the PORT from the environment if available (important for Render)
# Run WebSockets in separate threads

threading.Thread(target=lambda: safe_connect(sws1, "Equity"), daemon=True).start()

threading.Thread(target=lambda: safe_connect(sws2, "Options"), daemon=True).start()


port = int(os.environ.get("PORT", 5000))
app.run(host="0.0.0.0", port=5000)


# Keep script running
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    logger.info("Interrupt received. Closing WebSockets...")
    close_websockets()

