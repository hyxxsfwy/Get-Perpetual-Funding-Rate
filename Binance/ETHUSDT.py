import math
import time

import pandas as pd
import requests
from dateutil import parser
from tqdm import tqdm  # (Optional, used for progress-bars)

symbol = "ETHUSDT"
count = 500


def get_ticker_funding0(symbol) -> pd.DataFrame:
    URL = f"https://fapi.binance.com/fapi/v1/fundingRate?symbol={symbol}"
    r = requests.get(url=URL)
    r_json = r.json()
    funding_rates_df = pd.json_normalize(r_json)
    return funding_rates_df.rename(columns={"fundingTime": "timestamp"})


def get_ticker_funding(symbol, start_time, limit) -> pd.DataFrame:
    URL = f"https://fapi.binance.com/fapi/v1/fundingRate?symbol={symbol}&startTime={start_time}&limit={limit}"
    r = requests.get(url=URL)
    r_json = r.json()
    funding_rates_df = pd.json_normalize(r_json)
    return funding_rates_df.rename(columns={"fundingTime": "timestamp"})


def minutes_of_new_data(symbol, data):
    if len(data) > 0:
        old = parser.parse(data["timestamp"].iloc[-1])
    elif symbol == "ETHUSDT":
        old = get_ticker_funding(symbol=symbol, start_time=1568003500000, limit=10)[
            "timestamp"
        ].iloc[0]
    if symbol == "ETHUSDT":
        new = get_ticker_funding0(symbol=symbol)["timestamp"].iloc[-1]
    return old, new


def get_funding_rate(symbol):
    filename = "./data/%s-funding-rate.csv" % (symbol)
    data_df = pd.DataFrame()
    oldest_point, newest_point = minutes_of_new_data("ETHUSDT", data_df)
    delta_period = (newest_point - oldest_point) / 28800000
    available_data = math.ceil(delta_period)
    rounds = math.ceil(available_data / count)
    if rounds > 0:
        print("Downloading " + symbol + " funding rate ...")
        for round_num in tqdm(range(rounds)):
            time.sleep(0.2)
            new_time = oldest_point + round_num * count * 8 * 3600000
            data = get_ticker_funding(symbol=symbol, start_time=new_time, limit=count)
            temp_df = pd.DataFrame(data)
            data_df = pd.concat([data_df, temp_df])

    data_df["timestamp"] = data_df["timestamp"] // 1000
    data_df["timestamp"] = pd.to_datetime(data_df["timestamp"], utc=True, unit="s")
    data_df.set_index("timestamp", inplace=True)
    data_df.index = data_df.index.tz_convert("Asia/Shanghai")
    data_df.to_csv(filename)
    print(data_df.tail())

    return data_df


get_funding_rate("ETHUSDT")
