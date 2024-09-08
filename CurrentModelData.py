import HistoricalModelData as hmd
import SP500Data as sp
import yfinance as yf
import pandas as pd
from sklearn.linear_model import LinearRegression
import numpy as np
import psycopg2
import LoginInformation


def create_data(data_df):
    unique_symbols = data_df["symbol"].unique()
    data_df['volumeTrend'] = np.nan
    data_df['priceTrend'] = np.nan
     # Iterate through unique symbols
    for symbol in unique_symbols:
        symbol_data = data_df[data_df["symbol"] == symbol]
        print(symbol)
        data = symbol_data.iloc[-180:]
        data.reset_index(drop=True, inplace=True)
        last_date = data.iloc[-1]["date"]
        volumetrend = hmd.get_volume_trend(data)
        pricetrend = hmd.get_price_trend(data)
        matching_row = data_df[(data_df["date"] == last_date) & (data_df["symbol"] == symbol)]
        data_df.loc[matching_row.index, "volumeTrend"] = volumetrend
        data_df.loc[matching_row.index, "priceTrend"] = pricetrend
    daily_data = data_df[data_df["date"] == last_date]
    return daily_data


def get_current_model_data():
    data = hmd.get_data()
    data = create_data(data)
    data = hmd.standardize_data(data)
    print(data)
    hmd.add_to_sql(data)
