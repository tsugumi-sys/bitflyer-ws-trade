import sys
from typing import List, Tuple
from datetime import timedelta

import pandas as pd

sys.path.append(".")
from bitflyer_hft_bot.db import models


def get_ohlcv_df(ohlcv_data: List[models.OHLCV], time_span: int) -> pd.DataFrame:
    """Get ohlcv data as pd.Dataframe

    Args:
        ohlcv_data (_type_): return of query.
        time_span (int): seconds.

    Returns:
        pd.DataFrame: pandas dataframe with the columns timestamp, open, high, low, close, volume
    """
    data = {"timestamp": [], "open": [], "high": [], "low": [], "close": [], "volume": []}
    for item in ohlcv_data:
        data["open"].append(item.open)
        data["high"].append(item.high)
        data["low"].append(item.low)
        data["close"].append(item.close)
        data["volume"].append(item.volume)
        data["timestamp"].append(item.timestamp)

    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["timestamp"] += timedelta(hours=9)
    df = df.set_index("timestamp")
    df = df.sort_index()
    return df


def get_predict_df(predict_data: List[models.PREDICT]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Get Predict Dataframe

    Args:
        predict_data (List[models.PREDICT]): predict data

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: buy_df, sell_df with columns timestamp, side, price, size, predict_value, symbol
    """
    data = {"timestamp": [], "side": [], "price": [], "size": [], "predict_value": [], "symbol": [], "is_entry": []}
    for item in predict_data:
        data["timestamp"].append(item.timestamp)
        data["side"].append(item.side)
        data["price"].append(item.price)
        data["size"].append(item.size)
        data["predict_value"].append(item.predict_value)
        data["symbol"].append(item.symbol)
        data["is_entry"].append(item.is_entry)

    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df["timestamp"] += timedelta(hours=9)
    df = df.sort_index()

    buy_df = df.loc[df["side"] == "BUY"]
    sell_df = df.loc[df["side"] == "SELL"]
    buy_df = buy_df.set_index("timestamp")
    sell_df = sell_df.set_index("timestamp")
    return buy_df, sell_df


def match_timestamp_for_ohlcv(ohlcv_df: pd.DataFrame, target_df: pd.DataFrame, time_span: int) -> pd.DataFrame:
    """Match timestamp for ohlcv dataframe

    Args:
        ohlcv_df (pd.DataFrame): ohlcv dataframe
        target_df (pd.DataFrame): buy_df or sell_df
        time_span (int): Time span. seconds.

    Returns:
        matched_timstamp_dataframe (pd.Dataframe):
    """
    target_df["acutual_timestamp"] = target_df.index
    column_names = target_df.columns.values.tolist()
    matched_timestamp_df = pd.DataFrame({"timestamp": ohlcv_df.index})
    for idx, timestamp in enumerate(ohlcv_df.index):
        target_row = target_df.loc[(target_df.index >= timestamp) & (target_df.index < pd.Timedelta(time_span, unit="s") + timestamp)]
        if len(target_row) > 0:
            target_row = target_row.iloc[0, :]
            matched_timestamp_df.loc[idx, column_names] = target_row

    return matched_timestamp_df.set_index("timestamp")
