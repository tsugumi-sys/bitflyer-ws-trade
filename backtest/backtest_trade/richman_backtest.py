import sys
from typing import Tuple

import pandas as pd
import numpy as np

sys.path.append(".")
from backtest.backtest_trade.backtest import backtest


def richman_backtest(ohlcv_df: pd.DataFrame, buy_df: pd.DataFrame, sell_df: pd.DataFrame) -> pd.DataFrame:
    buy_executed, buy_price = trade_executed(ohlcv_df, buy_df, side="buy")
    sell_executed, sell_price = trade_executed(ohlcv_df, sell_df, side="sell")
    predict_buy_entry = (buy_df["is_entry"] == True).to_numpy()  # noqa: E712
    predict_sell_entry = (sell_df["is_entry"] == True).to_numpy()  # noqa: E712
    # If you use two different model for buy and sell, you have to custom priority_buy_entry.
    # Else, priority_buy_entry is the same as buy_executed.
    prioty_buy_entry = predict_buy_entry

    cumulative_return, possition, buy_entry_prices, sell_entry_prices, buy_exit_prices, sell_exit_prices = backtest(
        close=ohlcv_df["close"].to_numpy(),
        predict_buy_entry=predict_buy_entry,
        predict_sell_entry=predict_sell_entry,
        priority_buy_entry=prioty_buy_entry,
        buy_executed=buy_executed,
        sell_executed=sell_executed,
        buy_price=buy_price,
        sell_price=sell_price,
    )

    result_df = pd.DataFrame(
        {
            "cumulative_return": cumulative_return,
            "position": possition,
            "buy_entry_price": buy_entry_prices,
            "sell_entry_price": sell_entry_prices,
            "buy_exit_price": buy_exit_prices,
            "sell_exit_price": sell_exit_prices,
        },
        index=ohlcv_df.index,
    )
    result_df["buy_executed"] = buy_executed
    result_df["sell_executed"] = sell_executed
    return result_df


def trade_executed(ohlcv_df: pd.DataFrame, target_df: pd.DataFrame, side: str) -> Tuple[np.ndarray, np.ndarray]:
    """Trade Executed and entry price

    Args:
        ohlcv_df (pd.DataFrame): ohlcv dataframe
        target_df (pd.DataFrame): buy_df or sell_df
        side (str): `buy` or `sell`

    Raises:
        ValueError: raises if side is wrong.

    Returns:
        Tuple[np.ndarray, np.ndarray]: executed and prices
    """
    if side.upper() not in ["BUY", "SELL"]:
        raise ValueError("side should be buy or sell")

    merged_df = ohlcv_df.merge(target_df["price"], how="left", left_index=True, right_index=True)
    if side.upper() == "BUY":
        executed = merged_df["price"] >= merged_df["low"].shift(-1)
        return executed.to_numpy(), merged_df["price"].to_numpy()
    else:
        executed = merged_df["price"] <= merged_df["high"].shift(-1)
        return executed.to_numpy(), merged_df["price"].to_numpy()


def entry_by_prediction(ohlcv_df: pd.DataFrame, target_df: pd.DataFrame) -> np.ndarray:
    merged_df = ohlcv_df.merge(target_df[["size", "price"]], how="left", left_index=True, right_index=True)
    # dont entry if size is 0 and price is 0. Check gmo_hft_bot/queue_and_trade_threads.py
    return ((merged_df["size"] != 0.0) & (merged_df["price"] != 0.0)).to_numpy()
