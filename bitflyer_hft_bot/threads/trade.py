import sys
import asyncio
import logging
import time

# import aiohttp
import traceback

import sqlalchemy

sys.path.append(".")
from bitflyer_hft_bot.utils.queue_and_trade_manager import QueueAndTradeManager
from bitflyer_hft_bot.db import crud
from bitflyer_hft_bot.utils.custom_exceptions import ConnectionFailedError


class Trader:
    RUNNING = True

    async def run(
        self,
        symbol: str,
        trade_time_span: int,
        logger: logging.Logger,
        queue_and_trade_manager: QueueAndTradeManager,
        SessionLocal: sqlalchemy.orm.Session,
    ):
        """Trade threads

        Args:
            symbol (str): Name of symbol
            trade_time_span (int): Time span of trade. seconds
            logger (logging.Logger): logger
            queue_and_trade_manager (QueueAndTradeManager): Quene and trade manager
            SessionLocal (sqlalchemy.orm.Session): Session of sqlalchemy

        Raises:
            ConnectionFailedError: Raise if threads stopped.
        """
        before_timestamp_per_span = None

        # [Note]: Only local online backtest
        before_buy_order_price = None
        before_sell_order_price = None
        while self.RUNNING:
            try:
                current_timestamp_per_span = time.time() // trade_time_span
                if before_timestamp_per_span is not None and current_timestamp_per_span > before_timestamp_per_span:

                    with SessionLocal() as db:
                        predict_info = crud.get_prediction_info(db=db, symbol=symbol)

                    if predict_info.is_buy_entry is True:
                        # Buy
                        logger.info("Buy order.")
                        # Dummy order
                        # request_url, headers = queue_and_trade_manager.test_http_private_request_args()
                        # async with aiohttp.ClientSession() as session:
                        #     async with session.get(request_url, headers=headers) as response:
                        #         _ = await response.json()

                    if predict_info.is_sell_entry is True:
                        # Sell
                        logger.info("Sell order")
                        # Dummy order
                        # request_url, headers = queue_and_trade_manager.test_http_private_request_args()
                        # async with aiohttp.ClientSession() as session:
                        #     async with session.get(request_url, headers=headers) as response:
                        #         _ = await response.json()

                    with SessionLocal() as db:
                        buy_predict_item = {
                            "side": "BUY",
                            "size": predict_info.buy_size,
                            "price": predict_info.buy_price,
                            "predict_value": predict_info.buy_predict_value,
                            "symbol": symbol,
                            "is_entry": predict_info.is_buy_entry,
                        }
                        sell_predict_item = {
                            "side": "SELL",
                            "size": predict_info.sell_size,
                            "price": predict_info.sell_price,
                            "predict_value": predict_info.sell_predict_value,
                            "symbol": symbol,
                            "is_entry": predict_info.is_sell_entry,
                        }
                        crud.insert_predict_items(db=db, insert_items=[buy_predict_item, sell_predict_item])

                        # [Note]: Only local online backtest
                        before_buy_order_price = predict_info.buy_price
                        before_sell_order_price = predict_info.sell_price
                else:
                    # Execution check
                    with SessionLocal() as db:
                        buy_board_items, sell_board_items = crud.get_current_board(db=db, symbol=symbol)

                    if len(buy_board_items) > 0 and len(sell_board_items) > 0 and before_buy_order_price is not None:
                        update_best_bid, update_best_ask = buy_board_items[-1], sell_board_items[0]
                        update_best_bid_price = update_best_bid.price
                        update_best_ask_price = update_best_ask.price
                        update_predict_items = []
                        if update_best_bid_price > before_buy_order_price:
                            update_buy_item = {
                                "side": "TrackBestBuy",
                                "size": 0,
                                "price": update_best_bid_price,
                                "predict_value": 0,
                                "symbol": symbol,
                                "is_entry": False,
                            }
                            update_predict_items.append(update_buy_item)
                            before_buy_order_price = update_best_bid_price

                        if update_best_ask_price < before_sell_order_price:
                            update_sell_item = {
                                "side": "TrackBestSell",
                                "size": 0,
                                "price": update_best_ask_price,
                                "predict_value": 0,
                                "symbol": symbol,
                                "is_entry": False,
                            }
                            update_predict_items.append(update_sell_item)
                            before_sell_order_price = update_best_ask_price

                        if len(update_predict_items) > 0:
                            with SessionLocal() as db:
                                crud.insert_predict_items(db=db, insert_items=update_predict_items)

                    await asyncio.sleep(0.5)

                before_timestamp_per_span = current_timestamp_per_span

                await asyncio.sleep(0.0)
            except asyncio.TimeoutError:
                logger.debug("Trade thread has ended with asyncio.TimeoutError")
                raise ConnectionFailedError

            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error(e)
                raise ConnectionFailedError
