import sys
import asyncio
import logging
import traceback

import sqlalchemy

sys.path.append(".")
from bitflyer_hft_bot.utils.queue_and_trade_manager import QueueAndTradeManager
from bitflyer_hft_bot.db import crud
from bitflyer_hft_bot.utils.custom_exceptions import ConnectionFailedError


class OrderbookQueueManager:
    RUNNING = True

    async def run(
        self,
        symbol: str,
        max_orderbook_table_rows: int,
        logger: logging.Logger,
        queue_and_trade_manager: QueueAndTradeManager,
        SessionLocal: sqlalchemy.orm.Session,
    ):
        while self.RUNNING:
            try:
                with SessionLocal() as db:
                    # Save orderbook queue
                    qsize = queue_and_trade_manager.get_orderbook_queue_size()
                    if qsize > 0:
                        logger.debug(f"Orderbook queue count: {qsize}")
                        for _ in range(qsize):
                            item = queue_and_trade_manager.get_orderbook_queue_item()

                            logger.debug("Add orderbook queue item to DB")
                            crud.insert_board_items(db=db, symbol=symbol, insert_items=item, max_board_counts=max_orderbook_table_rows)

                await asyncio.sleep(0.0)
            except asyncio.TimeoutError:
                logger.debug("Trade thread has ended with asyncio.TimeoutError")
                raise ConnectionFailedError

            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error(e)
                raise ConnectionFailedError
