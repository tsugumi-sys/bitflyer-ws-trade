import sys
import asyncio
import logging
import traceback

import sqlalchemy

sys.path.append(".")
from bitflyer_hft_bot.utils.queue_and_trade_manager import QueueAndTradeManager
from bitflyer_hft_bot.db import crud
from bitflyer_hft_bot.utils.custom_exceptions import ConnectionFailedError


class TickQueueManager:
    RUNNING = True

    async def run(
        self,
        symbol: str,
        time_span: int,
        max_tick_table_rows: int,
        max_ohlcv_table_rows: int,
        logger: logging.Logger,
        queue_and_trade_manager: QueueAndTradeManager,
        SessionLocal: sqlalchemy.orm.Session,
    ):
        while self.RUNNING:
            try:
                with SessionLocal() as db:
                    # Save ticks queue
                    qsize = queue_and_trade_manager.get_ticks_queue_size()
                    if qsize > 0:
                        logger.debug(f"Tick queue count: {qsize}")
                        for _ in range(qsize):
                            item = queue_and_trade_manager.get_ticks_queue_item()

                            logger.debug("Add tick queue item to DB")
                            crud.insert_tick_item(db=db, symbol=symbol, insert_items=item, max_rows=max_tick_table_rows)

                    # Create ohlcv
                    crud.create_ohlcv_from_ticks(db=db, symbol=symbol, time_span=time_span, max_rows=max_ohlcv_table_rows)
                await asyncio.sleep(0.0)
            except asyncio.TimeoutError:
                logger.debug("Trade thread has ended with asyncio.TimeoutError")
                raise ConnectionFailedError

            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error(e)
                raise ConnectionFailedError
