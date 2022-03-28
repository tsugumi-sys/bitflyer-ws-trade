import os
import sys
import asyncio
import logging
import multiprocessing
from typing import Optional, Tuple
import traceback

from dotenv import load_dotenv
import sqlalchemy

sys.path.append(".")
from bitflyer_hft_bot.utils.queue_and_trade_manager import QueueAndTradeManager
from bitflyer_hft_bot.threads.manage_orderbook_queue import OrderbookQueueManager
from bitflyer_hft_bot.threads.manage_tick_queue import TickQueueManager
from bitflyer_hft_bot.threads.trade import Trader
from bitflyer_hft_bot.db import models
from bitflyer_hft_bot.db.database import initialize_database
from bitflyer_hft_bot.utils.custom_exceptions import ConnectionFailedError
from bitflyer_hft_bot.utils.logger_utils import LOGGER_FORMAT, worker_configurer


async def run_manage_queue_and_trading(
    symbol: str,
    time_span: int,
    max_orderbook_table_rows: int,
    max_tick_table_rows: int,
    max_ohlcv_table_rows: int,
    logger: logging.Logger,
    queue_and_trade_manager: QueueAndTradeManager,
    SessionLocal: Optional[sqlalchemy.orm.Session] = None,
    database_uri: Optional[str] = None,
):
    if SessionLocal is None and database_uri is None:
        logger.error("database_uri is needed if SessionLocal is None.")

    orderbook_queue_manager = OrderbookQueueManager()
    tick_queue_manager = TickQueueManager()
    trader = Trader()

    if SessionLocal is None:
        # Run in multiprocessing.Process

        # Avoid AttributeError: Can't pickle local object 'create_engine.<locals>.connect'
        database_engine, SessionLocal = initialize_database(uri=database_uri)
        try:
            # Initialize sqlite3 in-memory database
            models.Base.metadata.create_all(database_engine)

            await asyncio.gather(
                tick_queue_manager.run(
                    symbol=symbol,
                    time_span=time_span,
                    max_tick_table_rows=max_tick_table_rows,
                    max_ohlcv_table_rows=max_ohlcv_table_rows,
                    logger=logger,
                    queue_and_trade_manager=queue_and_trade_manager,
                    SessionLocal=SessionLocal,
                ),
                orderbook_queue_manager.run(
                    symbol=symbol,
                    max_orderbook_table_rows=max_orderbook_table_rows,
                    logger=logger,
                    queue_and_trade_manager=queue_and_trade_manager,
                    SessionLocal=SessionLocal,
                ),
                trader.run(
                    symbol=symbol,
                    trade_time_span=time_span,
                    logger=logger,
                    queue_and_trade_manager=queue_and_trade_manager,
                    SessionLocal=SessionLocal,
                ),
            )
        except ConnectionFailedError:
            # Clear in-memory DB
            models.Base.metadata.drop_all(database_engine)

            # Raise ConnectionFailedError again so that restart from main process.
            logger.error(traceback.format_exc())
            raise ConnectionFailedError
    else:
        await asyncio.gather(
            tick_queue_manager.run(
                symbol=symbol,
                time_span=time_span,
                max_tick_table_rows=max_tick_table_rows,
                max_ohlcv_table_rows=max_ohlcv_table_rows,
                logger=logger,
                queue_and_trade_manager=queue_and_trade_manager,
                SessionLocal=SessionLocal,
            ),
            orderbook_queue_manager.run(
                max_orderbook_table_rows=max_orderbook_table_rows,
                logger=logger,
                queue_and_trade_manager=queue_and_trade_manager,
                SessionLocal=SessionLocal,
            ),
            trader.run(
                symbol=symbol,
                trade_time_span=time_span,
                logger=logger,
                queue_and_trade_manager=queue_and_trade_manager,
                SessionLocal=SessionLocal,
            ),
        )


def main(
    symbol: str,
    time_span: int,
    max_orderbook_table_rows: int,
    max_tick_table_rows: int,
    max_ohlcv_table_rows: int,
    queue_and_trade_manager: QueueAndTradeManager,
    database_uri: Optional[str] = None,
    logging_level: Optional[Tuple[str, int]] = None,
    logging_queue: Optional[multiprocessing.Queue] = None,
):
    if logging_queue is None:
        logger = logging.getLogger("QueueAndTradeLogger")

        if logging_level is not None:
            logger.setLevel(logging_level)

    else:
        logger = logging.getLogger("QueueAndTradeLogger")
        worker_configurer(logging_queue, logger.getEffectiveLevel())

        if logging_level is not None:
            logger.setLevel(logging_level)

    # Initialize database
    database_engine, SessionLocal = initialize_database(uri=database_uri)

    try:
        # Initialize sqlite3 in-memory database
        models.Base.metadata.create_all(database_engine)
        asyncio.run(
            run_manage_queue_and_trading(
                symbol=symbol,
                time_span=time_span,
                max_orderbook_table_rows=max_orderbook_table_rows,
                max_tick_table_rows=max_tick_table_rows,
                max_ohlcv_table_rows=max_ohlcv_table_rows,
                logger=logger,
                queue_and_trade_manager=queue_and_trade_manager,
                SessionLocal=SessionLocal,
            )
        )
    except ConnectionFailedError:
        # Clear in-memory DB
        models.Base.metadata.drop_all(database_engine)

        # Initialize sqlite3 in-memory DB
        models.Base.metadata.create_all(database_engine)

        logger.debug("Rerun queue_and_trade_threads")
        main(
            symbol=symbol,
            time_span=time_span,
            max_orderbook_table_rows=max_orderbook_table_rows,
            max_tick_table_rows=max_tick_table_rows,
            max_ohlcv_table_rows=max_ohlcv_table_rows,
            queue_and_trade_manager=queue_and_trade_manager,
            database_uri=database_uri,
            logging_level=logging_level,
            logging_queue=logging_queue,
        )

    # Clear in-memory DB again for next try
    models.Base.metadata.drop_all(database_engine)


if __name__ == "__main__":
    # Load .env file
    load_dotenv()

    logging.basicConfig(level=logging.DEBUG, format=LOGGER_FORMAT)
    queue_and_trade_manager = QueueAndTradeManager(api_key=os.environ["EXCHANGE_API_KEY"], api_secret=os.environ["EXCHANGE_API_SECRET"])

    database_engine, session_local = initialize_database(uri="sqlite:///example.db")
    symbol = "BTC_JPY"
    time_span = 5
    max_orderbook_table_rows = 1000
    max_tick_table_rows = 1000
    max_ohlcv_table_rows = 1000
    main(
        symbol=symbol,
        time_span=time_span,
        max_orderbook_table_rows=max_orderbook_table_rows,
        max_tick_table_rows=max_tick_table_rows,
        max_ohlcv_table_rows=max_ohlcv_table_rows,
        queue_and_trade_manager=queue_and_trade_manager,
        database_uri="sqlite:///example.db",
    )
