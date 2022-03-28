import asyncio
import logging
from typing import Tuple, Optional
import multiprocessing
import sys

from dotenv import load_dotenv
import sqlalchemy

sys.path.append(".")
from bitflyer_hft_bot.utils.queue_and_trade_manager import QueueAndTradeManager
from bitflyer_hft_bot.threads.websocket_threads import run_multiple_websockets
from bitflyer_hft_bot.threads.queue_and_trade_threads import run_manage_queue_and_trading
from bitflyer_hft_bot.utils.logger_utils import LOGGER_FORMAT, listener_configurer, listener_process, worker_configurer

# Load .env file
load_dotenv()


def websocket_process(
    symbol: str,
    queue_and_trade_manager: QueueAndTradeManager,
    logging_level: Tuple[str, int],
    logging_queue: Optional[multiprocessing.Queue] = None,
):
    """Main process

    Args:
        symbol (str): Name of symbol
        queue_and_trade_manager (QueueAndTradeManager): gmo websockets
        logging_level (Tuple[str, int]): Logging level
        logging_queue (multiprocessing.Queue): Logging queue for multiprocessing. Default is None
    """
    if logging_queue is None:
        logger = logging.getLogger("WebsocketThredsLogger")
        formatter = logging.Formatter(LOGGER_FORMAT)
        file_handler = logging.handlers.RotatingFileHandler("./log/main_process.log", "a", maxBytes=100000, backupCount=1)
        file_handler.setFormatter(formatter)
        logging.getLogger().addHandler(file_handler)

        if logging_level is not None:
            logger.setLevel(logging_level)
    else:
        logger = logging.getLogger("WebsocketThredsLogger")
        worker_configurer(logging_queue, logger.getEffectiveLevel())

        if logging_level is not None:
            logger.setLevel(logging_level)

    asyncio.run(run_multiple_websockets(symbol=symbol, logger=logger, queue_and_trade_manager=queue_and_trade_manager))


def get_logging_process(logging_queue: multiprocessing.Queue, queue_and_trade_manager: QueueAndTradeManager) -> multiprocessing.Process:
    logging_process = multiprocessing.Process(
        target=listener_process,
        args=(
            logging_queue,
            listener_configurer,
            queue_and_trade_manager,
        ),
    )
    return logging_process


def queue_and_trade_task(
    symbol: str,
    time_span: int,
    max_orderbook_table_rows: int,
    max_tick_table_rows: int,
    max_ohlcv_table_rows: int,
    queue_and_trade_manager: QueueAndTradeManager,
    logging_level: Tuple[str, int],
    logging_queue: multiprocessing.Queue,
    SessionLocal: Optional[sqlalchemy.orm.Session] = None,
    database_uri: Optional[str] = None,
):
    logger = logging.getLogger("QueueAndTradeLogger")
    worker_configurer(logging_queue, logger.getEffectiveLevel())
    logger.setLevel(logging_level)
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
            database_uri=database_uri,
        )
    )


def get_manage_queue_and_trade_process(
    symbol: str,
    time_span: int,
    max_orderbook_table_rows: int,
    max_tick_table_rows: int,
    max_ohlcv_table_rows: int,
    queue_and_trade_manager: QueueAndTradeManager,
    logging_level: Tuple[str, int],
    logging_queue: multiprocessing.Queue,
    database_uri: str,
) -> multiprocessing.Process:
    """Sub processes. Logging process and manage_queue_and_trade process.

    Args:
        symbol (str): Name of symbol.
        time_span (int): Time span (seconds).
        max_orderbook_table_rows (int): Number of max orderbook table rows.
        max_tick_table_rows (int): Number of max tick table rows.
        max_ohlcv_table_rows (int): Number of max ohlcv table rows.
        queue_and_trade_manager (QueueAndTradeManager): Manage queue class.
        logging_level (Tuple[str, int]): Logging level.
        logging_queue (multiprocessing.Queue): Queue of multiprocessing.

    Return:
        (logging_process, queue_and_trade_process)
    """

    queue_and_trade_process = multiprocessing.Process(
        target=queue_and_trade_task,
        args=(
            symbol,
            time_span,
            max_orderbook_table_rows,
            max_tick_table_rows,
            max_ohlcv_table_rows,
            queue_and_trade_manager,
            logging_level,
            logging_queue,
            None,  # Avoid AttributeError: Can't pickle local object 'create_engine.<locals>.connect'
            database_uri,
        ),
    )

    return queue_and_trade_process
