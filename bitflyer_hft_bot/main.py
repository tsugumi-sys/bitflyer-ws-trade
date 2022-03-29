import logging
import multiprocessing
import os
import sys
import traceback

from dotenv import load_dotenv

# Avoid AttributeError: module 'sqlalchemy' has no attribute 'orm'
import sqlalchemy.orm  # noqa: F401

sys.path.append(".")
from bitflyer_hft_bot.utils.logger_utils import LOGGER_FORMAT
from bitflyer_hft_bot.utils.custom_exceptions import ConnectionFailedError
from bitflyer_hft_bot.utils.queue_and_trade_manager import QueueAndTradeManager
from bitflyer_hft_bot.processes import get_logging_process, websocket_process, get_manage_queue_and_trade_process

# Load .env file
load_dotenv()

logger = logging.getLogger("rootLogger")


def main():
    logging_level = logging.DEBUG
    logging_queue = multiprocessing.Manager().Queue(-1)
    logging.basicConfig(level=logging_level, format=LOGGER_FORMAT)

    database_uri = "sqlite:///example.db"

    queue_and_trade_manager = QueueAndTradeManager(api_key=os.environ["EXCHANGE_API_KEY"], api_secret=os.environ["EXCHANGE_API_SECRET"])
    symbol = "FX_BTC_JPY"
    time_span = 5
    max_orderbook_table_rows = 1000
    max_tick_table_rows = 1000
    max_ohlcv_table_rows = 100000

    logging_process = get_logging_process(logging_queue=logging_queue, queue_and_trade_manager=queue_and_trade_manager)
    queue_and_trade_process = get_manage_queue_and_trade_process(
        symbol=symbol,
        time_span=time_span,
        max_orderbook_table_rows=max_orderbook_table_rows,
        max_tick_table_rows=max_tick_table_rows,
        max_ohlcv_table_rows=max_ohlcv_table_rows,
        queue_and_trade_manager=queue_and_trade_manager,
        logging_level=logging_level,
        logging_queue=logging_queue,
        database_uri=database_uri,
    )

    try:
        logging_process.start()
        queue_and_trade_process.start()

        websocket_process(symbol=symbol, queue_and_trade_manager=queue_and_trade_manager, logging_level=logging_level)

        logging_process.join()
        queue_and_trade_process.join()
    except ConnectionFailedError:
        logging_process.terminate()
        queue_and_trade_process.terminate()

        logger.warning("Rerun bot")

        main()

    except Exception as e:
        logger.error(traceback.format_exc())
        logger.error(e)

        logging_process.terminate()
        queue_and_trade_process.terminate()


if __name__ == "__main__":
    main()
