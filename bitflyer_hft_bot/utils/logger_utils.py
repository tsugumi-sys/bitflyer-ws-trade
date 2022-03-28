import sys
import logging
import logging.handlers
import multiprocessing
from typing import Callable, Tuple

sys.path.append(".")
from bitflyer_hft_bot.utils.queue_and_trade_manager import QueueAndTradeManager


# References: https://docs.python.org/ja/3/howto/logging-cookbook.html
# Functions to enable logging in multiprocessing.


LOGGER_FORMAT = "[%(asctime)s] [%(name)s] [%(levelname)s]: %(message)s (%(filename)s:%(lineno)d)"


def listener_configurer():
    root = logging.getLogger()
    file_handler = logging.handlers.RotatingFileHandler("./log/subprocesses.log", "a", maxBytes=100000, backupCount=1)
    stream_handler = logging.StreamHandler()
    f = logging.Formatter(LOGGER_FORMAT)
    file_handler.setFormatter(f)
    stream_handler.setFormatter(f)
    root.addHandler(file_handler)
    root.addHandler(stream_handler)


def listener_process(queue: multiprocessing.Queue, configurer: Callable, queue_and_trade_manager: QueueAndTradeManager):
    configurer()
    while True:
        try:
            record = queue.get()
            if record is None:  # We send this as a sentinel to tell the listener to quit.
                break

            if "gmo_hft_bot.utils.custom_exceptions.ConnectionFailedError" in record.getMessage():
                print("=" * 100)
                print("Execption detected")
                queue_and_trade_manager.update_subprocesses_alive_status(False)

            logger = logging.getLogger(record.name)
            logger.handle(record)  # No level or filter logic applied - just do it!
        except Exception:
            import sys
            import traceback

            print("Whoops! Problem:", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)


def worker_configurer(queue: multiprocessing.Queue, level: Tuple[str, int]):
    h = logging.handlers.QueueHandler(queue)  # Just the one handler needed
    root = logging.getLogger()
    root.addHandler(h)
    # send all messages, for demo; no other level or filter logic applied.
    root.setLevel(level)
