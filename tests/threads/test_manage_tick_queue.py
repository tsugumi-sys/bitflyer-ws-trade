import asyncio
import sys
import unittest
import logging
from unittest.mock import MagicMock, patch, PropertyMock

sys.path.append(".")
from bitflyer_hft_bot.threads.manage_tick_queue import TickQueueManager
from bitflyer_hft_bot.utils.queue_and_trade_manager import QueueAndTradeManager
from bitflyer_hft_bot.utils.custom_exceptions import ConnectionFailedError
from bitflyer_hft_bot.db import models
from bitflyer_hft_bot.db.database import initialize_database

database_engine, SessionLocal = initialize_database(uri=None)


class TestTickQueueManager(unittest.TestCase):
    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)
        self.dummy_symbol = "Uncoin"

    def setUp(self) -> None:
        models.Base.metadata.create_all(database_engine)

    def tearDown(self) -> None:
        models.Base.metadata.drop_all(database_engine)

    @patch("gmo_hft_bot.db.crud.insert_tick_item")
    @patch("gmo_hft_bot.db.crud.create_ohlcv_from_ticks")
    def test_with_ten_items_in_queue(self, mocked_create_ohlcv_func, mocked_insert_tick_func):
        queue_and_trade_manager = QueueAndTradeManager(api_key="dummy", api_secret="dummy")
        mock_running = PropertyMock(side_effect=[True, False])
        TickQueueManager.RUNNING = mock_running

        queue_count = 10
        dummy_items = [{"dummy_key": "dummy_value"} for _ in range(queue_count)]
        for item in dummy_items:
            queue_and_trade_manager.add_ticks_queue(item)

        orderbook_queue_manager = TickQueueManager()

        asyncio.run(
            orderbook_queue_manager.run(
                symbol=self.dummy_symbol,
                time_span=5,
                max_tick_table_rows=10,
                max_ohlcv_table_rows=10,
                logger=logging.getLogger("testLogger"),
                queue_and_trade_manager=queue_and_trade_manager,
                SessionLocal=SessionLocal,
            )
        )

        self.assertEqual(mocked_insert_tick_func.call_count, 10)
        self.assertEqual(mocked_create_ohlcv_func.call_count, 1)

    @patch("gmo_hft_bot.db.crud.insert_tick_item")
    @patch("gmo_hft_bot.db.crud.create_ohlcv_from_ticks")
    def test_with_zero_item_in_queue(self, mocked_create_ohlcv_func, mocked_insert_tick_func):
        queue_and_trade_manager = QueueAndTradeManager(api_key="dummy", api_secret="dummy")
        mock_running = PropertyMock(side_effect=[True, False])
        TickQueueManager.RUNNING = mock_running

        orderbook_queue_manager = TickQueueManager()
        asyncio.run(
            orderbook_queue_manager.run(
                symbol=self.dummy_symbol,
                time_span=5,
                max_tick_table_rows=10,
                max_ohlcv_table_rows=10,
                logger=logging.getLogger("testLogger"),
                queue_and_trade_manager=queue_and_trade_manager,
                SessionLocal=SessionLocal,
            )
        )

        self.assertEqual(mocked_insert_tick_func.call_count, 0)
        self.assertEqual(mocked_create_ohlcv_func.call_count, 1)

    @patch("gmo_hft_bot.utils.queue_and_trade_manager.QueueAndTradeManager.get_ticks_queue_size", MagicMock(side_effect=ConnectionFailedError()))
    def test_when_asyncio_timeout_error(self):
        queue_and_trade_manager = QueueAndTradeManager(api_key="dummy", api_secret="dummy")
        mock_running = PropertyMock(side_effect=[True, False])
        TickQueueManager.RUNNING = mock_running

        orderbook_queue_manager = TickQueueManager()
        with self.assertRaises(ConnectionFailedError):
            asyncio.run(
                orderbook_queue_manager.run(
                    symbol=self.dummy_symbol,
                    time_span=5,
                    max_tick_table_rows=10,
                    max_ohlcv_table_rows=10,
                    logger=logging.getLogger("testLogger"),
                    queue_and_trade_manager=queue_and_trade_manager,
                    SessionLocal=SessionLocal,
                )
            )
