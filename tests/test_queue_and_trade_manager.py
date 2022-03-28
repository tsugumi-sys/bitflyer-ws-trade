import unittest
import sys
import time

sys.path.append("./gmo-websocket/")
from bitflyer_hft_bot.utils.queue_and_trade_manager import QueueAndTradeManager


class TestQueueAndTradeManager(unittest.TestCase):
    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)
        self.dummy_item = {"i": "abcd"}

    def test_enable_trade(self):
        manager = QueueAndTradeManager(api_key="cscsd", api_secret="acsdca")
        self.assertFalse(manager.enable_trade)

        manager._enable_trade()
        self.assertTrue(manager.enable_trade)

    def test_disable_trade(self):
        manager = QueueAndTradeManager(api_key="cscsd", api_secret="acsdca")
        manager._enable_trade()
        self.assertTrue(manager.enable_trade)

        manager._disable_trade()
        self.assertFalse(manager.enable_trade)

    def test_add_orderbook_queue(self):
        manager = QueueAndTradeManager(api_key="cscsd", api_secret="acsdca")
        self.assertEqual(manager.orderbook_queue.qsize(), 0)

        manager.add_orderbook_queue(self.dummy_item)
        self.assertEqual(manager.orderbook_queue.qsize(), 1)

    def test_get_orderbook_queue_size(self):
        manager = QueueAndTradeManager(api_key="cscsd", api_secret="acsdca")
        self.assertEqual(manager.orderbook_queue.qsize(), 0)
        self.assertEqual(manager.get_orderbook_queue_size(), 0)

        manager.add_orderbook_queue(self.dummy_item)
        self.assertEqual(manager.orderbook_queue.qsize(), 1)
        self.assertEqual(manager.get_orderbook_queue_size(), 1)

    def test_get_orderbook_queue_item(self):
        manager = QueueAndTradeManager(api_key="cscsd", api_secret="acsdca")
        # Add item
        manager.add_orderbook_queue(self.dummy_item)
        self.assertEqual(manager.orderbook_queue.qsize(), 1)

        time.sleep(0.1)  # Sometime adding queue process gets behind than Queue.get.
        item = manager.get_orderbook_queue_item()
        self.assertEqual(item, self.dummy_item)
        self.assertEqual(manager.orderbook_queue.qsize(), 0)

    def test_add_ticks_queue(self):
        manager = QueueAndTradeManager(api_key="cscsd", api_secret="acsdca")
        self.assertEqual(manager.ticks_queue.qsize(), 0)
        manager.add_ticks_queue(self.dummy_item)
        self.assertEqual(manager.ticks_queue.qsize(), 1)

    def test_get_ticks_queue_item(self):
        manager = QueueAndTradeManager(api_key="cscsd", api_secret="acsdca")
        # Add item
        manager.add_ticks_queue(self.dummy_item)
        self.assertEqual(manager.ticks_queue.qsize(), 1)

        time.sleep(0.1)
        item = manager.get_ticks_queue_item()
        self.assertEqual(item, self.dummy_item)
        self.assertEqual(manager.ticks_queue.qsize(), 0)
