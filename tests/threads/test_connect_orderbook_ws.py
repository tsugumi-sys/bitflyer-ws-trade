import multiprocessing
import logging
import sys
import unittest
import time
import asyncio
from unittest.mock import PropertyMock, patch
import json

from tests.utils.dummy_gmo_websocket import dummy_gmo_websockt_server

sys.path.append(".")
from bitflyer_hft_bot.utils.queue_and_trade_manager import QueueAndTradeManager
from bitflyer_hft_bot.utils.bitflyer_websocket_subscriber import GmoWebsocketSubscriber
from bitflyer_hft_bot.threads.connect_orderbook_ws import ConnectOrderbookWs
from bitflyer_hft_bot.utils.custom_exceptions import ConnectionFailedError


class TestConnectOrderbookWs(unittest.TestCase):
    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)

        self.test_logger = logging.getLogger("testLogger")
        self.dummy_websocket_process = None
        self.dummy_websocket_url = "ws://localhost:8001/"
        self.dummy_symbol = "Uncoin"

    def setUp(self) -> None:
        process = multiprocessing.Process(target=dummy_gmo_websockt_server)
        process.start()
        self.dummy_websocket_process = process

        # Wait to open websocket server
        time.sleep(0.5)

    def tearDown(self) -> None:
        self.dummy_websocket_process.terminate()

    def test_with_normal_response(self):
        queue_and_trade_manager = QueueAndTradeManager(api_key="dummy_key", api_secret="dummy_secret")
        mock_running = PropertyMock(side_effect=[True, False])
        ConnectOrderbookWs.RUNNING = mock_running

        connect_order_ws = ConnectOrderbookWs()
        asyncio.run(
            connect_order_ws.run(
                ws_url=self.dummy_websocket_url,
                symbol=self.dummy_symbol,
                logger=self.test_logger,
                queue_and_trade_manager=queue_and_trade_manager,
            )
        )

        self.assertEqual(queue_and_trade_manager.get_orderbook_queue_size(), 1)

    @patch.object(GmoWebsocketSubscriber, "subscribe_orderbooks_msg")
    def test_with_error_response(self, mocked_subscriber_method):
        mocked_subscriber_method.return_value = json.dumps({"channel": "errorTooManyRequest"})

        queue_and_trade_manager = QueueAndTradeManager(api_key="dummy_key", api_secret="dummy_secret")
        mock_running = PropertyMock(side_effect=[True, False])
        ConnectOrderbookWs.RUNNING = mock_running

        connect_order_ws = ConnectOrderbookWs()
        asyncio.run(
            connect_order_ws.run(
                ws_url=self.dummy_websocket_url,
                symbol=self.dummy_symbol,
                logger=self.test_logger,
                queue_and_trade_manager=queue_and_trade_manager,
            )
        )

        self.assertEqual(queue_and_trade_manager.get_orderbook_queue_size(), 0)

    @patch.object(GmoWebsocketSubscriber, "subscribe_orderbooks_msg")
    def test_when_connection_closed(self, mocked_subscriber_method):
        mocked_subscriber_method.return_value = json.dumps({"channel": "stopWebsocketServer"})

        queue_and_trade_manager = QueueAndTradeManager(api_key="dummy_key", api_secret="dummy_secret")
        mock_running = PropertyMock(side_effect=[True, False])
        ConnectOrderbookWs.RUNNING = mock_running

        connect_order_ws = ConnectOrderbookWs()
        with self.assertRaises(ConnectionFailedError):
            asyncio.run(
                connect_order_ws.run(
                    ws_url=self.dummy_websocket_url,
                    symbol=self.dummy_symbol,
                    logger=self.test_logger,
                    queue_and_trade_manager=queue_and_trade_manager,
                )
            )

    @patch.object(GmoWebsocketSubscriber, "subscribe_orderbooks_msg")
    def test_when_asyncio_timeouterror(self, mocked_subscriber_method):
        mocked_subscriber_method.return_value = json.dumps({"channel": "asyncioTimeout"})

        queue_and_trade_manager = QueueAndTradeManager(api_key="dummy_key", api_secret="dummy_secret")
        mock_running = PropertyMock(side_effect=[True, False])
        ConnectOrderbookWs.RUNNING = mock_running

        connect_order_ws = ConnectOrderbookWs()
        with self.assertRaises(ConnectionFailedError):
            asyncio.run(
                connect_order_ws.run(
                    ws_url=self.dummy_websocket_url,
                    symbol=self.dummy_symbol,
                    logger=self.test_logger,
                    queue_and_trade_manager=queue_and_trade_manager,
                )
            )
