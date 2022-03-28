import json


class BitflyerWebsocketSubscriber:
    SUBSCRIBE_ORDERBOOKS_MESSAGE = {"method": "subscribe", "params": {"channel": "lightning_board_snapshot_"}}
    SUBSCRIBE_TRADES_MESSAGE = {"method": "subscribe", "params": {"channel": "lightning_executions_"}}

    def subscribe_orderbooks_msg(self, symbol: str) -> str:
        """Subscribe orderbooks channel message

        Args:
            symbol (str): Name of symbol

        Returns:
            str: dumped object
        """
        msg = self.SUBSCRIBE_ORDERBOOKS_MESSAGE
        msg["params"]["channel"] += symbol
        return json.dumps(msg)

    def subscribe_trades_msg(self, symbol: str) -> str:
        """Subsctibe trades channel message

        Args:
            symbol (str): Name of symbol

        Returns:
            str: dumped object
        """
        msg = self.SUBSCRIBE_TRADES_MESSAGE
        msg["params"]["channel"] += symbol
        return json.dumps(msg)
