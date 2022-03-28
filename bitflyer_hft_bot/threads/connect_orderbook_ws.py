import websockets
import asyncio
import json
import logging
import time
import traceback

from bitflyer_hft_bot.utils.queue_and_trade_manager import QueueAndTradeManager
from bitflyer_hft_bot.utils.bitflyer_websocket_subscriber import BitflyerWebsocketSubscriber
from bitflyer_hft_bot.utils.custom_exceptions import ConnectionFailedError


class ConnectOrderbookWs:
    RUNNING = True
    bitflyer_websocket_subscriber = BitflyerWebsocketSubscriber()

    async def run(self, ws_url: str, symbol: str, logger: logging.Logger, queue_and_trade_manager: QueueAndTradeManager):
        async with websockets.connect(ws_url, logger=logger, ping_timeout=1.0) as ws:
            ws.logger.info("Start orderbook")
            # Subscribe board topic
            subscribe_message = self.bitflyer_websocket_subscriber.subscribe_orderbooks_msg(symbol=symbol)
            await asyncio.wait_for(ws.send(subscribe_message), timeout=1.0)
            ws.logger.info("Orderbook subscribed!")

            while self.RUNNING:
                ws.logger.debug("Running Orderbook websockets")
                ws.logger.debug(f"Orderbook Queue count: {queue_and_trade_manager.get_orderbook_queue_size()}")
                try:
                    if queue_and_trade_manager.is_subprocesses_alive() is True:
                        # Get data
                        res = await ws.recv()
                        res = json.loads(res)

                        res_keys = list(res.keys())
                        if "error" in res_keys:
                            if "Invalid request parameter" in res["error"]:
                                raise ValueError(f"Invalid request parameter sybol={symbol}")
                            else:
                                ws.logger.error(f"Error response: {res}. Try to subscribe again")
                                # Try to connect again
                                time.sleep(0.5)
                                await asyncio.wait_for(ws.send(subscribe_message), timeout=1.0)
                        elif "params" in res_keys:
                            queue_and_trade_manager.add_orderbook_queue(res["params"]["message"])
                        else:
                            ws.logger.warning(f"Unknow response {res}")

                        await asyncio.sleep(0.1)
                    else:
                        msg = "subprocesses are dead."
                        ws.logger.error(msg)
                        raise Exception(msg)
                except websockets.exceptions.ConnectionClosed:
                    ws.logger.error("Public websocket connection has been closed.")
                    await asyncio.sleep(0.0)
                    raise ConnectionFailedError

                except asyncio.TimeoutError:
                    ws.logger.error("Time out for sending to pubic websocket api.")
                    await asyncio.sleep(0.0)
                    raise ConnectionFailedError

                except Exception as e:
                    ws.logger.error(traceback.format_exc())
                    ws.logger.error(e)
                    raise ConnectionFailedError
