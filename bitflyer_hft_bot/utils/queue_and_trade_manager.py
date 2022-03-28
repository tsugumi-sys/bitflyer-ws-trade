from typing import Dict, Tuple, Optional
import time
from datetime import datetime
import hmac
import hashlib
import multiprocessing as mp


class QueueAndTradeManager:
    def __init__(self, api_key: str, api_secret: str) -> None:
        self.enable_trade = False
        if api_key is None or api_secret is None:
            raise ValueError("api_key or api_secret is None. Check your .env file.")

        self.api_key = api_key
        self.api_secret = api_secret
        self.http_request_private_baseurl = "https://api.coin.z.com/private"
        self.orderbook_queue = mp.Manager().Queue()
        self.ticks_queue = mp.Manager().Queue()

        self.subprocesses_info = mp.Manager().dict({"is_subprocesses_alive": True})

    def __del__(self):
        import time

        # Sometime, Broken pipe error raises becase main process finishes faster than Queue.close().
        time.sleep(0.01)

    def is_subprocesses_alive(self):
        return self.subprocesses_info["is_subprocesses_alive"]

    def update_subprocesses_alive_status(self, status: bool) -> None:
        self.subprocesses_info["is_subprocesses_alive"] = status

    def http_headers(self, method: str, endpint: str):
        timestamp = "{0}000".format(int(time.mktime(datetime.now().timetuple())))

        text = timestamp + method + endpint
        sign = hmac.new(bytes(self.api_secret.encode("ascii")), bytes(text.encode("ascii")), hashlib.sha256).hexdigest()

        headers = {"API-KEY": self.api_key, "API-TIMESTAMP": timestamp, "API-SIGN": sign}
        return headers

    def test_http_private_request_args(self) -> Tuple[str, Dict]:
        """Get args to HTTP private request for GMO exchange

        Returns:
            Tuple[str, Dict]: uri and headers
        """
        endpoint = "/v1/account/margin"
        return self.http_request_private_baseurl + endpoint, self.http_headers(method="GET", endpint=endpoint)

    def order_http_request_args(
        self,
        symbol: str,
        side: str,
        time_in_force: Optional[str],
        execution_type: str,
        price: Optional[str],
        loss_cut_price: Optional[str],
        size: float,
        cancel_before: Optional[bool],
    ):
        """Order http request POST.

        Args:
            symbol (str): _description_
            side (str): _description_
            time_in_force (Optional[str]): _description_
            execution_type (str): _description_
            price (Optional[str]): _description_
            loss_cut_price (Optional[str]): _description_
            size (float): _description_
            cancel_before (Optional[bool]): _description_
        """

    def _enable_trade(self):
        self.enable_trade = True

    def _disable_trade(self):
        self.enable_trade = False

    def add_orderbook_queue(self, item: Dict):
        # self.orderbook_queue.put_nowait(item)
        self.orderbook_queue.put(item)

    def get_orderbook_queue_size(self):
        return self.orderbook_queue.qsize()

    def get_orderbook_queue_item(self):
        # return self.orderbook_queue.get_nowait()
        return self.orderbook_queue.get(block=True, timeout=0.05)

    def add_ticks_queue(self, item: Dict):
        # self.ticks_queue.put_nowait(item)
        self.ticks_queue.put(item)

    def get_ticks_queue_item(self):
        # return self.ticks_queue.get_nowait()
        return self.ticks_queue.get(block=True, timeout=0.05)

    def get_ticks_queue_size(self):
        return self.ticks_queue.qsize()
