from datetime import datetime, timedelta, timezone
import unittest
import sys
from dateutil import parser

from tests.utils import response_schemas

sys.path.append("./gmo-websocket/")
from bitflyer_hft_bot.db import crud, models, schemas
from bitflyer_hft_bot.db.database import initialize_database

database_engine, SessionLocal = initialize_database(uri=None)


# [TODO] timestamp assertion occasionally fails because of the slip of unix time.
class TestCrudOHLCV(unittest.TestCase):
    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)
        self.dummy_symbol = "Uncoin"
        self.dummy_timestamps = []

    def setUp(self) -> None:
        models.Base.metadata.create_all(database_engine)
        # Create dummy tick data.
        now = datetime.now(timezone.utc)
        dummy_timestamp1 = "{}Z".format(now.isoformat()[:-9])
        dummy_timestamp2 = "{}Z".format((now + timedelta(seconds=1)).isoformat()[:-9])
        dummy_timestamp3 = "{}Z".format((now + timedelta(seconds=2)).isoformat()[:-9])
        dummy_timestamp4 = "{}Z".format((now + timedelta(seconds=3)).isoformat()[:-9])

        self.dummy_timestamps += [dummy_timestamp1, dummy_timestamp2, dummy_timestamp3, dummy_timestamp4]
        dummy_tick_items = [
            response_schemas.TickResponseItem(
                channel="trades",
                price="200",
                side="BUY",
                size="0.1",
                timestamp=dummy_timestamp1,
                symbol=self.dummy_symbol,
            ),
            response_schemas.TickResponseItem(
                channel="trades",
                price="250",
                side="SELL",
                size="0.1",
                timestamp=dummy_timestamp2,
                symbol=self.dummy_symbol,
            ),
            response_schemas.TickResponseItem(
                channel="trades",
                price="50",
                side="BUY",
                size="0.1",
                timestamp=dummy_timestamp3,
                symbol=self.dummy_symbol,
            ),
            response_schemas.TickResponseItem(
                channel="trades",
                price="100",
                side="SELL",
                size="0.1",
                timestamp=dummy_timestamp4,
                symbol=self.dummy_symbol,
            ),
        ]

        with SessionLocal() as db:
            for item in dummy_tick_items:
                crud.insert_tick_item(db=db, insert_item=item.dict())

    def tearDown(self) -> None:
        self.dummy_timestamps = []
        models.Base.metadata.drop_all(database_engine)

    def test_count_ohlcv(self):
        with SessionLocal() as db:
            crud.create_ohlcv_from_ticks(db=db, symbol=self.dummy_symbol, time_span=5)

            rows = crud._count_ohlcv(db=db)

        self.assertTrue(rows == 1 or rows == 2)

    def test_check_if_ohlcv_stored(self):
        time_span = 5
        with SessionLocal() as db:
            crud.create_ohlcv_from_ticks(db=db, symbol=self.dummy_symbol, time_span=time_span)

            timestamp = parser.parse(self.dummy_timestamps[0]).timestamp() * 1000
            timestamp = (timestamp // (time_span * 1000)) * time_span
            is_stored = crud._check_if_ohclv_stored(db=db, timestamp=timestamp)

        self.assertTrue(is_stored)

    def test_get_ohlcv_with_symbol(self):
        with SessionLocal() as db:
            time_span = 2
            crud.create_ohlcv_from_ticks(db=db, symbol=self.dummy_symbol, time_span=time_span)
            with self.subTest("Raise ValueError if limit is less than 1"):
                with self.assertRaises(ValueError):
                    _ = crud.get_ohlcv_with_symbol(db=db, symbol=self.dummy_symbol, limit=0)

            with self.subTest("If ascending of time is True"):
                res = crud.get_ohlcv_with_symbol(db=db, symbol=self.dummy_symbol, ascending=True)

                timestamp = parser.parse(self.dummy_timestamps[1]).timestamp() * 1000
                timestamp = (timestamp // (time_span * 1000)) * time_span

                self.assertTrue(res[0].timestamp == timestamp or res[0].timestamp == timestamp - 2)

            with self.subTest("If ascending of time is False"):
                res = crud.get_ohlcv_with_symbol(db=db, symbol=self.dummy_symbol, ascending=False)

                timestamp = parser.parse(self.dummy_timestamps[2]).timestamp() * 1000
                timestamp = (timestamp // (time_span * 1000)) * time_span

                self.assertTrue(res[0].timestamp == timestamp or res[0].timestamp == timestamp + 2)

            with self.subTest("If limit is one"):
                res = crud.get_ohlcv_with_symbol(db=db, symbol=self.dummy_symbol, limit=1)

                self.assertEqual(len(res), 1)

    def test_get_ohlcv(self):
        with SessionLocal() as db:
            time_span = 2
            crud.create_ohlcv_from_ticks(db=db, symbol=self.dummy_symbol, time_span=time_span)

            with self.subTest("Raise Value error when limit is less than 0"):
                with self.assertRaises(ValueError):
                    _ = crud.get_ohlcv(db=db, limit=0)

            with self.subTest("If ascenging of time is True"):
                res = crud.get_ohlcv(db=db, ascending=True)

                timestamp = parser.parse(self.dummy_timestamps[0]).timestamp() * 1000
                timestamp = (timestamp // (time_span * 1000)) * time_span

                self.assertEqual(res[0].timestamp, timestamp)

            with self.subTest("If ascending of time is False"):
                res = crud.get_ohlcv(db=db, ascending=False)

                timestamp = parser.parse(self.dummy_timestamps[2]).timestamp() * 1000
                timestamp = (timestamp // (time_span * 1000)) * time_span

                self.assertTrue(res[0].timestamp == timestamp or res[0].timestamp == timestamp + 2)

            with self.subTest("If limit is one"):
                res = crud.get_ohlcv(db=db, limit=1)

                self.assertEqual(len(res), 1)

    def test_insert_ohlcv_items(self):
        time_span = 2
        now = datetime.now(timezone.utc)
        dummy_timestamp = "{}Z".format((now + timedelta(minutes=10)).isoformat()[:-9])
        timestamp = parser.parse(dummy_timestamp).timestamp() * 1000
        timestamp = (timestamp // (time_span * 1000)) * time_span
        insert_items = [
            schemas.OHLCV(timestamp=timestamp, open=50.0, high=60.0, low=20.0, close=30.0, volume=15.0, symbol=self.dummy_symbol),
        ]
        with SessionLocal() as db:
            crud.create_ohlcv_from_ticks(db=db, symbol=self.dummy_symbol, time_span=time_span)
            crud.insert_ohlcv_items(db=db, insert_items=insert_items, max_rows=1)

            rows = crud._count_ohlcv(db=db)
            ohlc_row = crud.get_ohlcv_with_symbol(db=db, symbol=self.dummy_symbol)[0]

        self.assertEqual(rows, 1)
        self.assertEqual(ohlc_row.timestamp, timestamp)

    def test_update_ohlcv_items(self):
        time_span = 1
        timestamp = parser.parse(self.dummy_timestamps[0]).timestamp() * 1000
        timestamp = timestamp // (time_span * 1000)

        update_items = [schemas.OHLCV(timestamp=timestamp, open=0.0, high=1.0, low=0.0, close=0.5, volume=2.0, symbol=self.dummy_symbol)]
        with SessionLocal() as db:
            crud.create_ohlcv_from_ticks(db=db, symbol=self.dummy_symbol, time_span=time_span)

            crud.update_ohlcv_items(db=db, update_items=update_items)

            res = crud.get_ohlcv_with_symbol(db=db, symbol=self.dummy_symbol, limit=1, ascending=True)

        self.assertEqual(res[0].open, 0.0)
        self.assertEqual(res[0].close, 0.5)

    def test_delete_ohlcv_items(self):
        time_span = 1
        timestamp = parser.parse(self.dummy_timestamps[0]).timestamp() * 1000
        timestamp = timestamp // (time_span * 1000)
        delete_items = [{"timestamp": timestamp}]

        with SessionLocal() as db:
            crud.create_ohlcv_from_ticks(db=db, symbol=self.dummy_symbol, time_span=time_span)
            before_rows = crud._count_ohlcv(db=db)
            crud.delete_ohlcv_items(db=db, delete_items=delete_items)
            after_rows = crud._count_ohlcv(db=db)

            res = crud.get_ohlcv(db=db, limit=1, ascending=True)

        self.assertEqual(before_rows, 4)
        self.assertEqual(after_rows, 3)

        valid_timestamp = parser.parse(self.dummy_timestamps[1]).timestamp() * 1000
        valid_timestamp = (valid_timestamp // (time_span * 1000)) * time_span
        self.assertEqual(res[0].timestamp, valid_timestamp)

    def test_create_ohlcv_from_ticks(self):
        time_span = 100
        with SessionLocal() as db:
            crud.create_ohlcv_from_ticks(db=db, symbol=self.dummy_symbol, time_span=time_span, max_rows=1)
            rows = crud._count_ohlcv(db)
            res = crud.get_ohlcv_with_symbol(db=db, symbol=self.dummy_symbol, limit=1)

        self.assertEqual(rows, 1)
        valid_timestamp = parser.parse(self.dummy_timestamps[0]).timestamp() * 1000
        valid_timestamp = (valid_timestamp // (time_span * 1000)) * time_span
        self.assertEqual(res[0].timestamp, valid_timestamp)

        self.assertEqual(res[0].open, 200)
        self.assertEqual(res[0].high, 250)
        self.assertEqual(res[0].low, 50)
        self.assertEqual(res[0].close, 100)
