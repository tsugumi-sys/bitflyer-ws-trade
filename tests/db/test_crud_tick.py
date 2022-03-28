import unittest
import sys

from tests.utils import response_schemas

sys.path.append("./gmo-websocket/")
from bitflyer_hft_bot.db import crud, models
from bitflyer_hft_bot.db.database import initialize_database

database_engine, SessionLocal = initialize_database(uri=None)


class TestCrudTick(unittest.TestCase):
    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)
        self.dummy_symbol = "Uncoin"

    def setUp(self) -> None:
        models.Base.metadata.create_all(database_engine)

    def tearDown(self) -> None:
        models.Base.metadata.drop_all(database_engine)

    def test_get_all_ticks(self):
        with SessionLocal() as db:
            all_ticks = crud.get_all_ticks(db=db, symbol=self.dummy_symbol)

        self.assertEqual(len(all_ticks), 0)

    def test_count_ticks(self):
        with SessionLocal() as db:
            count_ticks = crud._count_ticks(db=db)

        self.assertEqual(count_ticks, 0)

    def test_get_ticks(self):
        with SessionLocal() as db:
            insert_items = [
                response_schemas.TickResponseItem(
                    channel="trades",
                    price="200",
                    side="BUY",
                    size="0.2",
                    timestamp="2019-03-30T12:34:56.789Z",
                    symbol=self.dummy_symbol,
                ),
                response_schemas.TickResponseItem(
                    channel="trades",
                    price="100",
                    side="SELL",
                    size="0.1",
                    timestamp="2018-03-30T12:34:56.789Z",
                    symbol=self.dummy_symbol,
                ),
            ]

            for item in insert_items:
                crud.insert_tick_item(db=db, insert_item=item.dict())

            newer_tick_item = crud.get_ticks(db=db, is_newer=True, limit=1)
            older_tick_item = crud.get_ticks(db=db, is_newer=False, limit=1)

        self.assertEqual(newer_tick_item[0].price, 200.0)
        self.assertEqual(newer_tick_item[0].size, 0.2)
        self.assertEqual(older_tick_item[0].price, 100.0)
        self.assertEqual(older_tick_item[0].size, 0.1)

    def test_delete_tick_items(self):
        # Create tick
        with SessionLocal() as db:
            crud.insert_tick_item(
                db=db,
                insert_item=response_schemas.TickResponseItem(
                    channel="trades",
                    price="200",
                    side="BUY",
                    size="0.2",
                    timestamp="2019-03-30T12:34:56.789Z",
                    symbol=self.dummy_symbol,
                ).dict(),
            )

            tick = crud.get_ticks(db=db, is_newer=True, limit=1)

        with SessionLocal() as db:
            crud.delete_tick_items(db=db, delete_items=tick)

            rows = crud._count_ticks(db=db)

        self.assertEqual(rows, 0)

    def test_insert_tick_items(self):
        with SessionLocal() as db:
            insert_items = [
                response_schemas.TickResponseItem(
                    channel="trades",
                    price="200",
                    side="BUY",
                    size="0.2",
                    timestamp="2019-03-30T12:34:56.789Z",
                    symbol=self.dummy_symbol,
                ),
                response_schemas.TickResponseItem(
                    channel="trades",
                    price="100",
                    side="SELL",
                    size="0.1",
                    timestamp="2018-03-30T12:34:56.789Z",
                    symbol=self.dummy_symbol,
                ),
            ]

            for item in insert_items:
                crud.insert_tick_item(db=db, insert_item=item.dict())

            rows = crud._count_ticks(db=db)

        self.assertEqual(rows, 2)

        with SessionLocal() as db:
            insert_item = response_schemas.TickResponseItem(
                channel="trades",
                price="200",
                side="BUY",
                size="0.2",
                timestamp="2019-03-30T12:34:56.789Z",
                symbol=self.dummy_symbol,
            )

            crud.insert_tick_item(db=db, insert_item=insert_item.dict(), max_rows=1)

            rows = crud._count_ticks(db=db)

        self.assertEqual(rows, 1)
