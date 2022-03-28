import unittest
import sys

from tests.utils import response_schemas

sys.path.append("./gmo_websocket/")
from bitflyer_hft_bot.db import crud, models
from bitflyer_hft_bot.db.database import initialize_database

database_engine, SessionLocal = initialize_database(uri=None)


class TestCrudBoard(unittest.TestCase):
    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)
        self.dummy_symbol = "Uncoin"

    def setUp(self) -> None:
        models.Base.metadata.create_all(database_engine)

    def tearDown(self) -> None:
        models.Base.metadata.drop_all(database_engine)

    def test_count_board_rows(self):
        # Create dummy item
        with SessionLocal() as db:
            crud.insert_board_items(
                db=db,
                insert_items=response_schemas.BoardResponseItem(
                    asks=[response_schemas.BidsAsks(price="300", size="10")],
                    bids=[response_schemas.BidsAsks(price="100", size="3")],
                    symbol=self.dummy_symbol,
                    timestamp="2018-03-30T12:34:56.789Z",
                ).dict(),
            )

        with SessionLocal() as db:
            rows = crud._count_board_rows(db)

        self.assertEqual(rows, 2)

    def test_count_boards(self):
        # Create dummy item
        with SessionLocal() as db:
            crud.insert_board_items(
                db=db,
                insert_items=response_schemas.BoardResponseItem(
                    asks=[response_schemas.BidsAsks(price="300", size="10")],
                    bids=[response_schemas.BidsAsks(price="100", size="3")],
                    symbol=self.dummy_symbol,
                    timestamp="2018-03-30T12:34:56.789Z",
                ).dict(),
            )

        with SessionLocal() as db:
            count_boards = crud._count_boards(db=db)

        self.assertEqual(count_boards, 1)

    def test_get_current_board(self):
        # Create dummy item
        with SessionLocal() as db:
            crud.insert_board_items(
                db=db,
                insert_items=response_schemas.BoardResponseItem(
                    asks=[response_schemas.BidsAsks(price="300", size="10")],
                    bids=[response_schemas.BidsAsks(price="100", size="3")],
                    symbol=self.dummy_symbol,
                    timestamp="2018-03-30T12:34:56.789Z",
                ).dict(),
            )
            crud.insert_board_items(
                db=db,
                insert_items=response_schemas.BoardResponseItem(
                    asks=[response_schemas.BidsAsks(price="350", size="100")],
                    bids=[response_schemas.BidsAsks(price="150", size="30")],
                    symbol=self.dummy_symbol,
                    timestamp="2019-03-30T12:34:56.789Z",
                ).dict(),
            )

        with self.subTest("When side paraeter is wrong, raise value error"):
            with self.assertRaises(ValueError):
                with SessionLocal() as db:
                    _ = crud.get_current_board(db=db, symbol=self.dummy_symbol, side="wrong-side")

        with self.subTest("When side is correct"):
            with SessionLocal() as db:
                buy_borad = crud.get_current_board(db=db, symbol=self.dummy_symbol, side="BUY")
                sell_board = crud.get_current_board(db=db, symbol=self.dummy_symbol, side="SELL")

                self.assertEqual(len(buy_borad), 1)
                self.assertEqual(buy_borad[0].price, 150.0)
                self.assertEqual(len(sell_board), 1)
                self.assertEqual(sell_board[0].price, 350.0)

        with self.subTest("When try to get both side"):
            with SessionLocal() as db:
                buy_borad, sell_board = crud.get_current_board(db=db, symbol=self.dummy_symbol)
                self.assertEqual(len(buy_borad), 1)
                self.assertEqual(buy_borad[0].price, 150.0)
                self.assertEqual(len(sell_board), 1)
                self.assertEqual(sell_board[0].price, 350.0)

    def test_get_oldest_board(self):
        # Create dummy item
        with SessionLocal() as db:
            crud.insert_board_items(
                db=db,
                insert_items=response_schemas.BoardResponseItem(
                    asks=[response_schemas.BidsAsks(price="300", size="10")],
                    bids=[response_schemas.BidsAsks(price="100", size="3")],
                    symbol=self.dummy_symbol,
                    timestamp="2018-03-30T12:34:56.789Z",
                ).dict(),
            )
            crud.insert_board_items(
                db=db,
                insert_items=response_schemas.BoardResponseItem(
                    asks=[response_schemas.BidsAsks(price="350", size="100")],
                    bids=[response_schemas.BidsAsks(price="150", size="30")],
                    symbol=self.dummy_symbol,
                    timestamp="2019-03-30T12:34:56.789Z",
                ).dict(),
            )

        with self.subTest("When side parameter is wrong"):
            with self.assertRaises(ValueError):
                with SessionLocal() as db:
                    _ = crud.get_oldest_board(db=db, symbol=self.dummy_symbol, side="wrong-side")

        with self.subTest("When side is correct."):
            with SessionLocal() as db:
                buy_board = crud.get_oldest_board(db=db, symbol=self.dummy_symbol, side="BUY")
                sell_board = crud.get_oldest_board(db=db, symbol=self.dummy_symbol, side="SELL")

                self.assertEqual(len(buy_board), 1)
                self.assertEqual(buy_board[0].price, 100.0)
                self.assertEqual(len(sell_board), 1)
                self.assertEqual(sell_board[0].price, 300.0)

        with self.subTest("When side is None"):
            with SessionLocal() as db:
                buy_board, sell_board = crud.get_oldest_board(db=db, symbol=self.dummy_symbol)

                self.assertEqual(len(buy_board), 1)
                self.assertEqual(buy_board[0].price, 100.0)
                self.assertEqual(len(sell_board), 1)
                self.assertEqual(sell_board[0].price, 300.0)

    def test_insert_board_items(self):
        with SessionLocal() as db:
            crud.insert_board_items(
                db=db,
                insert_items=response_schemas.BoardResponseItem(
                    asks=[response_schemas.BidsAsks(price="300", size="10")],
                    bids=[response_schemas.BidsAsks(price="100", size="3")],
                    symbol=self.dummy_symbol,
                    timestamp="2018-03-30T12:34:56.789Z",
                ).dict(),
            )

            rows = crud._count_board_rows(db=db)

        self.assertEqual(rows, 2)

    def test_update_board_items(self):
        # Create dummy item
        with SessionLocal() as db:
            crud.insert_board_items(
                db=db,
                insert_items=response_schemas.BoardResponseItem(
                    asks=[response_schemas.BidsAsks(price="300", size="10")],
                    bids=[response_schemas.BidsAsks(price="100", size="3")],
                    symbol=self.dummy_symbol,
                    timestamp="2018-03-30T12:34:56.789Z",
                ).dict(),
            )

        with SessionLocal() as db:
            buy_item = crud.get_current_board(db=db, symbol=self.dummy_symbol, side="BUY")
            buy_item = buy_item[0]
            buy_item_obj = {
                "id": buy_item.id,
                "timestamp": buy_item.timestamp,
                "price": 5000.0,
                "size": 500.0,
                "side": buy_item.side,
                "symbol": buy_item.symbol,
            }
            crud.update_board_items(db=db, update_items=[buy_item_obj])

            buy_item = crud.get_current_board(db=db, symbol=self.dummy_symbol, side="BUY")

        self.assertEqual(buy_item[0].price, 5000.0)
        self.assertEqual(buy_item[0].size, 500.0)

    def test_delete_board_items(self):
        # Create dummy item
        with SessionLocal() as db:
            crud.insert_board_items(
                db=db,
                insert_items=response_schemas.BoardResponseItem(
                    asks=[response_schemas.BidsAsks(price="300", size="10")],
                    bids=[response_schemas.BidsAsks(price="100", size="3")],
                    symbol=self.dummy_symbol,
                    timestamp="2018-03-30T12:34:56.789Z",
                ).dict(),
            )

            rows = crud._count_board_rows(db=db)

        self.assertEqual(rows, 2)

        with SessionLocal() as db:
            delete_item = crud.get_current_board(db=db, symbol=self.dummy_symbol, side="BUY")
            delete_item = delete_item[0]
            delete_item_obj = {
                "id": delete_item.id,
                "timestamp": delete_item.timestamp,
                "price": delete_item.price,
                "size": delete_item.size,
                "side": delete_item.side,
                "symbol": delete_item.symbol,
            }
            crud.delete_board_items(db=db, delete_items=[delete_item_obj])

            rows = crud._count_board_rows(db=db)

        self.assertEqual(rows, 1)

    def test_delete_board(self):
        # Create dummy item
        with SessionLocal() as db:
            crud.insert_board_items(
                db=db,
                insert_items=response_schemas.BoardResponseItem(
                    asks=[response_schemas.BidsAsks(price="300", size="10")],
                    bids=[response_schemas.BidsAsks(price="100", size="3")],
                    symbol=self.dummy_symbol,
                    timestamp="2018-03-30T12:34:56.789Z",
                ).dict(),
            )

            rows = crud._count_board_rows(db)

        self.assertEqual(rows, 2)
        with SessionLocal() as db:
            delete_item = crud.get_current_board(db=db, symbol=self.dummy_symbol, side="BUY")
            delete_item = delete_item[0]
            crud.delete_board(db=db, timestamp=delete_item.timestamp)

            rows = crud._count_board_rows(db)

        self.assertEqual(rows, 0)
