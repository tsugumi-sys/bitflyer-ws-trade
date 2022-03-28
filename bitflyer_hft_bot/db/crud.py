from typing import Dict, List, Optional, Tuple, Union
from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.engine.row import Row
import uuid
import time
from dateutil import parser
import pandas as pd

from bitflyer_hft_bot.db import schemas, models


# Board methods
def _count_board_rows(db: Session) -> int:
    """Count board table rows.

    Args:
        db (Session): Session of sqlalchemy

    Returns:
        int: The number of table rows.
    """
    return db.query(models.Board).count()


def _count_boards(db: Session) -> int:
    """Count board items group by timestamp.

    Args:
        db (Session): Session of sqlalchemy

    Returns:
        int: Number of board items.
    """
    return db.query(models.Board).group_by(models.Board.timestamp).count()


def get_whole_board(db: Session) -> List[schemas.Board]:
    """[Get all board]

    Args:
        db (Session): [Session of sqlalchemy.]
    """
    return db.query(models.Board).all()


def get_current_board(db: Session, symbol: str, side: Optional[str] = None) -> Union[List[Row], Tuple[List[Row]]]:
    """[Get current board. return with ascending order of price.]

    Args:
        db (Session): [Session of sqlalchemy.]
        symbol (str): [target symbol.]
        side (Optional[str]): [target side (BUY or SELL). If None return both.]

    Raises:
        ValueError: [raise error if `side` is invalid]

    Returns:
        (Union(List[schemas.Board], Tuple)): Union(List[schemas.Board] if side = "BUY" or "SELL".
            Tuple(buy_board_items, sell_board_items) if side is None
    """
    stat = text(
        """
            select * from board
            where timestamp = (select max(timestamp) from board where board.symbol=:symbol and board.side=:side) and
            board.symbol=:symbol and board.side=:side
            order by board.price
        """
    )

    if isinstance(side, str):
        side = side.upper()
        if side not in schemas.sides:
            raise ValueError(f"Invalid side {side}. side should be in {schemas.sides}")

        return db.execute(stat, {"symbol": symbol, "side": side}).all()
    else:
        return db.execute(stat, {"symbol": symbol, "side": "BUY"}).all(), db.execute(stat, {"symbol": symbol, "side": "SELL"}).all()


def get_oldest_board(db: Session, symbol: str, side: Optional[str] = None) -> Union[List[schemas.Board], Tuple]:
    """[Get oldest board. return with ascending order of price.]

    Args:
        db (Session): [Session of sqlalchemy.]
        symbol (str): [target symbol.]
        side (Optional[str]): [target side (BUY or SELL). If None return both.]

    Raises:
        ValueError: [raise error if `side` is invalid]

    Returns:
        (Union(List[schemas.Board], Tuple)): Union(List[schemas.Board] if side = "BUY" or "SELL".
            Tuple(buy_board_items, sell_board_items) if side is None
    """
    stat = """
        select * from board
        where timestamp = (select min(timestamp) from board where board.symbol=:symbol and board.side=:side) and
        board.symbol=:symbol and board.side=:side
        order by board.price
    """

    if isinstance(side, str):
        side = side.upper()
        if side not in schemas.sides:
            raise ValueError(f"Invalid side {side}. side should be in {schemas.sides}")

        return db.execute(stat, {"symbol": symbol, "side": side}).all()
    else:
        return db.execute(stat, {"symbol": symbol, "side": "BUY"}).all(), db.execute(stat, {"symbol": symbol, "side": "SELL"}).all()


def insert_board_items(db: Session, symbol: str, insert_items: Dict, max_board_counts: int = 1000) -> None:
    """[Insert Board items.]

    Args:
        db (Session): [Session of sqlalchemy]
        insert_items (Dict): [board items]
            e.g. {
                    "channel":"orderbooks",
                    "asks": [
                        {"price": "455659","size": "0.1"},
                        {"price": "455658","size": "0.2"}
                    ],
                    "bids": [
                        {"price": "455665","size": "0.1"},
                        {"price": "455655","size": "0.3"}
                    ],
                }
        max_board_counts (int): Max board counts (group by timestamp)
    """
    board_items = []
    # ask items
    insert_asks_items = insert_items["asks"]
    insert_bids_item = insert_items["bids"]
    timestamp = time.time() * 1000
    timestamp = int(timestamp)

    count_boards = _count_boards(db)
    if count_boards > max_board_counts:
        oldest_board = get_oldest_board(db=db, symbol=symbol, side="BUY")
        delete_board(db=db, timestamp=oldest_board[0].timestamp)

    if len(insert_asks_items) > 0:
        for item in insert_asks_items:
            id = uuid.uuid4().hex
            side = "SELL"
            board_items.append(
                models.Board(
                    id=id,
                    timestamp=timestamp,
                    price=float(item["price"]),
                    size=float(item["size"]),
                    side=side,
                    symbol=symbol,
                )
            )

    if len(insert_bids_item) > 0:
        for item in insert_bids_item:
            id = uuid.uuid4().hex
            side = "BUY"
            board_items.append(
                models.Board(
                    id=id,
                    timestamp=timestamp,
                    price=float(item["price"]),
                    size=float(item["size"]),
                    side=side,
                    symbol=symbol,
                )
            )

    db.add_all(board_items)
    db.commit()


def update_board_items(db: Session, update_items: List[Dict]) -> None:
    """[Update Board items from delta format responce of bybit websocket.]

    Args:
        db (Session): [Session of sqlalchemy]
        update_items (List[schemas.Board]): [items to update.]
    """
    for item in update_items:
        db.query(models.Board).filter(models.Board.id == item["id"]).update(item)

    db.commit()


def delete_board_items(db: Session, delete_items: List[Dict]) -> None:
    """[Delete Board items from delta format respone of bybit websocket.]

    Args:
        db (Session): [Session of sqlalchemy]
        delete_items (List[schemas.Board]): [items to be deleted.]
    """
    for item in delete_items:
        db.query(models.Board).filter(models.Board.id == item["id"]).delete()

    db.commit()


def delete_board(db: Session, timestamp: int) -> None:
    """Delete board at a certain timestamp

    Args:
        db (Session): Session of sqlalchemy
        timestamp (int): timestamp
    """
    db.query(models.Board).filter(models.Board.timestamp == timestamp).delete()
    db.commit()


# Tick methods
def get_all_ticks(db: Session, symbol: str) -> List[schemas.Tick]:
    """get all tick data

    Args:
        db (Session): Session of sqlalchemy

    Returns:
        List[schemas.Tick]: list of ticks.
    """
    return db.query(models.Tick).filter(models.Tick.symbol == symbol).all()


def _count_ticks(db: Session) -> int:
    """Get count of rows in `tick` table

    Args:
        db (Session): Session of sqlalchemy

    Returns:
        int: counts of rows
    """
    return db.query(models.Tick).count()


def get_ticks(db: Session, is_newer: bool, limit: int = 1) -> List[schemas.Tick]:
    """Get older or newer tick data.

    Args:
        db (Session): Session of sqlalchemy
        is_newer (bool): If True, get newer data.
        limit (int, optional): the number of ticks to get. Defaults to 1 (oldest ticks).

    Returns:
        List[schemas.Tick]: list of older ticks
    """
    if is_newer:
        return db.query(models.Tick).order_by(models.Tick.timestamp.desc()).limit(limit).all()
    else:
        return db.query(models.Tick).order_by(models.Tick.timestamp).limit(limit).all()


def delete_tick_items(db: Session, delete_items: List[schemas.Tick]) -> None:
    """Delete tick items

    Args:
        db (Session): Session of sqlalchemy
        delete_items (List[Dict]): the list of delete items
    """
    for item in delete_items:
        db.query(models.Tick).filter(models.Tick.id == item.id).delete()

    db.commit()


def insert_tick_item(db: Session, symbol: str, insert_items: List[Dict], max_rows: int = 1000) -> None:
    """Insert tick item

    Args:
        db (Session): Session of sqlalchemy
        insert_item (Dict): Response of GMO websocket (subscribe tick).
            e.g. {
                    "channel":"trades",
                    "price": "750760",
                    "side": "BUY",
                    "size": "0.1",
                    "timestamp": "2018-03-30T12:34:56.789Z",
                    "symbol": "BTC"
                }
        max_rows (int, optional): Number of max rows. Defaults to 1000.
    """
    # Delete older items
    count_ticks = _count_ticks(db)
    if count_ticks > max_rows:
        delete_items_count = count_ticks - max_rows + 1
        delete_items = get_ticks(db=db, is_newer=False, limit=delete_items_count)
        delete_tick_items(db=db, delete_items=delete_items)

    # insert new tick data
    if len(insert_items) > 0:
        tick_items = []
        for item in insert_items:
            id = uuid.uuid4().hex
            timestamp = parser.parse(item["exec_date"]).timestamp() * 1000
            timestamp = int(timestamp)
            tick_items.append(
                models.Tick(
                    id=id,
                    timestamp=timestamp,
                    price=float(item["price"]),
                    size=float(item["size"]),
                    side=item["side"],
                    symbol=symbol,
                )
            )
        db.add_all(tick_items)
        db.commit()


# OHLCV methods
def _count_ohlcv(db: Session) -> int:
    """Count ohlcv rows

    Args:
        db (Session): Session of sqlalchemy

    Returns:
        (int): counts of ohlcv table rows
    """
    return db.query(models.OHLCV).count()


def _check_if_ohclv_stored(db: Session, timestamp: int) -> bool:
    """Check if the data has stored in ohlcv table

    Args:
        db (Session): Session of sqlalchemy
        timestamp (int): timestamp (id)

    Returns:
        bool: Return true if exists.
    """
    count_item = db.query(models.OHLCV).filter(models.OHLCV.timestamp == timestamp).count()
    return True if count_item == 1 else False


def get_ohlcv_with_symbol(
    db: Session, symbol: Optional[str] = None, limit: Optional[int] = None, ascending: bool = True, as_df: bool = False
) -> List[schemas.OHLCV]:
    """get all ohlcv of a symbol

    Args:
        db (Session): Session of sqlalchemy
        symbol (str): Name of symbol
        limit (Optional[int], optional): limit. Defaults to None.
        ascending (bool, optional): Ascending order of timestamp. Defaults to True.

    Returns:
        List[schemas.OHLCV]: list of ohlcv
    """
    if limit is not None and limit < 1:
        raise ValueError("`limit` should be more than 1.")

    if ascending:
        if limit is None:
            if as_df is True:
                stat = text("select * from ohlcv where ohlcv.symbol = :symbol order by timestamp")
                return pd.read_sql(stat, db.bind, params={"symbol": symbol})
            else:
                return db.query(models.OHLCV).filter(models.OHLCV.symbol == symbol).order_by(models.OHLCV.timestamp).all()
        else:
            if as_df is True:
                stat = text("select * from ohlcv where ohlcv.symbol = :symbol order by timestamp limit :limit")
                return pd.read_sql(stat, db.bind, params={"symbol": symbol, "limit": limit})
            else:
                return db.query(models.OHLCV).filter(models.OHLCV.symbol == symbol).order_by(models.OHLCV.timestamp).limit(limit).all()
    else:
        if limit is None:
            if as_df is True:
                stat = text("select * from ohlcv where ohlcv.symbol = :symbol order by timestamp desc")
                return pd.read_sql(stat, db.bind, params={"symbol": symbol})
            else:
                return db.query(models.OHLCV).filter(models.OHLCV.symbol == symbol).order_by(models.OHLCV.timestamp.desc()).all()
        else:
            if as_df is True:
                stat = text("select * from ohlcv where ohlcv.symbol = :symbol order by timestamp desc limit :limit")
                return pd.read_sql(stat, db.bind, params={"symbol": symbol, "limit": limit})
            else:
                return db.query(models.OHLCV).filter(models.OHLCV.symbol == symbol).order_by(models.OHLCV.timestamp.desc()).limit(limit).all()


def get_ohlcv(db: Session, limit: Optional[int] = None, ascending: bool = True, as_df: bool = False) -> List[schemas.OHLCV]:
    """get all ohlcv

    Args:
        db (Session): Session of sqlalchemy
        limit (Optional[int], optional): limit. Defaults to None.
        ascending (bool, optional): ascending order. Defaults to True.
        as_df (bool, optional): Get data as dataframe (timestamp is index column).

    Returns:
        List[schemas.OHLCV]: Session of sqlalchemy
    """
    if limit is not None and limit < 1:
        raise ValueError("`limit` should be more than 1.")

    if ascending:
        if limit is None:
            if as_df is True:
                return pd.read_sql("select * from ohlcv order by timestamp", db.bind)
            else:
                return db.query(models.OHLCV).order_by(models.OHLCV.timestamp).all()
        else:
            if as_df is True:
                stat = text("select * from ohlcv order by timestamp limit :limit")
                return pd.read_sql(stat, db.bind, params={"limit": limit})
            else:
                return db.query(models.OHLCV).order_by(models.OHLCV.timestamp).limit(limit).all()
    else:
        if limit is None:
            if as_df is True:
                return pd.read_sql("select * from ohlcv order by timestamp desc", db.bind)
            else:
                return db.query(models.OHLCV).order_by(models.OHLCV.timestamp.desc()).all()
        else:
            if as_df is True:
                stat = text("select * from ohlcv order by timestamp desc limit :limit")
                return pd.read_sql(stat, db.bind, params={"limit": limit})
            else:
                return db.query(models.OHLCV).order_by(models.OHLCV.timestamp.desc()).limit(limit).all()


def insert_ohlcv_items(db: Session, insert_items: List[schemas.OHLCVCreate], max_rows: int = 100) -> None:
    """Insert ohlcv items

    Args:
        db (Session): Session of sqlalchemy
        insert_items (List[Union[Dict, schemas.OHLCV]]): List of ohlcv items.
    """
    # Delete older rows
    count_ohlcv = _count_ohlcv(db=db)
    if count_ohlcv + len(insert_items) - 1 > max_rows:
        query_limit = count_ohlcv + len(insert_items) - max_rows + 1
        delete_items = get_ohlcv(db=db, limit=query_limit, ascending=True)
        delete_ohlcv_items(db=db, delete_items=delete_items)

    ohlcv_items = []
    for item in insert_items:
        item = models.OHLCV(**item.dict())
        ohlcv_items.append(item)

    db.add_all(ohlcv_items)
    db.commit()


def update_ohlcv_items(db: Session, update_items: List[schemas.OHLCV]) -> None:
    """Update ohlcv items

    Args:
        db (Session): Session of sqlalchemy
        update_items (List[schemas.OHLCV]): update ohlcv items.
    """
    for item in update_items:
        db.query(models.OHLCV).filter(models.OHLCV.timestamp == item.timestamp).update(item.dict())

    db.commit()


def delete_ohlcv_items(db: Session, delete_items: List[Union[Dict, schemas.OHLCV]]) -> None:
    for item in delete_items:
        if isinstance(item, Dict):
            db.query(models.OHLCV).filter(models.OHLCV.timestamp == item["timestamp"]).delete()
        else:
            db.query(models.OHLCV).filter(models.OHLCV.timestamp == item.timestamp).delete()

    db.commit()


def create_ohlcv_from_ticks(db: Session, symbol: str, time_span: int, max_rows: int = 100) -> None:
    """Create OHLCV (5 seconds) from tick data.

    Args:
        db (Session): Session of sqlalchemy
        symbol (str): Name of pair
        timespan (int): Timespan to create timebar.
        max_rows (int): Number of max rows of ohlcv table. Default is 100.
    """
    stat = text(
        """
        select
            open,
            max(price) as high,
            min(price) as low,
            close,
            sum(size) as volume,
            open_time
        from (
            select
                *,
                first_value(price) over (partition by open_time order by timestamp, price desc) as open,
                first_value(price) over (partition by open_time order by timestamp desc, price desc) as close
            from (
                select
                    *,
                    cast(timestamp/(1000*:time_span) as int) as open_time
                from tick
                where tick.symbol= :symbol and tick.timestamp > :min_unix_timestamp
            )
        )
        group by open_time
        order by open_time desc
        """
    )
    # [TODO]: filter ticks by timestamp
    min_unix_timestamp = (time.time() // time_span - 1) * time_span * 1000

    ohlcv_items = db.execute(stat, {"symbol": symbol, "time_span": time_span, "min_unix_timestamp": min_unix_timestamp}).all()
    ohlcv_insert_items = []
    ohlcv_update_items = []
    timestamps = []

    for item in ohlcv_items:
        ohlcv_model = schemas.OHLCVCreate(open=item[0], high=item[1], low=item[2], close=item[3], volume=item[4], timestamp=item[5] * time_span, symbol=symbol)
        if _check_if_ohclv_stored(db, timestamp=ohlcv_model.timestamp) is True:
            ohlcv_update_items.append(ohlcv_model)
            timestamps.append(ohlcv_model.timestamp)
        else:
            ohlcv_insert_items.append(ohlcv_model)
            timestamps.append(ohlcv_model.timestamp)

    # Check if no trade when before step.
    check_timestamp = int(min_unix_timestamp // 1000)
    if (
        check_timestamp not in timestamps
        and _check_if_ohclv_stored(db, timestamp=check_timestamp) is False
        and _check_if_ohclv_stored(db, timestamp=check_timestamp - time_span) is True
    ):
        ohlcv_item = db.execute(text("select * from ohlcv where ohlcv.timestamp = :timestamp"), {"timestamp": check_timestamp - time_span}).all()
        close = ohlcv_item[0].close
        ohlcv_insert_items.append(schemas.OHLCVCreate(timestamp=check_timestamp, open=close, low=close, high=close, close=close, volume=0.0, symbol=symbol))

    insert_ohlcv_items(db=db, insert_items=ohlcv_insert_items, max_rows=max_rows)
    update_ohlcv_items(db=db, update_items=ohlcv_update_items)


# PREDICT methods
def insert_predict_items(db: Session, insert_items: List[Dict]):
    predict_items = []
    for item in insert_items:
        item["id"] = uuid.uuid4().hex
        item["timestamp"] = round(time.time() * 1000)
        predict_items.append(models.PREDICT(**item))

    db.add_all(predict_items)
    db.commit()


def get_predict_items(db: Session, symbol: str):
    return db.query(models.PREDICT).filter(models.PREDICT.symbol == symbol).order_by(models.PREDICT.timestamp).all()


# Predict calculation
def get_prediction_info(db: Session, symbol: str) -> schemas.PreidictInfo:
    # Do predict calculation.

    # Get Best bid & best ask
    buy_board_items, sell_board_items = get_current_board(db=db, symbol=symbol)

    # Get ohlcv
    ohlcv_df = get_ohlcv_with_symbol(db=db, limit=5, as_df=True, ascending=False, symbol=symbol)
    ohlcv_df = ohlcv_df.sort_values("timestamp")

    prediction = get_prediction(ohlcv_df)

    if len(buy_board_items) > 0 and len(sell_board_items) > 0 and prediction is not None:
        best_bid, best_ask = buy_board_items[-1], sell_board_items[0]
        # spread = best_ask.price - best_bid.price
        return schemas.PreidictInfo(
            is_buy_entry=prediction["is_buy_entry"],
            is_sell_entry=prediction["is_sell_entry"],
            buy_price=best_bid.price + 1,
            sell_price=best_ask.price - 1,
            buy_size=0.01,
            sell_size=0.01,
            buy_predict_value=prediction["buy_predict_value"],
            sell_predict_value=prediction["sell_predict_value"],
        )
    else:
        return schemas.PreidictInfo(
            is_buy_entry=False,
            is_sell_entry=False,
            buy_price=0.0,
            sell_price=0.0,
            buy_size=0.0,
            sell_size=0.0,
            buy_predict_value=0.0,
            sell_predict_value=0.0,
        )


# Ryotaro trade
def get_prediction(ohlcv_df: pd.DataFrame) -> Optional[Dict]:
    # ===================
    #  Price Transform Functions
    # ===================
    if len(ohlcv_df) > 0:
        # 予測値の計算
        # open, high, low, close = ohlcv_df["open"].iloc[-1], ohlcv_df["high"].iloc[-1], ohlcv_df["low"].iloc[-1], ohlcv_df["close"].iloc[-1]
        # predict_value = close / ((open + high + low + close) / 4)
        ohlcv_df["up_candle"] = ohlcv_df["close"] / ohlcv_df["open"] - 1
        predict_value = len(ohlcv_df.loc[ohlcv_df["up_candle"] > 0])

        buy_threshhold = 1
        if predict_value != 1.0:
            is_buy_entry = predict_value > buy_threshhold
            return {"is_buy_entry": is_buy_entry, "is_sell_entry": not is_buy_entry, "buy_predict_value": predict_value, "sell_predict_value": predict_value}
        else:
            return {"is_buy_entry": False, "is_sell_entry": False, "buy_predict_value": predict_value, "sell_predict_value": predict_value}
    else:
        return None
