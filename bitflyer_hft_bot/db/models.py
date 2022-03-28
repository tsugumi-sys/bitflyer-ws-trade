from sqlalchemy import Column, Integer, Float, String, Boolean

from bitflyer_hft_bot.db.database import Base

# do not use relative import refere from the issue https://github.com/pallets/flask-sqlalchemy/issues/672


class Board(Base):
    __tablename__ = "board"

    # Generate id using uuid
    id = Column(String(128), primary_key=True)
    # Unix timestamp (ms) should be int converted from isoformat string
    timestamp = Column(Integer, index=True)
    price = Column(Float, index=True)
    size = Column(Float)
    side = Column(String(10), index=True)
    symbol = Column(String(10), index=True)


class Tick(Base):
    __tablename__ = "tick"

    # generate id using uuid
    id = Column(String(128), primary_key=True)
    # Unix timestamp (ms) should be int converted from isoformat string
    timestamp = Column(Integer, index=True)
    price = Column(Float, index=True)
    size = Column(Float)
    symbol = Column(String(10), index=True)
    side = Column(String(10))


class OHLCV(Base):
    __tablename__ = "ohlcv"

    # Unix timestamp (s).
    timestamp = Column(Integer, primary_key=True, index=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)
    symbol = Column(String(10), index=True)


class PREDICT(Base):
    __tablename__ = "predict"
    # generate id using uuid
    id = Column(String(128), primary_key=True)
    # Unix timestamp (s).
    timestamp = Column(Integer, index=True)
    side = Column(String(10))
    price = Column(Float)
    size = Column(Float)
    predict_value = Column(Float)
    symbol = Column(String(10), index=True)
    is_entry = Column(Boolean)
