from typing import Optional, Tuple

import sqlalchemy


def initialize_database(uri: Optional[str] = None) -> Tuple[sqlalchemy.engine.Engine, sqlalchemy.orm.Session]:
    """Initialize database engine and session

    Args:
        uri (Optional[str], optional): DB file uri. Defaults to None. If None, use in-memory db.

    Returns:
        Tuple[sqlalchemy.engine.Engine, sqlalchemy.orm.Session]: Engine and Session.
    """
    if uri is None:
        engine = sqlalchemy.create_engine("sqlite:///:memory:")
    else:
        engine = sqlalchemy.create_engine(uri)

    SessionLocal = sqlalchemy.orm.sessionmaker(bind=engine, autocommit=False, autoflush=False)

    return engine, SessionLocal


Base = sqlalchemy.orm.declarative_base()
