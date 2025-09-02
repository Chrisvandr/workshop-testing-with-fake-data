from collections.abc import Iterator
from contextlib import contextmanager
from functools import lru_cache

from sqlalchemy import Engine
from sqlalchemy.schema import CreateSchema
from sqlmodel import Session, create_engine

# # NOTE: https://sqlmodel.tiangolo.com/tutorial/create-db-and-table/?h=metadata#sqlmodel-metadata-order-matters
from models import v1  # noqa: F401
from shared.constants import source
from shared.settings import settings


@lru_cache
def get_engine(testing: bool = False) -> Engine:
    if testing:
        engine = create_engine(
            f"postgresql+psycopg2://postgres:postgres@{settings.POSTGRES_HOST}:8888/postgres"
        )

        schema_names = [source]
        with Session(engine) as session:
            for schema_name in schema_names:
                session.exec(CreateSchema(schema_name, if_not_exists=True))
                session.commit()

        return engine
    return create_engine(str(settings.database_url))


def get_sessions() -> Iterator[Session]:
    """
    Used for FastAPI, provides a session for the request lifecycle.
    """
    engine = get_engine(testing=False)
    with Session(engine) as session:
        yield session


@contextmanager
def get_session(testing: bool = False) -> Iterator[Session]:
    """
    Context manager wrapper around get_sessions

    Args:
        testing: Whether the session is for testing purposes.
        schema_translate_map: Optional mapping for schema translation.
        connection_type: The type of connection to use (API or Step Functions),
        uses a different secret key for each.

    """
    engine = get_engine(testing=testing)
    with Session(engine) as session:
        yield session
