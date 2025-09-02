from collections.abc import Iterator
from datetime import date

import pandas as pd
import pytest
import structlog
from _pytest.config import Config
from factory.alchemy import SQLAlchemyModelFactory
from factory.random import reseed_random
from sqlalchemy import Engine, event
from sqlalchemy.orm.session import SessionTransaction
from sqlmodel import Session, SQLModel
from structlog.testing import LogCapture

from shared.engine import get_engine
from shared.settings import settings

pd.options.mode.copy_on_write = True
pd.set_option("display.max_columns", settings.PANDAS_DISPLAY_MAX_COLUMNS)
pd.set_option("display.width", settings.PANDAS_DISPLAY_WIDTH)


@pytest.fixture
def current_year() -> int:
    return date.today().year


@pytest.fixture
def gm_code() -> str:
    return "GM1234"


@pytest.fixture(autouse=True)
def fixture_configure_structlog(log_output: LogCapture):
    structlog.configure(processors=[log_output])


def pytest_collection_modifyitems(config: Config, items):  # noqa: ANN001, ARG001
    # any tests not marked will be marked as 'unit'
    for item in items:
        if not any(item.iter_markers()):
            item.add_marker("unit")


@pytest.fixture(autouse=True)
def set_factory_seed():
    reseed_random("workshop")


@pytest.fixture(scope="session")
def engine() -> Iterator[Engine]:
    """
    Create test engine and ensure clean state.
    This is a scoped session fixture so it runs only once
    """
    _engine = get_engine(testing=True)

    # Ensure clean state at start of test session
    SQLModel.metadata.drop_all(_engine)
    SQLModel.metadata.create_all(_engine)

    yield _engine

    # Clean up after all tests
    SQLModel.metadata.drop_all(_engine)
    _engine.dispose()


def _rollback_session(engine: Engine) -> Iterator[Session]:
    """Create a database session with transaction rollback after each test."""
    connection = engine.connect()
    transaction = connection.begin()

    try:
        session = Session(bind=connection, autoflush=False, expire_on_commit=False)

        # Configure FactoryBoy to use this session
        for factory in SQLAlchemyModelFactory.__subclasses__():
            factory._meta.sqlalchemy_session = session

        # Enable savepoints for nested transactions
        @event.listens_for(session, "after_transaction_end")
        def restart_savepoint(
            session: Session, transaction: SessionTransaction
        ) -> None:
            if transaction.nested and not transaction._parent.nested:
                session.expire_all()
                session.begin_nested()

        # Start a nested transaction (savepoint)
        session.begin_nested()

        yield session

    finally:
        # Reset sequences after each test
        for factory in SQLAlchemyModelFactory.__subclasses__():
            factory.reset_sequence()

        session.close()
        transaction.rollback()
        connection.close()


def _write_session(engine: Engine) -> Iterator[Session]:
    """Writes to database during tests - data persists"""
    # Ensure clean state at start
    SQLModel.metadata.drop_all(engine)
    SQLModel.metadata.create_all(engine)

    # Create a session for the test
    with Session(engine) as session:
        # Configure FactoryBoy to use this session
        for factory in SQLAlchemyModelFactory.__subclasses__():
            factory._meta.sqlalchemy_session = session

        # Clean existing data
        for table in reversed(SQLModel.metadata.sorted_tables):
            session.exec(table.delete())
        session.commit()

        yield session

        # Reset sequences after test
        for factory in SQLAlchemyModelFactory.__subclasses__():
            factory.reset_sequence()


@pytest.fixture
def session(engine: Engine) -> Iterator[Session]:
    if settings.ROLLBACK_TRANSACTIONS:
        yield from _rollback_session(engine)
    else:
        yield from _write_session(engine)


@pytest.fixture
def log_output() -> LogCapture:
    return LogCapture()
