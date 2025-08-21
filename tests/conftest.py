from collections.abc import Iterator
from datetime import date

import pandas as pd
import pytest
import structlog
from _pytest.config import Config
from factory.alchemy import SQLAlchemyModelFactory
from factory.random import reseed_random
from sqlalchemy import Engine
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


@pytest.fixture
def engine() -> Iterator[Session]:
    return get_engine(testing=True)


@pytest.fixture
def db_session(engine: Engine) -> Iterator[Session]:
    # ensure deletion of existing table data
    SQLModel.metadata.drop_all(engine)

    # Create tables for all SQLModel models
    SQLModel.metadata.create_all(engine)

    # Create a session for the test
    with Session(engine) as session:
        # Ensure that all factories use the same session
        for factory in SQLAlchemyModelFactory.__subclasses__():
            factory._meta.sqlalchemy_session = session

        yield session


@pytest.fixture
def log_output() -> LogCapture:
    return LogCapture()
