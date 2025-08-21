from unittest.mock import MagicMock

import pytest
from sqlalchemy import Engine

from etl.apis.rest_client import AsyncRestClient, SyncRestClient
from etl.flows.base import SqlmodelLoader
from etl.settings.database import PostgresConnector


@pytest.fixture
def postgres_connector(engine: Engine) -> MagicMock:
    return MagicMock(spec=PostgresConnector, engine=engine)


@pytest.fixture
def postgres_loader(postgres_connector: PostgresConnector) -> SqlmodelLoader:
    return SqlmodelLoader(connector=postgres_connector)


@pytest.fixture
def sync_client() -> MagicMock:
    return MagicMock(spec=SyncRestClient)


@pytest.fixture
def async_client() -> MagicMock:
    return MagicMock(spec=AsyncRestClient)
