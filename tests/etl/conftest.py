from unittest.mock import MagicMock

import pytest

from etl.apis.rest_client import AsyncRestClient, SyncRestClient


@pytest.fixture
def sync_client() -> MagicMock:
    return MagicMock(spec=SyncRestClient)


@pytest.fixture
def async_client() -> MagicMock:
    return MagicMock(spec=AsyncRestClient)
