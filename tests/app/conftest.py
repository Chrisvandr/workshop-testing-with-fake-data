from collections.abc import Generator, Iterator

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlmodel import Session

from app.create_app import create_app
from shared.engine import get_sessions


@pytest.fixture
def app() -> FastAPI:
    return create_app()


@pytest.fixture
def api_client(app: FastAPI) -> Iterator[TestClient]:
    return TestClient(app)


@pytest.fixture
def client(app: FastAPI, session: Session) -> Iterator[TestClient]:
    """writes to the database running in the docker container"""

    def get_session_override() -> Generator[Session, None, None]:
        return session

    app.dependency_overrides[get_sessions] = get_session_override

    yield TestClient(app)

    app.dependency_overrides.clear()
