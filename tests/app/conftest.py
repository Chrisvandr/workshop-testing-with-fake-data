from collections.abc import Generator, Iterator

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlmodel import Session

from app.api.deps import get_db
from app.create_app import create_app


@pytest.fixture
def app() -> FastAPI:
    return create_app()


@pytest.fixture
def client(app: FastAPI) -> Iterator[TestClient]:
    return TestClient(app)


@pytest.fixture
def db_client(app: FastAPI, db_session: Session) -> Iterator[TestClient]:
    """writes to database"""

    def get_session_override() -> Generator[Session, None, None]:
        return db_session

    app.dependency_overrides[get_db] = get_session_override

    yield TestClient(app)

    app.dependency_overrides.clear()
