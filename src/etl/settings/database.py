from typing import Self

from sqlalchemy import Engine

from shared.engine import get_engine


class PostgresConnector:
    @classmethod
    def create(cls) -> Self:
        return cls()

    @property
    def driver(self) -> str:
        return "postgresql+psycopg2"

    @property
    def engine(self) -> Engine:
        return get_engine()
