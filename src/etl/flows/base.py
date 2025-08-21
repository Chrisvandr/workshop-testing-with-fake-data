from typing import Self

import pandas as pd
from sqlalchemy import Engine, RowMapping, TextClause
from sqlalchemy.exc import SQLAlchemyError
from sqlmodel import Session, SQLModel
from structlog import get_logger

from etl.flows.utils import collect_validation_errors
from etl.settings.database import PostgresConnector

logger = get_logger(__name__)


class PostgresSqlModelExtractor:
    def __init__(self, connector: PostgresConnector, query: TextClause):
        self.connector = connector
        self.query = query

    def extract(self) -> list[RowMapping]:
        with Session(self.connector.engine) as session:
            logger.info("Opened connection to database. Start extracting data")
            return session.exec(self.query).all()


class SqlmodelLoader:
    def __init__(self, connector: PostgresConnector):
        self.connector = connector

    @classmethod
    def create(cls) -> Self:
        return cls(connector=PostgresConnector.create())

    @property
    def engine(self) -> Engine:
        return self.connector.engine

    @staticmethod
    def recreate_tables(models: list[type[SQLModel]], engine: Engine) -> None:
        tables = [
            SQLModel.metadata.tables[
                f"{table.__table_args__['schema']}.{table.__tablename__}"
            ]  # type: ignore
            for table in models
        ]
        logger.info(f"(Re) creating table: {tables}")

        SQLModel.metadata.drop_all(engine, tables=tables, checkfirst=True)
        SQLModel.metadata.create_all(engine, tables=tables, checkfirst=True)

    def recreate_and_load(
        self,
        tables_to_recreate: list[type[SQLModel]],
        objects: list[SQLModel],
    ):
        if len(objects) == 0:
            msg = "No objects found in the data. Please check the data and try again."
            raise ValueError(msg)

        logger.info(f"Loading {len(objects)} objects")
        with self.engine.begin() as connection:
            self.recreate_tables(tables_to_recreate, connection)

            session = Session(bind=connection)
            try:
                # Pass the session's connection so it joins the transaction.
                session.add_all(objects)
                session.flush()  # flush pending changes into the transaction
                logger.info("Transaction committed successfully.")
            except SQLAlchemyError as e:
                logger.error("Transaction failed, rolling back.", exc_info=e)
                raise e


class SqlmodelTransformer:
    @staticmethod
    def transform(
        model_class: type[SQLModel],
        records: list[RowMapping],
    ) -> list[SQLModel]:
        logger.info(
            f"Transforming sqlalchemy rows to objects of type {model_class.__name__}",
        )

        objects = []
        for record in records:
            obj = model_class(**record)

            if collect_validation_errors(obj):
                continue

            objects.append(obj)

        logger.info(f"Transformed {len(objects)} objects")

        return objects


class DataframeTransformer:
    @staticmethod
    def transform(model_class: type[SQLModel], df: pd.DataFrame) -> list[SQLModel]:
        logger.info(f"Transforming dataframe to objects of type {model_class.__name__}")
        objects = []
        for _, row in df.iterrows():
            obj = model_class(**row)

            if collect_validation_errors(obj):
                continue

            objects.append(obj)

        logger.info(f"Transformed {len(objects)} objects")
        return objects
