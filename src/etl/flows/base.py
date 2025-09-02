import pandas as pd
from sqlalchemy import RowMapping
from sqlalchemy.exc import SQLAlchemyError
from sqlmodel import Session, SQLModel
from structlog import get_logger

from etl.flows.utils import collect_validation_errors

logger = get_logger(__name__)


class SqlmodelLoader:
    def __init__(self, session: Session):
        self.session = session

    def recreate_tables(self, models: list[type[SQLModel]]) -> None:
        tables = [
            SQLModel.metadata.tables[
                f"{table.__table_args__['schema']}.{table.__tablename__}"
            ]  # type: ignore
            for table in models
        ]
        logger.info(f"(Re) creating table: {tables}")

        bind = self.session.get_bind()

        SQLModel.metadata.drop_all(bind, tables=tables, checkfirst=True)
        SQLModel.metadata.create_all(bind, tables=tables, checkfirst=True)

    def recreate_and_load(
        self,
        tables_to_recreate: list[type[SQLModel]],
        objects: list[SQLModel],
    ):
        if len(objects) == 0:
            msg = "No objects found in the data. Please check the data and try again."
            raise ValueError(msg)

        logger.info(f"Loading {len(objects)} objects")
        try:
            self.recreate_tables(tables_to_recreate)
            self.session.add_all(objects)
            self.session.commit()
            logger.info("Transaction committed successfully.")

        except SQLAlchemyError as e:
            logger.error("Transaction failed, rolling back.", exc_info=e)
            self.session.rollback()
            raise e


class SqlmodelTransformer:
    @staticmethod
    def transform(
        model_class: type[SQLModel], records: list[RowMapping]
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
