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
