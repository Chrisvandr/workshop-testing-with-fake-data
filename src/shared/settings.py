import logging

from pydantic import computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    The environment variables of pydantic-settings are case-insensitive
    in parsing the environment variables.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_ignore_empty=True,
        extra="ignore",
    )

    # API Settings
    API_STRING: str = "/api"
    PROJECT_NAME: str = "Workshop Testing with Fake Data"

    LOCAL_DEVELOPMENT: bool = False
    ROLLBACK_TRANSACTIONS: bool = True
    LOG_LEVEL: int = logging.INFO

    PANDAS_DISPLAY_MAX_COLUMNS: int = 30
    PANDAS_DISPLAY_WIDTH: int = 1000

    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 9999
    POSTGRES_TEST_PORT: int = 8888
    POSTGRES_DB: str = "postgres"
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "postgres"

    ENVIRONMENT: str = "tst"

    @computed_field  # type: ignore[misc]
    @property
    def database_url(self) -> str:
        """Database URL for the application."""
        return (
            f"postgresql+psycopg2://{self.POSTGRES_USER}:"
            f"{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:"
            f"{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )


settings = Settings()
