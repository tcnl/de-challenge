from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "Trip Ingestion Challenge"
    environment: str = "development"
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/trips"
    ingest_batch_size: int = Field(default=500, ge=1, le=5000)
    sse_heartbeat_seconds: int = Field(default=15, ge=5, le=60)
    upload_dir: Path = Path("data/uploads")
    spark_submit_command: str = "spark-submit"
    spark_jdbc_packages: str = "org.postgresql:postgresql:42.7.5"

    @property
    def database_sync_url(self) -> str:
        return self.database_url.replace("+asyncpg", "+psycopg")

    @property
    def database_driverless_url(self) -> str:
        return self.database_url.replace("+asyncpg", "")


@lru_cache
def get_settings() -> Settings:
    settings = Settings()
    settings.upload_dir.mkdir(parents=True, exist_ok=True)
    return settings
