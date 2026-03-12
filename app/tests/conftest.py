from __future__ import annotations

import importlib
import os
import subprocess
import tempfile
from collections.abc import Iterator
from pathlib import Path

import docker
import psycopg
import pytest
from fastapi.testclient import TestClient
from docker.errors import DockerException
from testcontainers.postgres import PostgresContainer

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="session")
def postgres_env() -> Iterator[dict[str, str]]:
    try:
        docker.from_env().ping()
    except DockerException as exc:
        pytest.skip(f"Docker daemon unavailable for integration tests: {exc}")

    uploads_dir = tempfile.TemporaryDirectory()
    with PostgresContainer("postgis/postgis:16-3.4", dbname="trips", username="postgres", password="postgres") as container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(5432)
        async_url = f"postgresql+asyncpg://postgres:postgres@{host}:{port}/trips"
        sync_url = f"postgresql+psycopg://postgres:postgres@{host}:{port}/trips"

        env = os.environ.copy()
        env["DATABASE_URL"] = async_url
        env["UPLOAD_DIR"] = uploads_dir.name
        subprocess.run(["alembic", "upgrade", "head"], cwd=ROOT, env=env, check=True)

        old_database = os.environ.get("DATABASE_URL")
        old_upload = os.environ.get("UPLOAD_DIR")
        os.environ["DATABASE_URL"] = async_url
        os.environ["UPLOAD_DIR"] = uploads_dir.name

        try:
            yield {"async_url": async_url, "sync_url": sync_url}
        finally:
            if old_database is None:
                os.environ.pop("DATABASE_URL", None)
            else:
                os.environ["DATABASE_URL"] = old_database
            if old_upload is None:
                os.environ.pop("UPLOAD_DIR", None)
            else:
                os.environ["UPLOAD_DIR"] = old_upload
            uploads_dir.cleanup()


@pytest.fixture(scope="session")
def client(postgres_env: dict[str, str]) -> Iterator[TestClient]:
    import app.core.config as config_module

    config_module.get_settings.cache_clear()
    main_module = importlib.import_module("app.main")

    with TestClient(main_module.app) as test_client:
        yield test_client


@pytest.fixture()
def db_conn(postgres_env: dict[str, str]) -> Iterator[psycopg.Connection]:
    with psycopg.connect(postgres_env["sync_url"]) as conn:
        yield conn


@pytest.fixture(autouse=True)
def reset_db(db_conn: psycopg.Connection) -> Iterator[None]:
    with db_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE trips, ingestion_jobs RESTART IDENTITY CASCADE")
    db_conn.commit()
    yield


@pytest.fixture()
def sample_csv_bytes() -> bytes:
    return (ROOT / "trips.csv").read_bytes()
