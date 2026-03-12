# Trip Ingestion Challenge

FastAPI service for asynchronous CSV ingestion, PostGIS-backed trip analytics, and server-sent ingestion status updates.

## Start Here
For a fresh clone and a full end-to-end walkthrough, see [TUTORIAL.md](TUTORIAL.md).

## What It Covers
- Automated CSV ingestion into PostgreSQL 16 + PostGIS
- Optional Spark-based ingestion path for large files
- Trip grouping by origin geohash, destination geohash, and time-of-day bucket
- Weekly average trip analytics by region or bounding box
- Push-based ingestion status updates through Server-Sent Events backed by PostgreSQL `LISTEN/NOTIFY`
- Dockerized local setup, bonus SQL queries, and a production AWS/Spark/Kafka architecture sketch

## Stack
- Python 3.12+
- FastAPI
- SQLAlchemy + Alembic
- PostgreSQL 16 + PostGIS
- pytest + Testcontainers
- Optional PySpark scalable ingestion path

## Quick Start
```bash
cp .env.example .env
docker compose up --build
```

The API will be available at `http://localhost:8000`.

For the Spark-enabled ingestion path:
```bash
docker compose -f docker-compose.yml -f docker-compose.spark.yml up --build
```

## Local Development
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
alembic upgrade head
uvicorn app.main:app --reload
```

## API
- `POST /ingestions`
  - multipart form with `file=@trips.csv`
  - optional `source_name` form field
  - optional `execution_mode` form field: `local` or `spark`
- `GET /ingestions/{job_id}`
- `GET /ingestions/{job_id}/events`
- `GET /analytics/groups?region=Prague&time_bucket=morning`
- `GET /analytics/weekly-average?region=Prague`
- `GET /analytics/weekly-average?min_lon=14.2&min_lat=49.9&max_lon=14.7&max_lat=50.2`

Example upload:
```bash
curl -X POST http://localhost:8000/ingestions \
  -F "file=@trips.csv" \
  -F "source_name=sample-dataset" \
  -F "execution_mode=local"
```

Spark-backed upload:
```bash
curl -X POST http://localhost:8000/ingestions \
  -F "file=@synthetic_trips.csv" \
  -F "source_name=synthetic-large" \
  -F "execution_mode=spark"
```

Example SSE stream:
```bash
curl -N http://localhost:8000/ingestions/<job_id>/events
```

## Schema Notes
- `ingestion_jobs` stores file metadata, progress counters, timestamps, and terminal errors.
- `trips` stores normalized raw fields plus generated PostGIS and grouping columns:
  - `origin_geom`, `destination_geom`
  - `origin_geohash`, `destination_geohash`
  - `time_bucket`, `week_start`, `day_date`
- `trip_groups_daily` is a SQL view that pre-groups trips for analytics endpoints.

## Testing
```bash
pytest
```

The test suite starts a disposable PostGIS container with Testcontainers, applies Alembic migrations, uploads CSV fixtures through the API, and validates ingestion, SSE, analytics, and deduplication behavior.

## Bonus Deliverables
- `Containerize your solution`: [Dockerfile](Dockerfile), [docker-compose.yml](docker-compose.yml), [Dockerfile.spark](Dockerfile.spark), [docker-compose.spark.yml](docker-compose.spark.yml)
- `Sketch up how you would set up the application using any cloud provider`: [docs/architecture.md](docs/architecture.md)
- `Include a .sql file with queries`: [sql/bonus_queries.sql](sql/bonus_queries.sql)

Run the bonus SQL file against the local database:
```bash
docker compose exec -T db psql -U postgres -d trips -f /dev/stdin < sql/bonus_queries.sql
```

## Scalability Proof
This repo includes the implementation hooks needed for larger-scale validation:
- spatial indexes on origin and destination geometries
- grouping indexes on `region`, geohashes, `time_bucket`, and `week_start`
- async background ingestion with batched inserts and idempotent row hashes
- a dedicated Spark batch ingestion job at `scripts/spark_batch_ingest.py`
- a synthetic data generator at `scripts/generate_synthetic_data.py`

Example synthetic data generation:
```bash
python scripts/generate_synthetic_data.py --rows 1000000 --output synthetic_trips.csv
```

For a production 100M-row path, the intended deployment uses S3 + Kafka/MSK + Spark Structured Streaming on Databricks or EMR feeding Aurora PostgreSQL/PostGIS. That architecture is described in [docs/architecture.md](docs/architecture.md).

The code-level scalable path and load-test procedure are documented in [docs/scalability.md](docs/scalability.md).

## Notes for Reviewers
- This repository is intentionally local-first. The cloud setup is documented in [docs/architecture.md](docs/architecture.md), but there is no live hosted environment in this submission, so there are no cloud credentials to provide for testing.
- To make the implementation easier to review regardless of platform familiarity, a plain-language walkthrough of the main design and code paths is included in [docs/implementation-notes.md](docs/implementation-notes.md).
