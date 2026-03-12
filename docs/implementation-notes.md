# Implementation Notes

This document explains the main design decisions in straightforward terms so the repository is easier to review without deep familiarity with FastAPI, PostGIS, or Spark.

## Stack Choice
- `FastAPI` provides a small REST API for on-demand ingestion and analytics.
- `PostgreSQL` satisfies the SQL database requirement.
- `PostGIS` makes the bounding-box analytics practical and indexable.
- `SQLAlchemy` and `Alembic` keep the schema and migrations explicit.
- `PySpark` is used only for the scalable ingestion path.

## Main Ingestion Paths

### Local path
The default `execution_mode=local` path is optimized for simplicity.

Flow:
1. Save the uploaded CSV to disk.
2. Create an `ingestion_jobs` row.
3. Read and validate the CSV in a background worker.
4. Normalize coordinates and timestamps.
5. Compute a deterministic `row_hash`.
6. Insert rows in batches with idempotency enforced by `row_hash`.

Why:
- It is easy to run locally.
- It keeps the API responsive.
- It provides a clear reference implementation for the challenge requirements.

### Spark path
The `execution_mode=spark` path exists for the scalability requirement.

Flow:
1. The API still stores the file and creates a job.
2. It starts `spark-submit` as a separate process.
3. Spark reads, validates, and normalizes the CSV in parallel.
4. Spark writes to a staging table through JDBC.
5. PostgreSQL merges staged rows into `trips` with `ON CONFLICT (row_hash) DO NOTHING`.

Why:
- It keeps large-file processing out of the API worker.
- It is closer to the kind of ingestion path that can scale in production.

## Data Model

### `ingestion_jobs`
This table stores:
- source metadata
- execution mode
- status
- row counters
- timestamps
- terminal error messages

It powers both the job snapshot endpoint and the streaming status updates.

### `trips`
This is the canonical trip table.

It stores normalized raw values plus generated fields:
- `origin_geom`, `destination_geom`
- `origin_geohash`, `destination_geohash`
- `time_bucket`
- `week_start`
- `day_date`

This keeps the raw data auditable while also making reporting and geospatial filtering faster.

## How Similar Trips Are Grouped
The challenge asks for similar origin, destination, and time of day.

This implementation defines similarity as:
- origin similarity: `ST_GeoHash(origin_geom, 6)`
- destination similarity: `ST_GeoHash(destination_geom, 6)`
- time-of-day similarity: 4 fixed six-hour buckets
  - `night`
  - `morning`
  - `afternoon`
  - `evening`

The grouping logic is deterministic and encoded in the database schema.

## Weekly Average Analytics
The weekly average endpoint supports:
- filtering by `region`
- filtering by bounding box coordinates

The response includes:
- each weekly trip count
- the overall `average_trips_per_week`

Returning both values makes the result easier to verify and explain.

## Status Without Polling
The challenge requires a non-polling status mechanism.

This solution uses:
- PostgreSQL `LISTEN/NOTIFY`
- Server-Sent Events from FastAPI

This is simple to run locally and satisfies the requirement directly.

## Cloud Account Note
The challenge mentions providing credentials if the solution is integrated with a cloud platform.

For this submission:
- the cloud deployment is documented only
- the project is not hosted in a live cloud account
- there are therefore no cloud credentials to provide

That choice keeps the repository self-contained and locally testable.
