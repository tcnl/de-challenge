# Scalability Proof and High-Scale Path

## What Is Implemented
- The default API path uses async batched inserts and SQL/PostGIS indexes for small to medium files.
- The repo now also includes a Spark batch ingestion path at `scripts/spark_batch_ingest.py`.
- `POST /ingestions` accepts `execution_mode=local` or `execution_mode=spark`.
- In `spark` mode, the API persists the upload, creates an ingestion job, and spawns `spark-submit` to process the file with PySpark.

## Why This Scales Better
- Spark reads and validates the CSV in parallel instead of row-by-row in the API process.
- Spark writes valid rows to PostgreSQL through JDBC in large batches.
- The final merge into `trips` still preserves idempotency through the unique `row_hash` constraint.
- Spatial and grouping indexes remain in PostgreSQL, which keeps analytics query behavior unchanged.

## Suggested Load-Test Procedure
1. Generate a large input file:
   ```bash
   python scripts/generate_synthetic_data.py --rows 1000000 --output synthetic_1m.csv
   ```
2. Start the Spark-enabled stack:
   ```bash
   docker compose -f docker-compose.yml -f docker-compose.spark.yml up --build
   ```
3. Upload the file with Spark mode:
   ```bash
   curl -X POST http://localhost:8000/ingestions \
     -F "file=@synthetic_1m.csv" \
     -F "source_name=synthetic-1m" \
     -F "execution_mode=spark"
   ```
4. Record:
   - end-to-end ingestion time
   - rows processed per minute
   - `EXPLAIN ANALYZE` for `GET /analytics/weekly-average`
   - index sizes and table growth

## 100M-Row Production Direction
- Move uploads to S3 instead of local disk.
- Trigger Spark jobs on Databricks or EMR rather than running `spark-submit` inside the API container.
- Replace a single PostgreSQL instance with Aurora PostgreSQL or RDS PostgreSQL plus read replicas.
- Partition `trips` by time window in the production database and refresh pre-aggregated reporting tables incrementally.
- Put Kafka/MSK in front of Spark when ingestion volume becomes event-driven rather than file-driven.
