# Production Architecture Sketch

## Goal
Keep the local implementation small while showing a credible path to 100 million rows and the Spark/Kafka tooling the role expects. This repo now includes a concrete Spark batch ingestion job for the scalable path.

## Proposed AWS Layout
- `API`: FastAPI service on ECS Fargate behind an Application Load Balancer.
- `File landing`: CSV uploads stored in S3 with object versioning enabled.
- `Control events`: API emits ingestion-job metadata into Kafka on Amazon MSK.
- `High-scale ingestion`: Spark Structured Streaming on Databricks or EMR reads from MSK/S3, validates records, normalizes WKT fields, and writes partitioned batches into Aurora PostgreSQL with PostGIS.
- `Serving database`: Aurora PostgreSQL or RDS PostgreSQL with PostGIS, read replicas for analytics traffic, and range partitioning on `week_start`.
- `Status streaming`: ingestion workers write status updates into PostgreSQL and publish them via `LISTEN/NOTIFY`; API instances fan them out to SSE clients.
- `Observability`: CloudWatch logs and metrics, plus dashboards for job throughput, batch latency, failed rows, and slow queries.

## Scaling Notes
- Bulk-load path should switch from ORM batch inserts to `COPY` from Spark-generated files or batched JDBC writes.
- The local scalable implementation uses a PySpark batch job plus JDBC staging/merge, which is the code-level precursor to that production pipeline.
- Keep raw trips partitioned by `week_start` and optionally `region` to limit index size and vacuum costs.
- Maintain pre-aggregated grouped-trip tables or materialized views refreshed incrementally.
- Use Kafka to decouple upload spikes from database write throughput.
- Store benchmark evidence from local load tests together with `EXPLAIN ANALYZE` output for analytics queries.
