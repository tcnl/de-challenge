from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse
from uuid import UUID

import psycopg
from psycopg import sql
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

REQUIRED_COLUMNS = {"region", "origin_coord", "destination_coord", "datetime", "datasource"}
POINT_PATTERN = r"^POINT \(([-0-9.]+) ([-0-9.]+)\)$"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Spark batch ingestion for trip CSV data.")
    parser.add_argument("--job-id", required=True, type=UUID)
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--database-url", required=True)
    return parser.parse_args()


def now_naive() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def sqlalchemy_sync_url_to_jdbc(database_url: str) -> tuple[str, str, str]:
    parsed = urlparse(database_url.replace("postgresql+psycopg://", "postgresql://"))
    jdbc_url = f"jdbc:postgresql://{parsed.hostname}:{parsed.port or 5432}{parsed.path}"
    return jdbc_url, parsed.username or "", parsed.password or ""


def normalize_psycopg_url(database_url: str) -> str:
    return database_url.replace("postgresql+psycopg://", "postgresql://")


def publish(conn: psycopg.Connection, payload: str) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_notify(%s, %s)", ("ingestion_events", payload))
    conn.commit()


def update_job(conn: psycopg.Connection, job_id: UUID, **fields: object) -> None:
    if not fields:
        return
    assignments = []
    params: list[object] = []
    for name, value in fields.items():
        assignments.append(sql.SQL("{} = %s").format(sql.Identifier(name)))
        params.append(value)
    params.append(job_id)
    query = sql.SQL("UPDATE ingestion_jobs SET {} WHERE id = %s").format(sql.SQL(", ").join(assignments))
    with conn.cursor() as cur:
        cur.execute(query, params)
    conn.commit()


def fetch_job_snapshot(conn: psycopg.Connection, job_id: UUID) -> str:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT json_build_object(
                'event', 'snapshot',
                'job_id', id::text,
                'status', status::text,
                'rows_total', rows_total,
                'rows_processed', rows_processed,
                'rows_failed', rows_failed,
                'error_message', error_message,
                'timestamp', now()::text
            )::text
            FROM ingestion_jobs
            WHERE id = %s
            """,
            (job_id,),
        )
        row = cur.fetchone()
    if row is None:
        raise RuntimeError(f"Unknown job: {job_id}")
    return row[0]


def main() -> None:
    args = parse_args()
    input_path = Path(args.input_path)
    spark = (
        SparkSession.builder.appName(f"trip-ingestion-{args.job_id}")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    normalized_database_url = normalize_psycopg_url(args.database_url)
    conn = psycopg.connect(normalized_database_url)
    stage_table = f"trips_stage_{str(args.job_id).replace('-', '_')}"

    try:
        update_job(conn, args.job_id, status="processing", started_at=now_naive(), error_message=None)
        publish(conn, fetch_job_snapshot(conn, args.job_id).replace('"snapshot"', '"processing"'))

        raw_df = spark.read.option("header", True).csv(str(input_path))
        if set(raw_df.columns) != REQUIRED_COLUMNS:
            raise ValueError(f"CSV header must be exactly: {sorted(REQUIRED_COLUMNS)}")

        normalized_df = (
            raw_df.select(
                F.trim("region").alias("region"),
                F.trim("origin_coord").alias("origin_coord"),
                F.trim("destination_coord").alias("destination_coord"),
                F.trim("datetime").alias("datetime"),
                F.trim("datasource").alias("datasource"),
            )
            .withColumn("origin_lon", F.regexp_extract("origin_coord", POINT_PATTERN, 1))
            .withColumn("origin_lat", F.regexp_extract("origin_coord", POINT_PATTERN, 2))
            .withColumn("destination_lon", F.regexp_extract("destination_coord", POINT_PATTERN, 1))
            .withColumn("destination_lat", F.regexp_extract("destination_coord", POINT_PATTERN, 2))
            .withColumn("trip_ts", F.to_timestamp("datetime", "yyyy-MM-dd HH:mm:ss"))
        )

        valid_df = normalized_df.where(
            (F.col("region") != "")
            & (F.col("datasource") != "")
            & (F.col("trip_ts").isNotNull())
            & (F.col("origin_lon") != "")
            & (F.col("origin_lat") != "")
            & (F.col("destination_lon") != "")
            & (F.col("destination_lat") != "")
        )
        rows_total = normalized_df.count()
        rows_valid = valid_df.count()
        rows_failed = rows_total - rows_valid

        prepared_df = (
            valid_df.withColumn("origin_wkt", F.format_string("POINT (%s %s)", "origin_lon", "origin_lat"))
            .withColumn("destination_wkt", F.format_string("POINT (%s %s)", "destination_lon", "destination_lat"))
            .withColumn(
                "row_hash",
                F.sha2(
                    F.concat_ws(
                        "|",
                        "region",
                        "origin_wkt",
                        "destination_wkt",
                        F.date_format("trip_ts", "yyyy-MM-dd HH:mm:ss"),
                        "datasource",
                    ),
                    256,
                ),
            )
            .withColumn("ingestion_job_id", F.lit(str(args.job_id)))
            .select("region", "origin_wkt", "destination_wkt", "trip_ts", "datasource", "ingestion_job_id", "row_hash")
        )

        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
                    CREATE TABLE {} (
                        region VARCHAR(255) NOT NULL,
                        origin_wkt TEXT NOT NULL,
                        destination_wkt TEXT NOT NULL,
                        trip_ts TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                        datasource VARCHAR(255) NOT NULL,
                        -- Keep staging permissive for JDBC writes; the final
                        -- merge casts this value back to UUID.
                        ingestion_job_id VARCHAR(36) NOT NULL,
                        row_hash VARCHAR(64) NOT NULL
                    )
                    """
                ).format(sql.Identifier(stage_table))
            )
        conn.commit()

        jdbc_url, username, password = sqlalchemy_sync_url_to_jdbc(normalized_database_url)
        (
            prepared_df.write.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", stage_table)
            .option("user", username)
            .option("password", password)
            .option("driver", "org.postgresql.Driver")
            .option("batchsize", 10000)
            .mode("append")
            .save()
        )

        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    """
                    -- Deduplicate only during the final merge so staging stays
                    -- lightweight and focused on bulk load throughput.
                    INSERT INTO trips (region, origin_wkt, destination_wkt, trip_ts, datasource, ingestion_job_id, row_hash)
                    SELECT region, origin_wkt, destination_wkt, trip_ts, datasource, ingestion_job_id::uuid, row_hash
                    FROM {}
                    ON CONFLICT (row_hash) DO NOTHING
                    """
                ).format(sql.Identifier(stage_table))
            )
            rows_processed = cur.rowcount or 0
            cur.execute(sql.SQL("DROP TABLE {}").format(sql.Identifier(stage_table)))
        conn.commit()

        update_job(
            conn,
            args.job_id,
            rows_total=rows_total,
            rows_processed=rows_processed,
            rows_failed=rows_failed,
            status="completed",
            finished_at=now_naive(),
        )
        publish(conn, fetch_job_snapshot(conn, args.job_id).replace('"snapshot"', '"completed"'))
    except Exception as exc:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(stage_table)))
        conn.commit()
        update_job(conn, args.job_id, status="failed", finished_at=now_naive(), error_message=str(exc))
        publish(conn, fetch_job_snapshot(conn, args.job_id).replace('"snapshot"', '"failed"'))
        raise
    finally:
        spark.stop()
        conn.close()
        input_path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
