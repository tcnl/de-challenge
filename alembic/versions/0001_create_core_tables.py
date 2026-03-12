"""create core ingestion tables and analytics view

Revision ID: 0001_create_core_tables
Revises:
Create Date: 2026-03-09 17:10:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0001_create_core_tables"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    job_status = postgresql.ENUM("queued", "processing", "completed", "failed", name="job_status")
    job_status_column = postgresql.ENUM(
        "queued",
        "processing",
        "completed",
        "failed",
        name="job_status",
        create_type=False,
    )

    op.execute("CREATE EXTENSION IF NOT EXISTS postgis")
    job_status.create(bind, checkfirst=True)

    op.create_table(
        "ingestion_jobs",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("source_name", sa.String(length=255), nullable=False),
        sa.Column("original_filename", sa.String(length=255), nullable=False),
        sa.Column("storage_path", sa.Text(), nullable=False),
        sa.Column("file_hash", sa.String(length=64), nullable=False),
        sa.Column("status", job_status_column, nullable=False),
        sa.Column("rows_total", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("rows_processed", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("rows_failed", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("started_at", sa.DateTime(timezone=False), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=False), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=False), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=False), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_ingestion_jobs_file_hash", "ingestion_jobs", ["file_hash"])
    op.create_index("ix_ingestion_jobs_status", "ingestion_jobs", ["status"])

    op.execute(
        """
        CREATE TABLE trips (
            id BIGSERIAL PRIMARY KEY,
            region VARCHAR(255) NOT NULL,
            origin_wkt TEXT NOT NULL,
            destination_wkt TEXT NOT NULL,
            trip_ts TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            datasource VARCHAR(255) NOT NULL,
            ingestion_job_id UUID NOT NULL REFERENCES ingestion_jobs(id) ON DELETE CASCADE,
            row_hash VARCHAR(64) NOT NULL UNIQUE,
            created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT now(),
            origin_geom geometry(Point, 4326) GENERATED ALWAYS AS (
                ST_SetSRID(ST_GeomFromText(origin_wkt), 4326)
            ) STORED,
            destination_geom geometry(Point, 4326) GENERATED ALWAYS AS (
                ST_SetSRID(ST_GeomFromText(destination_wkt), 4326)
            ) STORED,
            origin_geohash TEXT GENERATED ALWAYS AS (
                ST_GeoHash(ST_SetSRID(ST_GeomFromText(origin_wkt), 4326), 6)
            ) STORED,
            destination_geohash TEXT GENERATED ALWAYS AS (
                ST_GeoHash(ST_SetSRID(ST_GeomFromText(destination_wkt), 4326), 6)
            ) STORED,
            time_bucket TEXT GENERATED ALWAYS AS (
                CASE
                    WHEN EXTRACT(HOUR FROM trip_ts) BETWEEN 0 AND 5 THEN 'night'
                    WHEN EXTRACT(HOUR FROM trip_ts) BETWEEN 6 AND 11 THEN 'morning'
                    WHEN EXTRACT(HOUR FROM trip_ts) BETWEEN 12 AND 17 THEN 'afternoon'
                    ELSE 'evening'
                END
            ) STORED,
            week_start DATE GENERATED ALWAYS AS (
                date_trunc('week', trip_ts)::date
            ) STORED,
            day_date DATE GENERATED ALWAYS AS (
                trip_ts::date
            ) STORED
        )
        """
    )
    op.create_index("ix_trips_region", "trips", ["region"])
    op.create_index("ix_trips_trip_ts", "trips", ["trip_ts"])
    op.create_index("ix_trips_datasource", "trips", ["datasource"])
    op.create_index("ix_trips_ingestion_job_id", "trips", ["ingestion_job_id"])
    op.create_index("ix_trips_row_hash", "trips", ["row_hash"], unique=True)
    op.execute("CREATE INDEX ix_trips_week_start ON trips (week_start)")
    op.execute("CREATE INDEX ix_trips_grouping ON trips (region, origin_geohash, destination_geohash, time_bucket, week_start)")
    op.execute("CREATE INDEX ix_trips_origin_geom_gist ON trips USING GIST (origin_geom)")
    op.execute("CREATE INDEX ix_trips_destination_geom_gist ON trips USING GIST (destination_geom)")
    op.execute(
        """
        CREATE OR REPLACE VIEW trip_groups_daily AS
        SELECT
            region,
            origin_geohash,
            destination_geohash,
            time_bucket,
            week_start,
            day_date,
            COUNT(*) AS trip_count
        FROM trips
        GROUP BY region, origin_geohash, destination_geohash, time_bucket, week_start, day_date
        """
    )


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS trip_groups_daily")
    op.drop_index("ix_trips_destination_geom_gist", table_name="trips")
    op.drop_index("ix_trips_origin_geom_gist", table_name="trips")
    op.execute("DROP INDEX IF EXISTS ix_trips_grouping")
    op.execute("DROP INDEX IF EXISTS ix_trips_week_start")
    op.drop_index("ix_trips_row_hash", table_name="trips")
    op.drop_index("ix_trips_ingestion_job_id", table_name="trips")
    op.drop_index("ix_trips_datasource", table_name="trips")
    op.drop_index("ix_trips_trip_ts", table_name="trips")
    op.drop_index("ix_trips_region", table_name="trips")
    op.drop_table("trips")
    op.drop_index("ix_ingestion_jobs_status", table_name="ingestion_jobs")
    op.drop_index("ix_ingestion_jobs_file_hash", table_name="ingestion_jobs")
    op.drop_table("ingestion_jobs")
    sa.Enum("queued", "processing", "completed", "failed", name="job_status").drop(op.get_bind(), checkfirst=True)
