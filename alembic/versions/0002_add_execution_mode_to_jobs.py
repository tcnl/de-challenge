"""add execution mode to ingestion jobs

Revision ID: 0002_add_execution_mode_to_jobs
Revises: 0001_create_core_tables
Create Date: 2026-03-09 18:00:00
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0002_add_execution_mode_to_jobs"
down_revision = "0001_create_core_tables"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "ingestion_jobs",
        sa.Column("execution_mode", sa.String(length=32), nullable=False, server_default="local"),
    )
    op.execute("ALTER TABLE ingestion_jobs ALTER COLUMN execution_mode DROP DEFAULT")


def downgrade() -> None:
    op.drop_column("ingestion_jobs", "execution_mode")
