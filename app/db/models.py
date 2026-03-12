from __future__ import annotations

import enum
import uuid
from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Enum, ForeignKey, Identity, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class JobStatus(str, enum.Enum):
    queued = "queued"
    processing = "processing"
    completed = "completed"
    failed = "failed"


class IngestionJob(Base):
    __tablename__ = "ingestion_jobs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source_name: Mapped[str] = mapped_column(String(255), nullable=False)
    original_filename: Mapped[str] = mapped_column(String(255), nullable=False)
    storage_path: Mapped[str] = mapped_column(Text, nullable=False)
    execution_mode: Mapped[str] = mapped_column(String(32), nullable=False, default="local")
    file_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    status: Mapped[JobStatus] = mapped_column(Enum(JobStatus, name="job_status"), nullable=False, default=JobStatus.queued)
    rows_total: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    rows_processed: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    rows_failed: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=False), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), server_default=func.now(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=False),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    trips: Mapped[list["Trip"]] = relationship(back_populates="job")


class Trip(Base):
    __tablename__ = "trips"

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=False), primary_key=True)
    region: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    origin_wkt: Mapped[str] = mapped_column(Text, nullable=False)
    destination_wkt: Mapped[str] = mapped_column(Text, nullable=False)
    trip_ts: Mapped[datetime] = mapped_column(DateTime(timezone=False), nullable=False, index=True)
    datasource: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    ingestion_job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("ingestion_jobs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    row_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=False), server_default=func.now(), nullable=False)

    job: Mapped[IngestionJob] = relationship(back_populates="trips")
