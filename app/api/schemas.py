from __future__ import annotations

from datetime import date, datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from app.db.models import JobStatus


class JobResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    source_name: str
    original_filename: str
    execution_mode: str
    file_hash: str
    status: JobStatus
    rows_total: int
    rows_processed: int
    rows_failed: int
    started_at: datetime | None
    finished_at: datetime | None
    error_message: str | None
    created_at: datetime
    updated_at: datetime


class IngestionCreatedResponse(BaseModel):
    job: JobResponse
    message: str


class GroupedTripResponse(BaseModel):
    region: str
    origin_geohash: str
    destination_geohash: str
    time_bucket: str
    week_start: date
    trip_count: int


class WeeklyTripCount(BaseModel):
    week_start: date
    trip_count: int


class WeeklyAverageResponse(BaseModel):
    filter_type: str
    filter_value: str
    average_trips_per_week: float
    weekly_trip_counts: list[WeeklyTripCount]
