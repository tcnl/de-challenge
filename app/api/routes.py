from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import date
from uuid import UUID

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.schemas import GroupedTripResponse, IngestionCreatedResponse, JobResponse, WeeklyAverageResponse
from app.db.models import IngestionJob
from app.db.session import get_session
from app.services.analytics import fetch_grouped_trips, fetch_weekly_average
from app.services.ingestion import IngestionManager
from app.services.notifications import fetch_job_snapshot, format_sse, listen_for_job_events

router = APIRouter()


def get_ingestion_manager(request: Request) -> IngestionManager:
    return request.app.state.ingestion_manager


@router.get("/health")
async def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@router.post("/ingestions", response_model=IngestionCreatedResponse, status_code=202)
async def create_ingestion(
    file: UploadFile = File(...),
    source_name: str | None = Form(default=None),
    execution_mode: str = Form(default="local"),
    manager: IngestionManager = Depends(get_ingestion_manager),
) -> IngestionCreatedResponse:
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV uploads are supported.")
    job = await manager.create_job(file, source_name, execution_mode)
    return IngestionCreatedResponse(job=JobResponse.model_validate(job), message="Ingestion queued.")


@router.get("/ingestions/{job_id}", response_model=JobResponse)
async def get_ingestion(job_id: UUID, session: AsyncSession = Depends(get_session)) -> JobResponse:
    job = await session.get(IngestionJob, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found.")
    return JobResponse.model_validate(job)


@router.get("/ingestions/{job_id}/events")
async def stream_ingestion_events(job_id: UUID) -> StreamingResponse:
    snapshot = await fetch_job_snapshot(job_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail="Job not found.")

    async def event_generator() -> AsyncIterator[str]:
        yield format_sse(snapshot)
        if snapshot["status"] in {"completed", "failed"}:
            return
        async for event in listen_for_job_events(job_id):
            yield format_sse(event)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@router.get("/analytics/groups", response_model=list[GroupedTripResponse])
async def grouped_trips(
    region: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    time_bucket: str | None = None,
    limit: int = 100,
    session: AsyncSession = Depends(get_session),
) -> list[GroupedTripResponse]:
    rows = await fetch_grouped_trips(
        session,
        region=region,
        start_date=start_date,
        end_date=end_date,
        time_bucket=time_bucket,
        limit=limit,
    )
    return [GroupedTripResponse.model_validate(row) for row in rows]


@router.get("/analytics/weekly-average", response_model=WeeklyAverageResponse)
async def weekly_average(
    region: str | None = None,
    min_lon: float | None = None,
    min_lat: float | None = None,
    max_lon: float | None = None,
    max_lat: float | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    session: AsyncSession = Depends(get_session),
) -> WeeklyAverageResponse:
    payload = await fetch_weekly_average(
        session,
        region=region,
        min_lon=min_lon,
        min_lat=min_lat,
        max_lon=max_lon,
        max_lat=max_lat,
        start_date=start_date,
        end_date=end_date,
    )
    return WeeklyAverageResponse.model_validate(payload)
