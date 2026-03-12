from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import asyncpg
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.db.models import IngestionJob
from app.db.session import AsyncSessionLocal

EVENT_CHANNEL = "ingestion_events"


def _serialize_job(job: IngestionJob, event: str) -> dict[str, Any]:
    return {
        "event": event,
        "job_id": str(job.id),
        "status": job.status.value,
        "rows_total": job.rows_total,
        "rows_processed": job.rows_processed,
        "rows_failed": job.rows_failed,
        "error_message": job.error_message,
        "timestamp": datetime.now(UTC).isoformat(),
    }


async def publish_job_event(session: AsyncSession, job: IngestionJob, event: str) -> None:
    payload = json.dumps(_serialize_job(job, event))
    await session.execute(text("SELECT pg_notify(:channel, :payload)"), {"channel": EVENT_CHANNEL, "payload": payload})


async def fetch_job_snapshot(job_id: UUID) -> dict[str, Any] | None:
    async with AsyncSessionLocal() as session:
        job = await session.get(IngestionJob, job_id)
        if job is None:
            return None
        return _serialize_job(job, "snapshot")


async def listen_for_job_events(job_id: UUID) -> AsyncIterator[dict[str, Any]]:
    settings = get_settings()
    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    conn = await asyncpg.connect(settings.database_driverless_url)

    def listener(_connection, _pid, _channel, payload: str) -> None:
        event = json.loads(payload)
        if event.get("job_id") == str(job_id):
            queue.put_nowait(event)

    await conn.add_listener(EVENT_CHANNEL, listener)
    try:
        while True:
            try:
                event = await asyncio.wait_for(queue.get(), timeout=settings.sse_heartbeat_seconds)
                yield event
                if event.get("status") in {"completed", "failed"}:
                    return
            except TimeoutError:
                yield {
                    "event": "heartbeat",
                    "job_id": str(job_id),
                    "timestamp": datetime.now(UTC).isoformat(),
                }
    finally:
        await conn.remove_listener(EVENT_CHANNEL, listener)
        await conn.close()


def format_sse(event: dict[str, Any]) -> str:
    return f"event: {event['event']}\ndata: {json.dumps(event, default=str)}\n\n"
