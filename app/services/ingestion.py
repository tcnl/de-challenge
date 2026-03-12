from __future__ import annotations

import asyncio
import csv
import hashlib
import re
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from fastapi import HTTPException, UploadFile
from sqlalchemy.dialects.postgresql import insert

from app.core.config import get_settings
from app.db.models import IngestionJob, JobStatus, Trip
from app.db.session import AsyncSessionLocal
from app.services.notifications import publish_job_event

POINT_PATTERN = re.compile(r"POINT \(([-0-9.]+) ([-0-9.]+)\)")
REQUIRED_FIELDS = {"region", "origin_coord", "destination_coord", "datetime", "datasource"}


@dataclass(slots=True)
class PreparedUpload:
    source_name: str
    original_filename: str
    file_hash: str
    storage_path: Path


def _utc_now_naive() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _normalize_point_wkt(value: str) -> str:
    match = POINT_PATTERN.fullmatch(value.strip())
    if match is None:
        raise ValueError(f"Invalid POINT literal: {value}")
    lon = float(match.group(1))
    lat = float(match.group(2))
    return f"POINT ({lon} {lat})"


def _parse_timestamp(value: str) -> datetime:
    return datetime.strptime(value.strip(), "%Y-%m-%d %H:%M:%S")


def _row_hash(region: str, origin_wkt: str, destination_wkt: str, trip_ts: datetime, datasource: str) -> str:
    payload = "|".join((region, origin_wkt, destination_wkt, trip_ts.isoformat(sep=" "), datasource))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class IngestionManager:
    def __init__(self) -> None:
        self.settings = get_settings()
        self._queue: asyncio.Queue[uuid.UUID] = asyncio.Queue()
        self._worker_task: asyncio.Task[None] | None = None
        self._spark_tasks: set[asyncio.Task[None]] = set()

    async def start(self) -> None:
        if self._worker_task is None:
            self._worker_task = asyncio.create_task(self._worker(), name="ingestion-worker")

    async def shutdown(self) -> None:
        if self._worker_task is None:
            pass
        else:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
            self._worker_task = None

        for task in list(self._spark_tasks):
            task.cancel()
        if self._spark_tasks:
            await asyncio.gather(*self._spark_tasks, return_exceptions=True)
        self._spark_tasks.clear()

    async def create_job(self, upload: UploadFile, source_name: str | None, execution_mode: str) -> IngestionJob:
        normalized_mode = execution_mode.strip().lower()
        if normalized_mode not in {"local", "spark"}:
            raise HTTPException(status_code=400, detail="execution_mode must be either 'local' or 'spark'.")
        prepared_upload = await self._persist_upload(upload, source_name)
        async with AsyncSessionLocal() as session:
            job = IngestionJob(
                source_name=prepared_upload.source_name,
                original_filename=prepared_upload.original_filename,
                storage_path=str(prepared_upload.storage_path),
                execution_mode=normalized_mode,
                file_hash=prepared_upload.file_hash,
                status=JobStatus.queued,
            )
            session.add(job)
            await session.flush()
            await publish_job_event(session, job, "queued")
            await session.commit()
            await session.refresh(job)
            if normalized_mode == "spark":
                # Large-file processing runs out-of-process so the API stays
                # responsive while Spark handles the heavier ingestion path.
                task = asyncio.create_task(self._run_spark_job(job.id), name=f"spark-ingestion-{job.id}")
                task.add_done_callback(self._spark_tasks.discard)
                self._spark_tasks.add(task)
            else:
                await self._queue.put(job.id)
            return job

    async def _persist_upload(self, upload: UploadFile, source_name: str | None) -> PreparedUpload:
        if not upload.filename:
            raise HTTPException(status_code=400, detail="Uploaded file must have a filename.")
        target_name = f"{uuid.uuid4()}-{Path(upload.filename).name}"
        storage_path = self.settings.upload_dir / target_name
        digest = hashlib.sha256()

        with storage_path.open("wb") as handle:
            while chunk := await upload.read(1024 * 1024):
                digest.update(chunk)
                handle.write(chunk)
        await upload.close()

        return PreparedUpload(
            source_name=source_name or Path(upload.filename).stem,
            original_filename=Path(upload.filename).name,
            file_hash=digest.hexdigest(),
            storage_path=storage_path,
        )

    async def _worker(self) -> None:
        while True:
            job_id = await self._queue.get()
            try:
                await self._process_job(job_id)
            finally:
                self._queue.task_done()

    async def _process_job(self, job_id: uuid.UUID) -> None:
        async with AsyncSessionLocal() as session:
            job = await session.get(IngestionJob, job_id)
            if job is None:
                return

            file_path = Path(job.storage_path)
            job.status = JobStatus.processing
            job.started_at = _utc_now_naive()
            job.error_message = None
            await publish_job_event(session, job, "processing")
            await session.commit()

            rows_total = 0
            rows_processed = 0
            rows_failed = 0
            batch: list[dict[str, object]] = []

            try:
                with file_path.open(newline="", encoding="utf-8") as handle:
                    reader = csv.DictReader(handle)
                    if reader.fieldnames is None or set(reader.fieldnames) != REQUIRED_FIELDS:
                        raise ValueError(f"CSV header must be exactly: {sorted(REQUIRED_FIELDS)}")

                    for row in reader:
                        rows_total += 1
                        try:
                            region = row["region"].strip()
                            origin_wkt = _normalize_point_wkt(row["origin_coord"])
                            destination_wkt = _normalize_point_wkt(row["destination_coord"])
                            trip_ts = _parse_timestamp(row["datetime"])
                            datasource = row["datasource"].strip()
                            batch.append(
                                {
                                    "region": region,
                                    "origin_wkt": origin_wkt,
                                    "destination_wkt": destination_wkt,
                                    "trip_ts": trip_ts,
                                    "datasource": datasource,
                                    "ingestion_job_id": job.id,
                                    "row_hash": _row_hash(region, origin_wkt, destination_wkt, trip_ts, datasource),
                                }
                            )
                        except Exception:
                            rows_failed += 1

                        if len(batch) >= self.settings.ingest_batch_size:
                            rows_processed += await self._flush_batch(session, batch)
                            batch.clear()
                            await self._update_progress(session, job, rows_total, rows_processed, rows_failed)

                if batch:
                    rows_processed += await self._flush_batch(session, batch)
                    batch.clear()

                job.status = JobStatus.completed
                job.rows_total = rows_total
                job.rows_processed = rows_processed
                job.rows_failed = rows_failed
                job.finished_at = _utc_now_naive()
                await publish_job_event(session, job, "completed")
                await session.commit()
            except Exception as exc:
                job.status = JobStatus.failed
                job.rows_total = rows_total
                job.rows_processed = rows_processed
                job.rows_failed = rows_failed
                job.finished_at = _utc_now_naive()
                job.error_message = str(exc)
                await publish_job_event(session, job, "failed")
                await session.commit()
            finally:
                file_path.unlink(missing_ok=True)

    async def _flush_batch(self, session, batch: list[dict[str, object]]) -> int:
        # The unique row_hash allows repeat ingestions to remain idempotent.
        statement = insert(Trip).values(batch).on_conflict_do_nothing(index_elements=[Trip.row_hash])
        result = await session.execute(statement)
        return int(result.rowcount or 0)

    async def _update_progress(
        self,
        session,
        job: IngestionJob,
        rows_total: int,
        rows_processed: int,
        rows_failed: int,
    ) -> None:
        job.rows_total = rows_total
        job.rows_processed = rows_processed
        job.rows_failed = rows_failed
        await publish_job_event(session, job, "progress")
        await session.commit()

    async def _run_spark_job(self, job_id: uuid.UUID) -> None:
        async with AsyncSessionLocal() as session:
            job = await session.get(IngestionJob, job_id)
            if job is None:
                return
            file_path = Path(job.storage_path)

        command = [
            self.settings.spark_submit_command,
            "--packages",
            self.settings.spark_jdbc_packages,
            "scripts/spark_batch_ingest.py",
            "--job-id",
            str(job_id),
            "--input-path",
            str(file_path),
            "--database-url",
            self.settings.database_driverless_url,
        ]
        process = None
        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            _stdout, stderr = await process.communicate()
            if process.returncode != 0:
                message = stderr.decode("utf-8", errors="replace").strip() or "spark-submit failed"
                await self._mark_job_failed(job_id, message[-4000:])
        except FileNotFoundError:
            await self._mark_job_failed(job_id, "spark-submit command not found. Install the spark extras or use local mode.")
        except asyncio.CancelledError:
            if process is not None and process.returncode is None:
                process.kill()
                await process.wait()
            raise
        except Exception as exc:
            await self._mark_job_failed(job_id, str(exc))

    async def _mark_job_failed(self, job_id: uuid.UUID, message: str) -> None:
        async with AsyncSessionLocal() as session:
            job = await session.get(IngestionJob, job_id)
            if job is None or job.status in {JobStatus.completed, JobStatus.failed}:
                return
            job.status = JobStatus.failed
            job.finished_at = _utc_now_naive()
            job.error_message = message
            await publish_job_event(session, job, "failed")
            await session.commit()
