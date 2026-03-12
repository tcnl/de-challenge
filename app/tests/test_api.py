from __future__ import annotations

import csv
import io
import json
import time
from pathlib import Path

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[2]


def create_csv_with_invalid_row() -> bytes:
    rows = [
        ["region", "origin_coord", "destination_coord", "datetime", "datasource"],
        ["Prague", "POINT (14.4973794438195 50.00136875782316)", "POINT (14.43109483523328 50.04052930943246)", "2018-05-28 09:03:40", "funny_car"],
        ["Prague", "INVALID", "POINT (14.47767895969969 50.09339790740321)", "2018-05-13 08:52:25", "cheap_mobile"],
    ]
    stream = io.StringIO()
    writer = csv.writer(stream)
    writer.writerows(rows)
    return stream.getvalue().encode("utf-8")


def create_bad_header_csv() -> bytes:
    return b"bad,header\n1,2\n"


def wait_for_status(client: TestClient, job_id: str, terminal: set[str] | None = None) -> dict:
    terminal = terminal or {"completed", "failed"}
    for _ in range(100):
        payload = client.get(f"/ingestions/{job_id}")
        assert payload.status_code == 200
        body = payload.json()
        if body["status"] in terminal:
            return body
        time.sleep(0.1)
    raise AssertionError(f"Job {job_id} did not reach a terminal state.")


def count_unique_rows(data: bytes) -> int:
    reader = csv.DictReader(io.StringIO(data.decode("utf-8")))
    return len({tuple(row.items()) for row in reader})


def test_ingestion_and_region_weekly_average(client: TestClient, sample_csv_bytes: bytes) -> None:
    response = client.post(
        "/ingestions",
        files={"file": ("trips.csv", sample_csv_bytes, "text/csv")},
        data={"source_name": "sample-data"},
    )
    assert response.status_code == 202
    job_id = response.json()["job"]["id"]

    job = wait_for_status(client, job_id)
    assert job["status"] == "completed"
    assert job["rows_total"] == 100
    assert job["rows_processed"] == 100
    assert job["rows_failed"] == 0

    analytics = client.get("/analytics/weekly-average", params={"region": "Prague"})
    assert analytics.status_code == 200
    payload = analytics.json()
    assert payload["filter_type"] == "region"
    assert payload["filter_value"] == "Prague"
    assert payload["weekly_trip_counts"]
    expected_average = sum(item["trip_count"] for item in payload["weekly_trip_counts"]) / len(payload["weekly_trip_counts"])
    assert payload["average_trips_per_week"] == expected_average


def test_malformed_rows_are_counted_without_failing_job(client: TestClient) -> None:
    response = client.post(
        "/ingestions",
        files={"file": ("invalid-row.csv", create_csv_with_invalid_row(), "text/csv")},
    )
    assert response.status_code == 202
    job_id = response.json()["job"]["id"]

    job = wait_for_status(client, job_id)
    assert job["status"] == "completed"
    assert job["rows_total"] == 2
    assert job["rows_processed"] == 1
    assert job["rows_failed"] == 1


def test_duplicate_ingestion_is_idempotent(client: TestClient, sample_csv_bytes: bytes, db_conn) -> None:
    first = client.post("/ingestions", files={"file": ("trips.csv", sample_csv_bytes, "text/csv")})
    second = client.post("/ingestions", files={"file": ("trips.csv", sample_csv_bytes, "text/csv")})

    first_job = wait_for_status(client, first.json()["job"]["id"])
    second_job = wait_for_status(client, second.json()["job"]["id"])

    assert first_job["status"] == "completed"
    assert second_job["status"] == "completed"
    assert second_job["rows_processed"] == 0

    with db_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM trips")
        row_count = cur.fetchone()[0]
    assert row_count == count_unique_rows(sample_csv_bytes)


def test_grouped_trips_and_bounding_box_query(client: TestClient, sample_csv_bytes: bytes) -> None:
    response = client.post("/ingestions", files={"file": ("trips.csv", sample_csv_bytes, "text/csv")})
    job = wait_for_status(client, response.json()["job"]["id"])
    assert job["status"] == "completed"

    groups = client.get(
        "/analytics/groups",
        params={"region": "Prague", "time_bucket": "morning", "start_date": "2018-05-01", "end_date": "2018-05-31"},
    )
    assert groups.status_code == 200
    rows = groups.json()
    assert rows
    assert all(row["region"] == "Prague" for row in rows)
    assert all(row["time_bucket"] == "morning" for row in rows)

    bbox = client.get(
        "/analytics/weekly-average",
        params={"min_lon": 14.2, "min_lat": 49.9, "max_lon": 14.7, "max_lat": 50.2},
    )
    assert bbox.status_code == 200
    payload = bbox.json()
    assert payload["filter_type"] == "bounding_box"
    assert payload["weekly_trip_counts"]


def test_sse_stream_reports_progress_and_completion(client: TestClient, sample_csv_bytes: bytes) -> None:
    response = client.post("/ingestions", files={"file": ("trips.csv", sample_csv_bytes, "text/csv")})
    job_id = response.json()["job"]["id"]

    statuses: list[str] = []
    with client.stream("GET", f"/ingestions/{job_id}/events") as stream:
        assert stream.status_code == 200
        for line in stream.iter_lines():
            if not line.startswith("data: "):
                continue
            payload = json.loads(line.removeprefix("data: "))
            if "status" in payload:
                statuses.append(payload["status"])
            if payload.get("status") == "completed":
                break

    assert "processing" in statuses or "queued" in statuses
    assert "completed" in statuses
    snapshot = client.get(f"/ingestions/{job_id}").json()
    assert snapshot["status"] == statuses[-1]


def test_failed_job_emits_terminal_failure(client: TestClient) -> None:
    response = client.post("/ingestions", files={"file": ("bad-header.csv", create_bad_header_csv(), "text/csv")})
    job_id = response.json()["job"]["id"]

    statuses: list[str] = []
    with client.stream("GET", f"/ingestions/{job_id}/events") as stream:
        for line in stream.iter_lines():
            if not line.startswith("data: "):
                continue
            payload = json.loads(line.removeprefix("data: "))
            if "status" in payload:
                statuses.append(payload["status"])
            if payload.get("status") == "failed":
                break

    assert "failed" in statuses
    job = client.get(f"/ingestions/{job_id}").json()
    assert job["status"] == "failed"
