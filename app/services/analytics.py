from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


async def fetch_grouped_trips(
    session: AsyncSession,
    *,
    region: str | None,
    start_date: date | None,
    end_date: date | None,
    time_bucket: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    conditions: list[str] = []
    params: dict[str, Any] = {"limit": limit}

    # Build only the filters that are present. This avoids asyncpg type
    # inference issues with "param IS NULL OR ..." query patterns.
    if region is not None:
        conditions.append("region = :region")
        params["region"] = region
    if start_date is not None:
        conditions.append("day_date >= :start_date")
        params["start_date"] = start_date
    if end_date is not None:
        conditions.append("day_date <= :end_date")
        params["end_date"] = end_date
    if time_bucket is not None:
        conditions.append("time_bucket = :time_bucket")
        params["time_bucket"] = time_bucket

    where_clause = ""
    if conditions:
        where_clause = "WHERE " + " AND ".join(conditions)

    query = text(
        f"""
        SELECT
            region,
            origin_geohash,
            destination_geohash,
            time_bucket,
            week_start,
            SUM(trip_count)::int AS trip_count
        FROM trip_groups_daily
        {where_clause}
        GROUP BY region, origin_geohash, destination_geohash, time_bucket, week_start
        ORDER BY week_start ASC, trip_count DESC
        LIMIT :limit
        """
    )
    result = await session.execute(query, params)
    return [dict(row._mapping) for row in result]


async def fetch_weekly_average(
    session: AsyncSession,
    *,
    region: str | None,
    min_lon: float | None,
    min_lat: float | None,
    max_lon: float | None,
    max_lat: float | None,
    start_date: date | None,
    end_date: date | None,
) -> dict[str, Any]:
    has_bbox = all(value is not None for value in (min_lon, min_lat, max_lon, max_lat))
    if not region and not has_bbox:
        raise HTTPException(status_code=400, detail="Provide either region or all bounding box coordinates.")
    if region and has_bbox:
        raise HTTPException(status_code=400, detail="Choose either region or bounding box, not both.")

    conditions: list[str] = []
    params: dict[str, Any] = {}

    # Region and bounding-box filters are intentionally exclusive so the metric
    # is unambiguous for callers and easy to explain in the API contract.
    if region is not None:
        conditions.append("region = :region")
        params["region"] = region
    if start_date is not None:
        conditions.append("trip_ts::date >= :start_date")
        params["start_date"] = start_date
    if end_date is not None:
        conditions.append("trip_ts::date <= :end_date")
        params["end_date"] = end_date
    if has_bbox:
        conditions.append(
            "("
            "origin_geom && ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326) "
            "OR destination_geom && ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)"
            ")"
        )
        params.update(
            {
                "min_lon": min_lon,
                "min_lat": min_lat,
                "max_lon": max_lon,
                "max_lat": max_lat,
            }
        )

    where_clause = ""
    if conditions:
        where_clause = "WHERE " + " AND ".join(conditions)

    query = text(
        f"""
        WITH filtered AS (
            SELECT
                week_start,
                COUNT(*)::int AS trip_count
            FROM trips
            {where_clause}
            GROUP BY week_start
        )
        SELECT week_start, trip_count
        FROM filtered
        ORDER BY week_start ASC
        """
    )
    result = await session.execute(query, params)
    weekly_trip_counts = [dict(row._mapping) for row in result]
    average = 0.0
    if weekly_trip_counts:
        average = sum(row["trip_count"] for row in weekly_trip_counts) / len(weekly_trip_counts)

    if region:
        filter_type = "region"
        filter_value = region
    else:
        filter_type = "bounding_box"
        filter_value = f"{min_lon},{min_lat},{max_lon},{max_lat}"

    return {
        "filter_type": filter_type,
        "filter_value": filter_value,
        "average_trips_per_week": average,
        "weekly_trip_counts": weekly_trip_counts,
    }
