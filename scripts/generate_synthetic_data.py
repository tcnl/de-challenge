from __future__ import annotations

import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

REGIONS = {
    "Prague": ((14.2, 14.7), (49.9, 50.2)),
    "Turin": ((7.5, 7.8), (44.9, 45.2)),
    "Hamburg": ((9.8, 10.2), (53.4, 53.7)),
}
DATASOURCES = ["cheap_mobile", "funny_car", "baba_car", "pt_search_app", "bad_diesel_vehicles"]


def point(lon_range: tuple[float, float], lat_range: tuple[float, float]) -> str:
    lon = random.uniform(*lon_range)
    lat = random.uniform(*lat_range)
    return f"POINT ({lon:.12f} {lat:.12f})"


def generate_rows(count: int):
    start = datetime(2018, 5, 1, 0, 0, 0)
    for _ in range(count):
        region, (lon_range, lat_range) = random.choice(list(REGIONS.items()))
        trip_ts = start + timedelta(minutes=random.randint(0, 60 * 24 * 120))
        yield {
            "region": region,
            "origin_coord": point(lon_range, lat_range),
            "destination_coord": point(lon_range, lat_range),
            "datetime": trip_ts.strftime("%Y-%m-%d %H:%M:%S"),
            "datasource": random.choice(DATASOURCES),
        }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic trip CSV data.")
    parser.add_argument("--rows", type=int, default=100000)
    parser.add_argument("--output", type=Path, default=Path("synthetic_trips.csv"))
    args = parser.parse_args()

    with args.output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["region", "origin_coord", "destination_coord", "datetime", "datasource"],
        )
        writer.writeheader()
        writer.writerows(generate_rows(args.rows))


if __name__ == "__main__":
    main()
