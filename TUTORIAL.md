# End-to-End Tutorial

This file is the fastest way to run the project from a fresh clone and verify the main challenge requirements end to end.

## 1. Prerequisites
- Docker and Docker Compose
- `curl`
- `python3`

Optional for the scalable path:
- enough local resources for Spark and PostgreSQL running together

## 2. Clone and Enter the Repository
```bash
git clone <YOUR_PUBLIC_REPO_URL>
cd de-challenge
```

If you already have the repository locally, just `cd` into it.

## 3. Start the Default Local Stack
This starts:
- PostgreSQL + PostGIS
- the FastAPI application

```bash
cp .env.example .env
docker compose up --build
```

Keep this terminal open.

Open a second terminal in the same repository for the next steps.

## 4. Check That the API Is Running
```bash
curl http://localhost:8000/health
```

Expected response:
```json
{"status":"ok"}
```

## 5. Ingest the Sample CSV
The repository includes the sample dataset from the challenge as `trips.csv`.

```bash
curl -X POST http://localhost:8000/ingestions \
  -F "file=@trips.csv" \
  -F "source_name=sample-data" \
  -F "execution_mode=local"
```

The response will include a `job.id`. Copy it or capture it with:

```bash
RESPONSE=$(curl -s -X POST http://localhost:8000/ingestions \
  -F "file=@trips.csv" \
  -F "source_name=sample-data" \
  -F "execution_mode=local")

echo "$RESPONSE"

JOB_ID=$(python3 -c 'import json,sys; print(json.loads(sys.stdin.read())["job"]["id"])' <<< "$RESPONSE")
echo "$JOB_ID"
```

## 6. Check the Ingestion Result
```bash
curl http://localhost:8000/ingestions/$JOB_ID
```

Expected outcome:
- `status` should become `completed`
- `rows_total` should be `100`
- `rows_processed` should be `100`
- `rows_failed` should be `0`

## 7. Show Push-Based Status Updates
This demonstrates the non-polling requirement using Server-Sent Events.

```bash
curl -N http://localhost:8000/ingestions/$JOB_ID/events
```

If the job already finished, the endpoint still returns a final snapshot event.

## 8. Query Weekly Average by Region
```bash
curl "http://localhost:8000/analytics/weekly-average?region=Prague"
```

This satisfies the weekly average requirement using a region filter.

## 9. Query Weekly Average by Bounding Box
```bash
curl "http://localhost:8000/analytics/weekly-average?min_lon=14.2&min_lat=49.9&max_lon=14.7&max_lat=50.2"
```

This satisfies the weekly average requirement using coordinates.

## 10. Query Grouped Trips
```bash
curl "http://localhost:8000/analytics/groups?region=Prague&time_bucket=morning&start_date=2018-05-01&end_date=2018-05-31"
```

This demonstrates grouping by:
- similar origin
- similar destination
- similar time of day

The grouping is based on geohash precision 6 and fixed time-of-day buckets.

## 11. Demonstrate Failure Handling
Create an invalid CSV:

```bash
printf 'bad,header\n1,2\n' > bad.csv
```

Upload it:

```bash
BAD_RESPONSE=$(curl -s -X POST http://localhost:8000/ingestions \
  -F "file=@bad.csv" \
  -F "source_name=bad-data" \
  -F "execution_mode=local")

echo "$BAD_RESPONSE"

BAD_JOB_ID=$(python3 -c 'import json,sys; print(json.loads(sys.stdin.read())["job"]["id"])' <<< "$BAD_RESPONSE")
echo "$BAD_JOB_ID"
```

Inspect the failed job:

```bash
curl http://localhost:8000/ingestions/$BAD_JOB_ID
```

Expected outcome:
- `status` should become `failed`
- `error_message` should explain the header problem

## 12. Run the Bonus SQL Queries
```bash
docker compose exec -T db psql -U postgres -d trips -f /dev/stdin < sql/bonus_queries.sql
```

This runs both required bonus questions:
- latest datasource from the two most common regions
- regions where `cheap_mobile` appeared

## 13. Optional: Try the Scalable Spark Path
Stop the default stack first:

```bash
docker compose down
```

Start the Spark-enabled stack:

```bash
docker compose -f docker-compose.yml -f docker-compose.spark.yml up --build
```

Generate a larger synthetic file:

```bash
python3 scripts/generate_synthetic_data.py --rows 1000000 --output synthetic_trips.csv
```

Submit it in Spark mode:

```bash
SPARK_RESPONSE=$(curl -s -X POST http://localhost:8000/ingestions \
  -F "file=@synthetic_trips.csv" \
  -F "source_name=synthetic-large" \
  -F "execution_mode=spark")

echo "$SPARK_RESPONSE"

SPARK_JOB_ID=$(python3 -c 'import json,sys; print(json.loads(sys.stdin.read())["job"]["id"])' <<< "$SPARK_RESPONSE")
echo "$SPARK_JOB_ID"
```

Check the job:

```bash
curl http://localhost:8000/ingestions/$SPARK_JOB_ID
```

Stream events:

```bash
curl -N http://localhost:8000/ingestions/$SPARK_JOB_ID/events
```

## 14. Stop the Stack
Default stack:
```bash
docker compose down
```

Spark-enabled stack:
```bash
docker compose -f docker-compose.yml -f docker-compose.spark.yml down
```

## 15. Where to Read More
- Setup and summary: [README.md](README.md)
- Cloud architecture: [docs/architecture.md](docs/architecture.md)
- Scalability notes: [docs/scalability.md](docs/scalability.md)
- Implementation rationale: [docs/implementation-notes.md](docs/implementation-notes.md)
- Video walkthrough script: [video.md](video.md)
