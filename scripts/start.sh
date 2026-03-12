#!/usr/bin/env sh
set -eu

attempt=1
max_attempts="${DB_START_MAX_ATTEMPTS:-30}"

until alembic upgrade head; do
  if [ "$attempt" -ge "$max_attempts" ]; then
    echo "Database did not become ready after $max_attempts attempts."
    exit 1
  fi

  echo "Waiting for database to accept connections ($attempt/$max_attempts)..."
  attempt=$((attempt + 1))
  sleep 2
done

exec uvicorn app.main:app --host 0.0.0.0 --port 8000
