#!/usr/bin/env bash
set -euo pipefail

# Wait for Postgres to be ready
until pg_isready -h "${POSTGRES_HOST:-postgres}" -p "${POSTGRES_PORT:-5432}" -U "${POSTGRES_USER:-airflow}"; do
  echo "Waiting for Postgres..."
  sleep 2
done

# Initialize DB
airflow db migrate

# Create admin user if missing
airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com || true

# Set OpenWeather API key as Airflow Variable
if [ -n "${OPENWEATHER_API_KEY:-}" ]; then
  airflow variables set OPENWEATHER_API_KEY "${OPENWEATHER_API_KEY}"
fi