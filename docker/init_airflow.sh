#!/usr/bin/env bash
set -euo pipefail

airflow db init

# Create admin if missing
airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com || true

# Optionally store OWM key as Airflow Variable for tasks to read
if [ -n "${OWM_API_KEY:-}" ]; then
  airflow variables set OWM_API_KEY "${OWM_API_KEY}"
fi