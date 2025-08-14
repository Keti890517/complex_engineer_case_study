from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator

# --- Make project code importable from /opt/airflow/dags ---
import sys
DAGS_DIR = Path(__file__).resolve().parent
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

# Project modules (kept short & focused)
from etl.extract import extract_orders_customers
from etl.api_integration import enrich_with_weather
from etl.region_mapping import load_region_mapping
from etl.transform import enrich_with_region
from etl.load import load_to_db

ARTIFACTS = DAGS_DIR / "artifacts"
DATA_DIR = DAGS_DIR / "data"             # where northwind.db + region_mapping.xlsx live (mounted)
TARGET_DB = DATA_DIR / "target.db"       # output DB

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

def _ensure_dirs():
    ARTIFACTS.mkdir(parents=True, exist_ok=True)

def _task_extract():
    # Extract & persist lightweight artifacts for downstream tasks/tests
    customers_df, orders_df = extract_orders_customers()
    (ARTIFACTS / "customers.csv").write_text(customers_df.to_csv(index=False))
    (ARTIFACTS / "orders.csv").write_text(orders_df.to_csv(index=False))

def _weather_gate() -> bool:
    # Skip weather steps until the API key is configured
    return bool(os.getenv("OPENWEATHER_API_KEY"))

def _task_api():
    import pandas as pd
    customers = pd.read_csv(ARTIFACTS / "customers.csv")
    customers_weather = enrich_with_weather(customers)  # requires OPENWEATHER_API_KEY
    (ARTIFACTS / "customers_weather.csv").write_text(customers_weather.to_csv(index=False))

def _task_region_mapping():
    mapping_df = load_region_mapping()
    (ARTIFACTS / "region_mapping.csv").write_text(mapping_df.to_csv(index=False))

def _task_transform():
    import pandas as pd
    customers_weather = pd.read_csv(ARTIFACTS / "customers_weather.csv")
    mapping_df = pd.read_csv(ARTIFACTS / "region_mapping.csv")
    enriched = enrich_with_region(customers_weather, mapping_df)
    (ARTIFACTS / "enriched_customers.csv").write_text(enriched.to_csv(index=False))

def _task_load():
    import pandas as pd
    enriched = pd.read_csv(ARTIFACTS / "enriched_customers.csv")
    load_to_db(enriched, table_name="enriched_customers")  # writes to data/target.db


with DAG(
    dag_id="northwind_weather_etl",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,     # enable a cron later if you want
    catchup=False,
    default_args=DEFAULT_ARGS,
    doc_md="""
    ETL: Northwind (SQLite) + OpenWeather + Region mapping (Excel).
    DQ is enforced via pytest tasks that run after each stage.
    """,
) as dag:

    ensure_dirs = PythonOperator(
        task_id="ensure_dirs",
        python_callable=_ensure_dirs,
    )

    # ---------- ETL stages ----------
    extract = PythonOperator(
        task_id="extract",
        python_callable=_task_extract,
    )

    weather_key_present = ShortCircuitOperator(
        task_id="weather_key_present",
        python_callable=_weather_gate,
    )

    api_integration = PythonOperator(
        task_id="api_integration",
        python_callable=_task_api,
    )

    region_map = PythonOperator(
        task_id="region_mapping",
        python_callable=_task_region_mapping,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=_task_transform,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=_task_load,
    )

    # ---------- Pytest DQ tasks ----------
    # We run the specific test file(s) after each stage.
    # PYTHONPATH makes `etl/` importable; tests log to Airflow task log.
    run_extract_tests = BashOperator(
        task_id="test_extract",
        bash_command="export PYTHONPATH=/opt/airflow/dags && pytest -v --tb=short tests/test_extract.py",
    )

    run_region_tests = BashOperator(
        task_id="test_region_mapping",
        bash_command="export PYTHONPATH=/opt/airflow/dags && pytest -v --tb=short tests/test_region_mapping.py",
    )

    run_api_tests = BashOperator(
        task_id="test_api_integration",
        bash_command="export PYTHONPATH=/opt/airflow/dags && pytest -v --tb=short tests/test_api_integration.py",
    )

    run_transform_tests = BashOperator(
        task_id="test_transform",
        bash_command="export PYTHONPATH=/opt/airflow/dags && pytest -v --tb=short tests/test_transform.py",
    )

    run_load_tests = BashOperator(
        task_id="test_load",
        bash_command="export PYTHONPATH=/opt/airflow/dags && pytest -v --tb=short tests/test_load.py",
    )

    # ---------- Dependencies ----------
    ensure_dirs >> extract >> run_extract_tests

    # Region mapping can run right after extraction (independent of weather)
    run_extract_tests >> region_map >> run_region_tests

    # Weather path (skippable if no API key yet)
    run_extract_tests >> weather_key_present >> api_integration >> run_api_tests

    # Transform needs both mapping + api
    [run_region_tests, run_api_tests] >> transform >> run_transform_tests >> load >> run_load_tests