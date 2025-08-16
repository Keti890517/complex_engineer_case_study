from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator

# --- Make project code importable ---
import sys
DAGS_DIR = Path(__file__).resolve().parent
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))
if str(DAGS_DIR / "etl") not in sys.path:
    sys.path.insert(0, str(DAGS_DIR / "etl"))

# Project ETL modules
from etl.extract import extract_orders_customers
from etl.api_integration import enrich_with_weather
from etl.region_mapping import load_region_mapping
from etl.transform import enrich_with_region
from etl.load import load_to_db

ARTIFACTS = DAGS_DIR / "artifacts"
DATA_DIR = DAGS_DIR / "data"             # northwind.db + region_mapping.xlsx live here
TARGET_DB = DATA_DIR / "target.db"       # output DB

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# ------------------- ETL task functions -------------------
def _ensure_dirs():
    ARTIFACTS.mkdir(parents=True, exist_ok=True)

def _task_extract():
    customers_df, orders_df = extract_orders_customers()
    (ARTIFACTS / "customers.csv").write_text(customers_df.to_csv(index=False))
    (ARTIFACTS / "orders.csv").write_text(orders_df.to_csv(index=False))

def _weather_gate() -> bool:
    # Skip weather step if no API key
    return bool(os.getenv("OPENWEATHER_API_KEY"))

def _task_api():
    import pandas as pd
    customers = pd.read_csv(ARTIFACTS / "customers.csv")
    customers_weather = enrich_with_weather(customers)
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
    load_to_db(enriched, table_name="enriched_customers")

# ------------------- DAG definition -------------------
with DAG(
    dag_id="northwind_weather_etl",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    doc_md="""
    ETL: Northwind (SQLite) + OpenWeather + Region mapping (Excel).
    Data quality enforced via pytest tasks after key stages.
    """,
) as dag:

    ensure_dirs = PythonOperator(
        task_id="ensure_dirs",
        python_callable=_ensure_dirs,
    )

    # ETL stages
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

    # ------------------- Pytest DQ tasks -------------------
    # Only the existing test files are included
    run_extract_tests = BashOperator(
        task_id="test_extract",
        bash_command="export PYTHONPATH=/opt/airflow/dags:/opt/airflow/etl && pytest -v --tb=short tests/test_extract.py",
    )

    run_transform_tests = BashOperator(
        task_id="test_transform",
        bash_command="export PYTHONPATH=/opt/airflow/dags:/opt/airflow/etl && pytest -v --tb=short tests/test_transform.py",
    )

    run_load_tests = BashOperator(
        task_id="test_load",
        bash_command="export PYTHONPATH=/opt/airflow/dags:/opt/airflow/etl && pytest -v --tb=short tests/test_load.py",
    )

    # ------------------- DAG dependencies -------------------
    ensure_dirs >> extract >> run_extract_tests
    run_extract_tests >> region_map >> api_integration >> weather_key_present
    weather_key_present >> api_integration >> run_transform_tests
    run_transform_tests >> transform >> load >> run_load_tests