from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator

from dotenv import load_dotenv
load_dotenv()

# --- Make project code importable ---
DAGS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = DAGS_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# --- Project ETL modules ---
from etl.extract import extract_orders_customers
from etl.api_integration import enrich_with_weather
from etl.region_mapping import load_region_mapping
from etl.transform import enrich_with_region
from etl.load import load_to_db
from etl.analysis import region_weather_summary
from config.config import (
    DATA_DIR,
    OUTPUT_DIR,
    STAGING_DIR,
    TARGET_DB,
    NORTHWIND_DB,
    REGION_MAPPING_XLSX,
    ENRICHED_CSV,
    CUSTOMERS_CSV,
    ORDERS_CSV,
    CUSTOMERS_WEATHER_CSV,
    REGION_WEATHER_SUMMARY_CSV
)

# ------------------- DAG constants -------------------
STAGING = STAGING_DIR        # intermediate/temp
TESTS_DIR = PROJECT_ROOT / "tests"  # dynamic test path

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# ------------------- ETL task functions -------------------
def _ensure_dirs():
    STAGING.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def _task_extract():
    customers_df, orders_df = extract_orders_customers()
    customers_df.to_csv(CUSTOMERS_CSV, index=False)
    orders_df.to_csv(ORDERS_CSV, index=False)

def _weather_gate() -> bool:
    return bool(os.getenv("OPENWEATHER_API_KEY"))

def _task_api():
    import pandas as pd
    customers = pd.read_csv(CUSTOMERS_CSV)
    api_key = os.getenv("OPENWEATHER_API_KEY")

    if not api_key:
        import logging
        logging.warning("OPENWEATHER_API_KEY not set, skipping API enrichment")
        customers.to_csv(CUSTOMERS_WEATHER_CSV, index=False)
        return

    customers_weather = enrich_with_weather(customers, api_key=api_key)
    customers_weather.to_csv(CUSTOMERS_WEATHER_CSV, index=False)

def _task_region_mapping():
    mapping_df = load_region_mapping()
    mapping_df.to_csv(STAGING_DIR / "region_mapping.csv", index=False)
    load_to_db(mapping_df, table_name="region_mapping")

def _task_transform():
    import pandas as pd
    customers_weather = pd.read_csv(CUSTOMERS_WEATHER_CSV)
    mapping_df = pd.read_csv(STAGING_DIR / "region_mapping.csv")
    enriched = enrich_with_region(customers_weather, mapping_df)
    enriched.to_csv(ENRICHED_CSV, index=False)

def _task_load():
    import pandas as pd
    enriched = pd.read_csv(ENRICHED_CSV)
    load_to_db(enriched, table_name="enriched_customers")

def _task_region_analysis():
    import pandas as pd
    enriched = pd.read_csv(ENRICHED_CSV)
    summary = region_weather_summary(enriched)
    summary.to_csv(REGION_WEATHER_SUMMARY_CSV, index=False)

# ------------------- DAG definition -------------------
with DAG(
    dag_id="northwind_weather_etl",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    doc_md="""ETL: Northwind (SQLite) + OpenWeather + Region mapping (Excel).""",
) as dag:

    ensure_dirs = PythonOperator(
        task_id="ensure_dirs",
        python_callable=_ensure_dirs,
    )

    extract = PythonOperator(
        task_id="extract",
        python_callable=_task_extract,
    )

    run_extract_tests = BashOperator(
        task_id="test_extract",
        bash_command=(
            f"export PYTHONPATH={PROJECT_ROOT}/dags:{PROJECT_ROOT}/etl && "
            f"pytest -v -s --tb=short {TESTS_DIR}/test_extract.py --cache-clear "
            f"| tee {OUTPUT_DIR}/data_quality_report.log"
        ),
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

    run_transform_tests = BashOperator(
        task_id="test_transform",
        bash_command=(
            f"export PYTHONPATH={PROJECT_ROOT}/dags:{PROJECT_ROOT}/etl && "
            f"pytest -v -s --tb=short {TESTS_DIR}/test_transform.py --cache-clear "
            f"| tee {OUTPUT_DIR}/data_quality_report.log"
        ),
    )

    analysis = PythonOperator(
        task_id="region_weather_analysis",
        python_callable=_task_region_analysis,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=_task_load,
    )

    run_load_tests = BashOperator(
        task_id="test_load",
        bash_command=(
            f"export PYTHONPATH={PROJECT_ROOT}/dags:{PROJECT_ROOT}/etl && "
            f"pytest -v -s --tb=short {TESTS_DIR}/test_load.py --cache-clear "
            f"| tee {OUTPUT_DIR}/data_quality_report.log"
        ),
    )

    run_data_quality = BashOperator(
        task_id="data_quality_summary",
        bash_command=(
            f"export PYTHONPATH={PROJECT_ROOT}/dags:{PROJECT_ROOT}/etl && "
            f"pytest -v -s --tb=short -q {TESTS_DIR} --cache-clear "
            f"| tee {OUTPUT_DIR}/data_quality_report.log"
        ),
)

    # ---------------- Dependencies ----------------
    ensure_dirs >> extract >> run_extract_tests
    run_extract_tests >> region_map
    run_extract_tests >> weather_key_present >> api_integration
    [region_map, api_integration] >> transform >> run_transform_tests
    run_transform_tests >> analysis
    run_transform_tests >> load >> run_load_tests
    run_load_tests >> run_data_quality