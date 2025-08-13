from __future__ import annotations
from datetime import datetime
import os
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

# ---- Config (adjust as needed)
DATA_DIR = "/opt/airflow/data"
NORTHWIND_DB = os.path.join(DATA_DIR, "Northwind_large.sqlite")
REGION_XLSX = os.path.join(DATA_DIR, "region_mapping.xlsx")
TARGET_DUCKDB = os.path.join(DATA_DIR, "warehouse.duckdb")
OWM_API_KEY = Variable.get("OWM_API_KEY", default_var=os.getenv("OWM_API_KEY", ""))

default_args = {
    "owner": "you",
    "retries": 1
}

with DAG(
    dag_id="northwind_weather_etl",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # manual for now
    catchup=False,
    default_args=default_args,
    tags=["case-study"]
) as dag:

    @task
    def check_setup():
        missing = []
        for p in [DATA_DIR, NORTHWIND_DB, REGION_XLSX]:
            if not os.path.exists(p):
                missing.append(p)
        if missing:
            raise FileNotFoundError(f"Missing required files/dirs: {missing}")
        if not OWM_API_KEY:
            # Not fatal if you want to develop extraction first
            return {"owm_key_present": False}
        return {"owm_key_present": True}

    @task
    def extract_orders_customers():
        # TODO: implement with pandas + sqlite (sqlalchemy)
        # returns serialized data artifacts paths or lightweight summaries
        return {
            "orders_parquet": os.path.join(DATA_DIR, "staging_orders.parquet"),
            "customers_parquet": os.path.join(DATA_DIR, "staging_customers.parquet")
        }

    @task
    def fetch_weather(meta: dict, setup_meta: dict):
        # TODO: implement API calls (requests/tenacity), per unique city
        # Respect setup_meta["owm_key_present"]
        return os.path.join(DATA_DIR, "staging_weather.parquet")

    @task
    def load_region_mapping():
        # TODO: read Excel -> write to duckdb (or parquet temp)
        return os.path.join(DATA_DIR, "staging_region_map.parquet")

    @task
    def dq_checks(paths: dict):
        # TODO: implement schema and data checks (GE or pandas asserts)
        return {"dq_passed": True}

    @task
    def enrich_join(paths: dict):
        # TODO: join customers + weather + region, persist to duckdb
        return os.path.join(DATA_DIR, "enriched_customers.parquet")

    @task
    def load_target(enriched_path: str):
        # TODO: load enriched data into target (DuckDB/SQLite/Postgres)
        return {"loaded": True, "target": TARGET_DUCKDB}

    setup_meta = check_setup()
    extracted = extract_orders_customers()
    weather_path = fetch_weather(extracted, setup_meta)
    region_path = load_region_mapping()

    # Hand a small bundle into DQ / join
    bundle = {"extracted": extracted, "weather": weather_path, "region": region_path}
    dq = dq_checks(bundle)
    enriched = enrich_join(bundle)
    done = load_target(enriched)