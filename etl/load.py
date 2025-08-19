from pathlib import Path
import sqlite3
import pandas as pd
import logging

from config.config import TARGET_DB, STAGING_DIR

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_to_db(df: pd.DataFrame, table_name: str):
    """Load DataFrame to SQLite table."""
    logger.info("Loading %d rows into table %s", len(df), table_name)
    with sqlite3.connect(TARGET_DB) as conn:
        df.to_sql(table_name, conn, if_exists="replace", index=False)

def load_region_mapping(region_mapping_df: pd.DataFrame):
    """Load region mapping table separately."""
    if region_mapping_df is None or region_mapping_df.empty:
        logger.warning("Region mapping DataFrame is empty, skipping load.")
        return
    load_to_db(region_mapping_df, "region_mapping")

def load_customers(customers_df: pd.DataFrame):
    """Load enriched customers table."""
    if customers_df is None or customers_df.empty:
        logger.warning("Customers DataFrame is empty, skipping load.")
        return
    load_to_db(customers_df, "enriched_customers")

def load_to_csv(customers_df, output_dir: Path = STAGING_DIR):
    """Export enriched customers to CSV using centralized STAGING_DIR."""
    output_dir.mkdir(parents=True, exist_ok=True)
    customers_file = output_dir / "customers_enriched.csv"
    customers_df.to_csv(customers_file, index=False)
    logger.info("CSV file written to %s", customers_file)