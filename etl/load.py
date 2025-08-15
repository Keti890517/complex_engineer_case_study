from pathlib import Path
import sqlite3
import pandas as pd
import logging
import os

TARGET_DB_PATH = Path(__file__).parent.parent / "data" / "target.db"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_to_db(df: pd.DataFrame, table_name: str):
    """Load DataFrame to SQLite table."""
    logger.info("Loading %d rows into table %s", len(df), table_name)
    with sqlite3.connect(TARGET_DB_PATH) as conn:
        df.to_sql(table_name, conn, if_exists="replace", index=False)

def load_to_csv(customers_df, orders_df, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    customers_file = os.path.join(output_dir, "customers_full.csv")
    orders_file = os.path.join(output_dir, "orders_full.csv")
    customers_df.to_csv(customers_file, index=False)
    orders_df.to_csv(orders_file, index=False)
    logger.info("CSV files written to %s", output_dir)