import sqlite3
import pandas as pd
from pathlib import Path
import logging

TARGET_DB_PATH = Path(__file__).parent.parent / "data" / "target.db"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def load_to_db(df: pd.DataFrame, table_name: str):
    """Load DataFrame to SQLite table."""
    logger.info("Loading %d rows into table %s", len(df), table_name)
    conn = sqlite3.connect(TARGET_DB_PATH)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()