from pathlib import Path
import sqlite3
import pandas as pd
import logging

DB_PATH = Path(__file__).parent.parent / "data" / "northwind.db"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def extract_orders_customers():
    """Extract Orders and Customers from SQLite."""
    logger.info("Extracting Orders and Customers from %s", DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    customers_df = pd.read_sql("SELECT * FROM Customers", conn)
    orders_df = pd.read_sql("SELECT * FROM Orders", conn)
    conn.close()
    logger.info("Extracted %d customers and %d orders", len(customers_df), len(orders_df))
    return customers_df, orders_df