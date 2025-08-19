import sqlite3
import pandas as pd
from config.config import NORTHWIND_DB

def extract_orders_customers():
    """Extract orders and customers from SQLite Northwind DB."""
    conn = sqlite3.connect(NORTHWIND_DB)
    customers_df = pd.read_sql("SELECT * FROM Customers", conn)
    orders_df = pd.read_sql("SELECT * FROM Orders", conn)
    conn.close()
    return customers_df, orders_df