import pytest
import pandas as pd
from pathlib import Path
import sqlite3

@pytest.fixture
def sample_customers():
    # Minimal customers DataFrame for testing
    return pd.DataFrame({
        "CustomerID": ["C001", "C002"],
        "CompanyName": ["Company A", "Company B"],
        "City": ["Berlin", "Mexico City"],
        "Country": ["Germany", "Mexico"]
    })

@pytest.fixture
def sample_orders():
    return pd.DataFrame({
        "OrderID": [1, 2],
        "CustomerID": ["C001", "C002"],
        "OrderDate": ["2025-01-01", "2025-01-02"],
        "ShipCity": ["Berlin", "Mexico City"]
    })

@pytest.fixture
def sample_enriched_data():
    return pd.DataFrame({
        "CustomerID": ["C001", "C002"],
        "CompanyName": ["Company A", "Company B"],
        "City": ["Berlin", "Mexico City"],
        "Country": ["Germany", "Mexico"],
        "temperature": [20, 25],
        "weather_description": ["clear sky", "sunny"],
        "Region": ["Europe", "Americas"]
    })

@pytest.fixture
def temp_db(tmp_path, sample_customers, sample_orders):
    """Create a temporary SQLite DB with Customers and Orders tables."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)

    sample_customers.to_sql("Customers", conn, index=False)
    sample_orders.to_sql("Orders", conn, index=False)

    yield conn
    conn.close()

@pytest.fixture
def sample_region_mapping():
    return pd.DataFrame({
        "Country": ["Germany", "Mexico", "USA"],
        "Region_2016": ["Europe", "Americas", "Americas"],
        "Region_post_2016": ["Europe", "Americas", "Americas"]
    })