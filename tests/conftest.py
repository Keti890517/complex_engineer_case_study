import logging
import pytest
from pathlib import Path
import pandas as pd
import sqlite3

# Configure shared logger for tests
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

@pytest.fixture
def db_connection():
    """Connection to Northwind DB (or temp DB for tests)."""
    db_path = Path(__file__).parent.parent / "data" / "northwind.db"
    conn = sqlite3.connect(db_path)
    yield conn
    conn.close()

@pytest.fixture
def sample_region_mapping():
    """Simulated region mapping for testing."""
    return pd.DataFrame({
        "Country": ["Germany", "Mexico", "USA", "Sweden"],
        "Region_2016": ["Europe", "Americas", "Americas", "Europe"],
        "Region_post_2016": ["Europe", "Americas", "Americas", "Europe"]
    })

@pytest.fixture
def sample_enriched_data():
    """Simulated enriched data including weather and region columns."""
    return pd.DataFrame([
        {"CustomerID": "ALFKI", "CompanyName": "Alfreds Futterkiste", "City": "Berlin",
         "Country": "Germany", "temperature": 22.5, "weather_description": "Clear", "Region": "Europe"},
        {"CustomerID": "ANATR", "CompanyName": "Ana Trujillo", "City": "México D.F.",
         "Country": "Mexico", "temperature": 28.0, "weather_description": "Clouds", "Region": "Americas"},
        {"CustomerID": "ANTON", "CompanyName": "Antonio Moreno", "City": "México D.F.",
         "Country": "Mexico", "temperature": 28.0, "weather_description": "Clouds", "Region": "Americas"},
        {"CustomerID": "BERGS", "CompanyName": "Berglunds snabbköp", "City": "Luleå",
         "Country": "Sweden", "temperature": 15.0, "weather_description": "Rain", "Region": "Europe"}
    ])