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
    db_path = Path(__file__).parent.parent / "data" / "northwind.db"
    conn = sqlite3.connect(db_path)
    yield conn
    conn.close()

@pytest.fixture
def sample_region_mapping():
    mapping_path = Path(__file__).parent.parent / "data" / "region_mapping.xlsx"
    return pd.read_excel(mapping_path)