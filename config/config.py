from pathlib import Path
import os

# ---------------- Project Root ----------------
PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).parent.parent))

# ---------------- Directories -----------------
DATA_DIR = Path(os.getenv("DATA_DIR", PROJECT_ROOT / "data"))
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", PROJECT_ROOT / "output"))
CONFIG_DIR = Path(os.getenv("CONFIG_DIR", PROJECT_ROOT / "config"))
STAGING_DIR = OUTPUT_DIR / "staging"

# Ensure output directories exist
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
STAGING_DIR.mkdir(parents=True, exist_ok=True)

# ---------------- Data/DB Files ---------------
NORTHWIND_DB = DATA_DIR / "northwind.db"
REGION_MAPPING_XLSX = DATA_DIR / "region_mapping.xlsx"
TARGET_DB = OUTPUT_DIR / "target.db"

# ---------------- Config Files -----------------
COUNTRY_MAPPING_YAML = CONFIG_DIR / "country_code_mapping.yaml"
CITY_MAPPING_YAML = CONFIG_DIR / "city_name_mapping.yaml"

# ---------------- ETL/Output Files -------------
ENRICHED_CSV = OUTPUT_DIR / "enriched.csv"
CUSTOMERS_CSV = STAGING_DIR / "customers.csv"
ORDERS_CSV = STAGING_DIR / "orders.csv"
CUSTOMERS_WEATHER_CSV = STAGING_DIR / "customers_weather.csv"
REGION_WEATHER_SUMMARY_CSV = OUTPUT_DIR / "region_weather_summary.csv"