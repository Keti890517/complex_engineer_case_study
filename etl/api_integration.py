import os
import time
import logging
import pandas as pd
import requests
import yaml
from unidecode import unidecode
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Paths
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
COUNTRY_MAPPING_PATH = os.path.join(BASE_DIR, "config", "country_code_mapping.yaml")
CITY_MAPPING_PATH = os.path.join(BASE_DIR, "config", "city_name_mapping.yaml")

# Load environment variables
load_dotenv()
API_KEY = os.getenv("OPENWEATHER_API_KEY")
if not API_KEY:
    raise EnvironmentError("OPENWEATHER_API_KEY not found in environment variables.")

# Load country mapping
if not os.path.exists(COUNTRY_MAPPING_PATH):
    raise FileNotFoundError(f"Country mapping file not found at {COUNTRY_MAPPING_PATH}")

with open(COUNTRY_MAPPING_PATH, "r") as f:
    country_mapping_raw = yaml.safe_load(f) or {}

# Normalize country keys
country_mapping = {k.strip().lower(): v for k, v in country_mapping_raw.items()}
logger.info(f"Loaded country codes for {len(country_mapping)} countries")

# Load city mapping
if not os.path.exists(CITY_MAPPING_PATH):
    logger.warning(f"City mapping file not found at {CITY_MAPPING_PATH}, proceeding without it.")
    city_name_mapping = {}
else:
    with open(CITY_MAPPING_PATH, "r") as f:
        city_mapping_yaml = yaml.safe_load(f) or {}
    # Keep keys lowercase for matching
    city_name_mapping = {k.strip().lower(): v for k, v in city_mapping_yaml.get("city_name_mapping", {}).items()}
    logger.info(f"Loaded city mappings for {len(city_name_mapping)} cities")

def get_country_code(country_name: str):
    """Return 2-letter country code from full country name, case-insensitive."""
    if not isinstance(country_name, str) or not country_name.strip():
        logger.warning(f"Invalid or missing country name: {country_name}")
        return None

    key = country_name.strip().lower()
    code = country_mapping.get(key)

    if not code:
        # Try ASCII normalization
        code = country_mapping.get(unidecode(key))
    if not code:
        logger.warning(f"Unknown country: '{country_name}' — please update country_code_mapping.yaml")
        logger.debug(f"Available keys: {list(country_mapping.keys())}")
    return code

def normalize_city_name(city: str) -> str:
    """Apply mapping first, then clean accents for unmapped cities."""
    if not city or not isinstance(city, str):
        return city
    city_key = city.strip().lower()
    # Respect mapping exactly if exists
    if city_key in city_name_mapping:
        return city_name_mapping[city_key]
    # Otherwise apply unidecode for API
    return unidecode(city.strip())

def fetch_weather(city: str, country_code: str):
    """Call OpenWeather API for a given city and country code."""
    if not city or not country_code:
        return None

    city_api = normalize_city_name(city)
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city_api},{country_code}&appid={API_KEY}&units=metric"
    try:
        resp = requests.get(url)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        logger.error(f"Error fetching weather for '{city}' ({country_code}): {e}")
        return None

def enrich_with_weather(customers_df: pd.DataFrame) -> pd.DataFrame:
    """Enrich customers_df with weather data from OpenWeather."""
    if customers_df is None or customers_df.empty:
        logger.warning("Input DataFrame is empty or None, skipping weather enrichment.")
        return customers_df

    if "City" not in customers_df.columns or "Country" not in customers_df.columns:
        raise ValueError("Input DataFrame must contain 'City' and 'Country' columns")

    # Trim whitespace from City and Country
    customers_df["City"] = customers_df["City"].astype(str).str.strip()
    customers_df["Country"] = customers_df["Country"].astype(str).str.strip()

    unique_cities = customers_df[["City", "Country"]].drop_duplicates()
    weather_data = []
    skipped_rows = []

    calls_made = 0
    for _, row in unique_cities.iterrows():
        city = row["City"]
        country_name = row["Country"]

        country_code = get_country_code(country_name)
        if not country_code or not city or not isinstance(city, str):
            skipped_rows.append((city, country_name))
            continue

        weather_json = fetch_weather(city, country_code)
        if weather_json:
            weather_data.append({
                "City": city,
                "Country": country_name,
                "Weather": weather_json.get("weather", [{}])[0].get("description"),
                "Temperature": weather_json.get("main", {}).get("temp")
            })

        calls_made += 1
        if calls_made % 60 == 0:
            logger.info("API rate limit reached — waiting 60 seconds...")
            time.sleep(60)

    if skipped_rows:
        logger.warning(f"Skipped {len(skipped_rows)} cities due to missing or invalid data: {skipped_rows}")

    if not weather_data:
        logger.warning("No weather data collected — check mappings and API responses.")
        weather_df = pd.DataFrame(columns=["City", "Country", "Weather", "Temperature"])
    else:
        weather_df = pd.DataFrame(weather_data)

    merged_df = customers_df.merge(weather_df, on=["City", "Country"], how="left")
    return merged_df