import requests
import pandas as pd
import logging
import os
from typing import Dict

API_KEY = os.getenv("OPENWEATHER_API_KEY")
BASE_URL = "http://api.openweathermap.org/data/2.5/weather"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def fetch_weather_for_city(city: str, country_code: str) -> Dict:
    """Fetch weather data for a single city."""
    params = {"q": f"{city},{country_code}", "appid": API_KEY, "units": "metric"}
    resp = requests.get(BASE_URL, params=params)
    resp.raise_for_status()
    return resp.json()

def enrich_with_weather(customers_df: pd.DataFrame) -> pd.DataFrame:
    """Add weather columns to customers DataFrame."""
    logger.info("Fetching weather data for %d unique cities", customers_df['City'].nunique())
    weather_data = []
    for _, row in customers_df.drop_duplicates(subset=["City", "Country"]).iterrows():
        data = fetch_weather_for_city(row["City"], row["Country"])
        weather_data.append({
            "City": row["City"],
            "temp_celsius": data["main"]["temp"],
            "weather_desc": data["weather"][0]["description"]
        })
    weather_df = pd.DataFrame(weather_data)
    return customers_df.merge(weather_df, on="City", how="left")