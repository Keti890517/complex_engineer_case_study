import pandas as pd

def test_weather_data_matches_city():
    logger = logging.getLogger(__name__)
    
    weather_df = pd.read_csv("data/weather_data.csv")  # example output

    assert "city" in weather_df.columns, "Missing 'city' column in weather data"
    assert "temperature" in weather_df.columns, "Missing 'temperature' column"

    # No nulls in key fields
    assert weather_df["city"].notna().all(), "Null city in weather data"
    assert weather_df["temperature"].notna().all(), "Null temperature in weather data"

    logger.info("✅ Weather data schema and null checks passed.")

    # Example match validation (pseudo-check)
    valid_cities = {"London", "Paris", "Berlin"}  # from customers
    assert set(weather_df["city"]).issubset(valid_cities), "Weather data contains unexpected cities"

    logger.info("✅ Weather cities match expected customer cities.")