import logging

def test_enriched_data_quality(sample_enriched_data, sample_region_mapping):
    logger = logging.getLogger(__name__)
    df = sample_enriched_data

    # 1. Schema validation
    expected_cols = {
        "CustomerID", "CompanyName", "City", "Country",
        "temperature", "weather_description", "Region"
    }
    assert expected_cols.issubset(df.columns), "Enriched data schema mismatch"

    # 2. Null checks
    key_fields = ["City", "temperature", "Region"]
    for field in key_fields:
        if df[field].isna().any():
            logger.warning(f"⚠ Missing values in {field}")
    
    # 3. Duplicate records
    if df.duplicated().any():
        logger.warning("⚠ Duplicate records found")
    
    # 4. Weather type validation
    for idx, row in df.iterrows():
        if not isinstance(row["temperature"], (int, float)):
            logger.warning(f"⚠ Invalid temperature at row {idx}")

    # 5. Region mapping check
    mapped_countries = set(sample_region_mapping["Country"])
    for country in df["Country"].unique():
        if country not in mapped_countries:
            logger.warning(f"⚠ Country {country} not found in region mapping")