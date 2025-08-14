def test_enriched_data_quality():
    logger = logging.getLogger(__name__)
    
    enriched_df = pd.read_csv("data/enriched_data.csv")

    expected_cols = {
        "CustomerID", "CompanyName", "City", "Country",
        "temperature", "weather_description", "Region"
    }
    assert expected_cols.issubset(enriched_df.columns), "Enriched data schema mismatch"

    # No missing key fields
    key_fields = ["City", "temperature", "Region"]
    for field in key_fields:
        assert enriched_df[field].notna().all(), f"Missing values in {field}"

    logger.info("✅ Enriched dataset key fields are complete.")

    # No duplicates
    assert not enriched_df.duplicated().any(), "Duplicate records found in enriched data"

    logger.info("✅ No duplicates in enriched dataset.")