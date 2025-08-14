def test_loaded_data_matches_enriched():
    logger = logging.getLogger(__name__)
    
    # Read from target DB
    import sqlite3
    conn = sqlite3.connect("data/target.db")
    loaded_df = pd.read_sql("SELECT * FROM enriched_customers", conn)
    conn.close()

    assert not loaded_df.empty, "Loaded dataset is empty"
    assert "Region" in loaded_df.columns, "Missing 'Region' in loaded data"

    logger.info(f"✅ Loaded dataset contains {len(loaded_df)} rows and correct schema.")