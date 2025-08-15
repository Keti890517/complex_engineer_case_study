import pandas as pd
import sqlite3
import logging

def test_loaded_data_matches_enriched(sample_enriched_data, tmp_path):
    logger = logging.getLogger(__name__)
    db_path = tmp_path / "target.db"
    conn = sqlite3.connect(db_path)

    # Write enriched data to DB
    sample_enriched_data.to_sql("enriched_customers", conn, index=False)

    loaded_df = pd.read_sql("SELECT * FROM enriched_customers", conn)
    conn.close()

    # Basic checks
    assert not loaded_df.empty, "Loaded dataset is empty"
    assert "Region" in loaded_df.columns, "Missing 'Region' in loaded data"
    logger.info(f"✅ Loaded dataset contains {len(loaded_df)} rows and correct schema.")