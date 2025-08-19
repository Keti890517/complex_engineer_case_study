import pandas as pd
import sqlite3
from tests import dq
from config.config import OUTPUT_DIR, TARGET_DB, ENRICHED_CSV

# Ensure output dir exists
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def test_tables_exist_in_target_db():
    """Check target.db exists and contains required tables."""
    assert TARGET_DB.exists(), "target.db not created"

    conn = sqlite3.connect(TARGET_DB)
    try:
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cur.fetchall()}
    finally:
        conn.close()

    assert "region_mapping" in tables, "region_mapping table missing in target.db"
    assert "enriched_customers" in tables, "enriched_customers table missing in target.db"

def test_loaded_data_quality():
    # Load enriched data
    loaded_df = pd.read_csv(ENRICHED_CSV)
    loaded_df.columns = loaded_df.columns.str.lower()

    # Schema check
    report_schema = dq.check_enriched_schema(loaded_df)
    report_schema["stage"] = "load_schema"

    # Data check (this will log nulls internally)
    report_data = dq.check_enriched_data(loaded_df, mapping_df=pd.DataFrame(), log_null_rows=True)
    report_data["stage"] = "load_data"

    # Write report
    dq.write_report([report_schema, report_data], OUTPUT_DIR)

    # Assertions only for schema issues
    assert not report_schema["errors"], f"Schema errors: {report_schema['errors']}"