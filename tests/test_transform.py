import pandas as pd
from tests import dq
from config.config import OUTPUT_DIR, ENRICHED_CSV, REGION_MAPPING_XLSX

# Ensure output dir exists
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def test_enriched_data_quality():
    # Load enriched data
    df = pd.read_csv(ENRICHED_CSV)
    df.columns = df.columns.str.lower()

    # Load region mapping from Excel
    mapping = pd.read_excel(REGION_MAPPING_XLSX)
    mapping.columns = mapping.columns.str.lower()

    # Run DQ checks (will log nulls internally)
    report_schema = dq.check_enriched_schema(df)
    report_data = dq.check_enriched_data(df, mapping, log_null_rows=True)

    # Ensure 'stage' key exists
    report_schema.setdefault("stage", "schema_check")
    report_data.setdefault("stage", "data_check")

    # Write DQ report
    dq.write_report([report_schema, report_data], OUTPUT_DIR)

    # Assertions: ignore null/missing rows, only fail for schema issues
    assert not report_schema["errors"], f"Schema errors: {report_schema['errors']}"