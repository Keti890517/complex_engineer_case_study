from etl.extract import extract_orders_customers
from etl.region_mapping import load_region_mapping
from tests import dq
from config.config import OUTPUT_DIR

# Ensure output dir exists
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def test_source_schemas_and_log_report():
    customers_df, orders_df = extract_orders_customers()
    mapping_df = load_region_mapping()

    # lowercase for consistency
    customers_df.columns = customers_df.columns.str.lower()
    orders_df.columns = orders_df.columns.str.lower()
    mapping_df.columns = mapping_df.columns.str.lower()

    report = dq.check_source_schemas(customers_df, orders_df, mapping_df)
    dq.write_report([report], OUTPUT_DIR)

    assert not report["errors"], f"Source schema errors: {report['errors']}"