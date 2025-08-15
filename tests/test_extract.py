import pandas as pd
import logging

def test_orders_customers_schema(db_connection, sample_region_mapping):
    logger = logging.getLogger(__name__)

    orders = pd.read_sql("SELECT * FROM Orders", db_connection)
    customers = pd.read_sql("SELECT * FROM Customers", db_connection)

    # Schema validation
    expected_orders_cols = {"OrderID", "CustomerID", "OrderDate", "ShipCity"}
    expected_customers_cols = {"CustomerID", "CompanyName", "City", "Country"}
    assert expected_orders_cols.issubset(orders.columns), "Orders schema mismatch"
    assert expected_customers_cols.issubset(customers.columns), "Customers schema mismatch"
    logger.info("✅ Orders & Customers schema validated successfully.")

    # Null checks
    if customers["City"].isna().any():
        logger.warning("⚠ Null City found in Customers")
    if orders["CustomerID"].isna().any():
        logger.warning("⚠ Null CustomerID found in Orders")

    # Region mapping check
    mapped_countries = set(sample_region_mapping["Country"])
    for country in customers["Country"].unique():
        if country not in mapped_countries:
            logger.warning(f"⚠ Country {country} not found in region mapping")