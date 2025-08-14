import pandas as pd

def test_orders_customers_schema(db_connection):
    logger = logging.getLogger(__name__)

    expected_orders_cols = {"OrderID", "CustomerID", "OrderDate", "ShipCity"}
    expected_customers_cols = {"CustomerID", "CompanyName", "City", "Country"}

    orders = pd.read_sql("SELECT * FROM Orders", db_connection)
    customers = pd.read_sql("SELECT * FROM Customers", db_connection)

    # Schema validation
    assert expected_orders_cols.issubset(orders.columns), "Orders schema mismatch"
    assert expected_customers_cols.issubset(customers.columns), "Customers schema mismatch"

    logger.info("✅ Orders & Customers schema validated successfully.")

    # Data validation
    assert orders["CustomerID"].notna().all(), "Null CustomerID found in Orders"
    assert customers["City"].notna().all(), "Null City found in Customers"

    logger.info("✅ No null values found in key fields.")