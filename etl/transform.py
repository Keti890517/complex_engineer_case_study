import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def enrich_with_region(customers_df: pd.DataFrame, mapping_df: pd.DataFrame) -> pd.DataFrame:
    """Join customers with region mapping."""
    logger.info("Joining customers with region mapping")
    return customers_df.merge(mapping_df, on="Country", how="left")

def transform_data(customers_df, orders_df, mapping_df=None):
    """Main transformation entry point."""
    if mapping_df is not None:
        customers_df = enrich_with_region(customers_df, mapping_df)

    # Deduplicate enriched dataset
    customers_before = len(customers_df)
    customers_df = customers_df.drop_duplicates(subset=["CustomerID", "Country"])
    customers_after= len(customers_df)
    if customers_before != customers_after:
        logger.info(f"Dropped {customers_before - customers_after} duplicate customers based on CustomerID and Country")
    
    return customers_df, orders_df