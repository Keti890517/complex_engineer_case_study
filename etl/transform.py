import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def enrich_with_region(customers_df: pd.DataFrame, mapping_df: pd.DataFrame) -> pd.DataFrame:
    """Join customers with region mapping."""
    logger.info("Joining customers with region mapping")
    enriched = customers_df.merge(mapping_df, on="Country", how="left")
    return enriched