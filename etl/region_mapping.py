import pandas as pd
import logging
from config.config import REGION_MAPPING_XLSX

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_region_mapping():
    """Load region mapping Excel file."""
    if not REGION_MAPPING_XLSX.exists():
        raise FileNotFoundError(f"Region mapping file not found: {REGION_MAPPING_XLSX}")
    logger.info("Loading region mapping from %s", REGION_MAPPING_XLSX)
    return pd.read_excel(REGION_MAPPING_XLSX)