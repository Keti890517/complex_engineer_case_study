import pandas as pd
from pathlib import Path
import logging

MAPPING_PATH = Path(__file__).parent.parent / "data" / "region_mapping.xlsx"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def load_region_mapping():
    """Load region mapping Excel file."""
    logger.info("Loading region mapping from %s", MAPPING_PATH)
    return pd.read_excel(MAPPING_PATH)