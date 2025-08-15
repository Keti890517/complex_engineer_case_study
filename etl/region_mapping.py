import pandas as pd
from pathlib import Path
import logging

MAPPING_PATH = Path(__file__).parent.parent / "data" / "region_mapping.xlsx"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_region_mapping():
    """Load region mapping Excel file."""
    if not MAPPING_PATH.exists():
        raise FileNotFoundError(f"Region mapping file not found: {MAPPING_PATH}")
    logger.info("Loading region mapping from %s", MAPPING_PATH)
    return pd.read_excel(MAPPING_PATH)