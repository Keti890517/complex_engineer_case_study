from __future__ import annotations
from pathlib import Path
from typing import Dict, List
import pandas as pd
from config.config import OUTPUT_DIR

def _present(df: pd.DataFrame, cols: list[str]) -> str | None:
    """Finds the first matching column (case-insensitive)."""
    cols_lower = [c.lower() for c in df.columns]
    for c in cols:
        if c.lower() in cols_lower:
            return df.columns[cols_lower.index(c.lower())]  # preserve original
    return None

def check_source_schemas(customers: pd.DataFrame, orders: pd.DataFrame, mapping: pd.DataFrame) -> Dict:
    errors: List[str] = []
    warnings: List[str] = []

    # Normalize
    customers.columns = customers.columns.str.lower()
    orders.columns = orders.columns.str.lower()
    mapping.columns = mapping.columns.str.lower()

    # Customers schema
    cust_expected = {"customerid", "city", "country"}
    missing = cust_expected - set(customers.columns)
    for c in missing:
        errors.append(f"Customers missing required column: {c}")

    # Orders schema
    orders_expected_any = [{"orderid", "customerid"}, {"id", "customerid"}]
    if not any(candidate.issubset(set(orders.columns)) for candidate in orders_expected_any):
        errors.append("Orders missing required columns (need orderid/customerid or id/customerid).")

    # Region mapping
    if "country" not in mapping.columns:
        errors.append("Region mapping missing column: country")
    if not any(col.startswith("region") for col in mapping.columns):
        errors.append("Region mapping does not contain any 'region*' columns.")

    return {"stage": "sources", "errors": errors, "warnings": warnings}


def check_enriched_schema(enriched: pd.DataFrame) -> Dict:
    errors, warnings = [], []
    enriched.columns = enriched.columns.str.lower()

    if _present(enriched, ["region"]) is None:
        errors.append("Enriched dataset missing 'region' column.")
    if _present(enriched, ["city"]) is None:
        errors.append("Enriched dataset missing 'city' column.")
    if _present(enriched, ["temp_c", "temperature_c", "temperature", "temp"]) is None:
        errors.append("Enriched dataset missing temperature column.")

    return {"stage": "enriched_schema", "errors": errors, "warnings": warnings}

def check_enriched_data(df: pd.DataFrame, mapping_df: pd.DataFrame, log_null_rows: bool = False) -> dict:
    errors = []
    warnings = []

    key_cols = ["city", "region", "temperature"]
    for col in key_cols:
        if col in df.columns:
            n_null = df[col].isnull().sum()
            if n_null > 0:
                errors.append(f"Nulls found in {col}: {n_null}")
                if log_null_rows:
                    report_path = Path(OUTPUT_DIR) / "data_quality_report.log"
                    with open(report_path, "a") as f:
                        f.write(f"\n--- Rows with null {col} ---\n")
                        df[df[col].isnull()].to_csv(f, index=False)
    return {"stage": "enriched_data", "errors": errors, "warnings": warnings}


def write_report(reports: List[Dict], output_dir: Path = OUTPUT_DIR) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    log_file = output_dir / "data_quality_report.log"

    with open(log_file, "a") as f:   # <- use append mode
        for r in reports:
            f.write(f"== {r['stage']} ==\n")
            for e in r.get("errors", []):
                f.write(f"ERROR: {e}\n")
            for w in r.get("warnings", []):
                f.write(f"WARNING: {w}\n")
            f.write("\n")