import pandas as pd

def region_weather_summary(enriched: pd.DataFrame) -> pd.DataFrame:
    """
    Summary by region:
      - unique customers
      - avg/min/max temperature
    """
    df = enriched.copy()
    df.columns = [c.lower() for c in df.columns]  # normalize just in case

    grouped = df.groupby("region", dropna=False)

    summary = pd.DataFrame({
        "region": grouped.size().index,
        "customers": grouped["customerid"].nunique().values,
        "avg_temp_c": grouped["temperature"].mean().values,
        "min_temp_c": grouped["temperature"].min().values,
        "max_temp_c": grouped["temperature"].max().values,
    })

    return summary