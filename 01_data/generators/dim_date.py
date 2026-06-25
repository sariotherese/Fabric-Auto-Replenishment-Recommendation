# 01_Data_Generator — Block 1: dim_date  (run cells in order; attach LH_Retail)
import pandas as pd
import numpy as np
from datetime import date, timedelta
from pathlib import Path

rng = np.random.default_rng(42)          # reproducible across all blocks

RAW_DIR = Path("01_data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

def save(df, name):                      # helper reused by every block
    output_path = RAW_DIR / f"{name}.csv"
    df.to_csv(output_path, index=False)

    # Keep notebook behavior: write to lakehouse table when Spark is available.
    if "spark" in globals():
        spark.createDataFrame(df).write.mode("overwrite").format("delta").saveAsTable(name)

    print(f"✓ {name}: {len(df):,} rows -> {output_path}")

start, ndays = date(2025, 1, 1), 540     # ~18 months of calendar
rows = []
for i in range(ndays):
    d = start + timedelta(days=i)
    rows.append({
        "date_key":   int(d.strftime("%Y%m%d")),   # PK (yyyymmdd)
        "full_date":  d.isoformat(),
        "day":        d.day,
        "month":      d.month,
        "month_name": d.strftime("%B"),
        "quarter":    (d.month - 1)//3 + 1,
        "year":       d.year,
        "day_name":   d.strftime("%A"),
        "is_weekend": d.weekday() >= 5,
    })
dim_date = pd.DataFrame(rows)
save(dim_date, "dim_date")