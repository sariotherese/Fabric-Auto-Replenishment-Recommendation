# Block 4: dim_supplier
import pandas as pd
import numpy as np
from pathlib import Path

rng = np.random.default_rng(42)

RAW_DIR = Path("01_data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

def save(df, name):
    output_path = RAW_DIR / f"{name}.csv"
    df.to_csv(output_path, index=False)

    # Keep notebook behavior: write to lakehouse table when Spark is available.
    if "spark" in globals():
        spark.createDataFrame(df).write.mode("overwrite").format("delta").saveAsTable(name)

    print(f"✓ {name}: {len(df):,} rows -> {output_path}")

nsup = 15
dim_supplier = pd.DataFrame({
    "supplier_key": range(1, nsup+1),                      # PK
    "supplier_name": [f"Supplier {chr(64+i)}" for i in range(1, nsup+1)],
    "country": rng.choice(["Philippines","Thailand","Vietnam","China","Malaysia"], nsup, p=[.5,.15,.15,.1,.1]),
    "lead_time_days": rng.choice([1,2,3,4,5], nsup, p=[.25,.30,.25,.12,.08]).astype(int),
    "reliability_pct": np.round(np.clip(rng.normal(95, 4, nsup), 80, 100), 1),  # standard normal
})
save(dim_supplier, "dim_supplier")