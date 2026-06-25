# Block 6: dim_promotion
import pandas as pd
from pathlib import Path

RAW_DIR = Path("01_data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

def save(df, name):
    output_path = RAW_DIR / f"{name}.csv"
    df.to_csv(output_path, index=False)

    # Keep notebook behavior: write to lakehouse table when Spark is available.
    if "spark" in globals():
        spark.createDataFrame(df).write.mode("overwrite").format("delta").saveAsTable(name)

    print(f"✓ {name}: {len(df):,} rows -> {output_path}")

dim_promotion = pd.DataFrame({
    "promotion_key": [0,1,2,3,4],                          # PK (0 = none)
    "promo_name":    ["No Promotion","Price Off","Bundle Deal","Weekend Special","Payday Sale"],
    "promo_type":    ["None","Markdown","Bundle","Seasonal","Seasonal"],
    "discount_pct":  [0.00, 0.10, 0.15, 0.12, 0.20],
})
save(dim_promotion, "dim_promotion")