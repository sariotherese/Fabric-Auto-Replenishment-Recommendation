# Block 3: dim_category
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

cats = ["Beverages","Snacks","Household","Personal Care","Ready-to-Eat","Frozen"]
dim_category = pd.DataFrame({
    "category_key": range(1, len(cats)+1),                 # PK
    "category_name": cats,
    "department": ["Food & Drink","Food & Drink","Non-Food","Non-Food","Food & Drink","Food & Drink"],
    "is_perishable": [False,False,False,False,True,True],
})
save(dim_category, "dim_category")