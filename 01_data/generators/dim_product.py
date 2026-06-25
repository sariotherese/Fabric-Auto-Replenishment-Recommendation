# Block 5: dim_product  — FKs into dim_category & dim_supplier (snowflake RI)
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

dim_category_path = RAW_DIR / "dim_category.csv"
dim_supplier_path = RAW_DIR / "dim_supplier.csv"

if not dim_category_path.exists() or not dim_supplier_path.exists():
    raise FileNotFoundError(
        "Missing dependencies. Run dim_category.py and dim_supply.py first to create files in 01_data/raw."
    )

dim_category = pd.read_csv(dim_category_path)
dim_supplier = pd.read_csv(dim_supplier_path)

NP = 200
cat_keys = dim_category.category_key.values
sup_keys = dim_supplier.supplier_key.values
dim_product = pd.DataFrame({
    "product_key":  range(1, NP+1),                        # PK
    "product_name": [f"SKU-{i:04d}" for i in range(1, NP+1)],
    "category_key": rng.choice(cat_keys, NP),              # FK ➜ dim_category
    "supplier_key": rng.choice(sup_keys, NP),              # FK ➜ dim_supplier
    "unit_cost":    np.round(np.clip(rng.normal(45, 20, NP), 5, None), 2),   # normal dist
    "case_pack":    rng.choice([6,12,24], NP, p=[.3,.45,.25]).astype(int),
    "base_demand":  np.round(np.clip(rng.normal(12, 5, NP), 1, None), 1),    # avg units/day (tier-2)
})
dim_product["base_price"] = np.round(dim_product["unit_cost"] * rng.normal(1.45, 0.05, NP), 2)  # margin
# denormalize lead_time onto product so the replenishment engine can join easily
dim_product = dim_product.merge(dim_supplier[["supplier_key","lead_time_days"]], on="supplier_key", how="left")
save(dim_product, "dim_product")