# Block 8: fact_sales  — 30,000 rows, full referential integrity
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

required_files = {
    "dim_date": RAW_DIR / "dim_date.csv",
    "dim_store": RAW_DIR / "dim_store.csv",
    "dim_product": RAW_DIR / "dim_product.csv",
    "dim_channel": RAW_DIR / "dim_channel.csv",
    "dim_promotion": RAW_DIR / "dim_promotion.csv",
}

missing_files = [str(path) for path in required_files.values() if not path.exists()]
if missing_files:
    raise FileNotFoundError(
        "Missing dependencies. Generate required dimension files first in 01_data/raw: "
        + ", ".join(missing_files)
    )

dim_date = pd.read_csv(required_files["dim_date"])
dim_store = pd.read_csv(required_files["dim_store"])
dim_product = pd.read_csv(required_files["dim_product"])
dim_channel = pd.read_csv(required_files["dim_channel"])
dim_promotion = pd.read_csv(required_files["dim_promotion"])

N = 30_000

# sample ROW INDICES into each dimension, then map to valid keys (guarantees no orphans)
di = rng.integers(0, len(dim_date),      N)
si = rng.integers(0, len(dim_store),     N)
pi = rng.integers(0, len(dim_product),   N)
ci = rng.integers(0, len(dim_channel),   N)
mi = rng.choice(len(dim_promotion), N, p=[.6,.1,.1,.1,.1])   # mostly "No Promotion"

# FKs — category_key & supplier_key come FROM the chosen product (kept consistent)
date_key      = dim_date.date_key.values[di]
store_key     = dim_store.store_key.values[si]
product_key   = dim_product.product_key.values[pi]
category_key  = dim_product.category_key.values[pi]          # consistent with product
supplier_key  = dim_product.supplier_key.values[pi]          # consistent with product
channel_key   = dim_channel.channel_key.values[ci]
promotion_key = dim_promotion.promotion_key.values[mi]

# demand drivers
tier      = dim_store.traffic_tier.values[si]
tier_mult = np.select([tier==1,tier==2,tier==3], [0.7,1.0,1.6])
wknd_mult = np.where(dim_date.is_weekend.values[di], 1.25, 1.0)
base_dem  = dim_product.base_demand.values[pi]
mean_u    = base_dem * tier_mult * wknd_mult

# MEASURES — normal/Gaussian distribution, truncated to be non-negative
units_sold   = np.clip(np.round(rng.normal(mean_u, mean_u*0.30)), 1, None).astype(int)
unit_price   = np.round(dim_product.base_price.values[pi] * rng.normal(1.0, 0.03, N), 2)  # std-normal noise
discount     = dim_promotion.discount_pct.values[mi]
sales_amount = np.round(units_sold * unit_price * (1 - discount), 2)
cost_amount  = np.round(units_sold * dim_product.unit_cost.values[pi], 2)
on_hand_qty  = np.clip(np.round(rng.normal(mean_u*5, mean_u*2)), 0, None).astype(int)  # snapshot stock

fact_sales = pd.DataFrame({
    "sales_key":     np.arange(1, N+1),     # PK
    "date_key":      date_key,              # FK ➜ dim_date
    "store_key":     store_key,             # FK ➜ dim_store
    "product_key":   product_key,           # FK ➜ dim_product
    "category_key":  category_key,          # FK ➜ dim_category
    "supplier_key":  supplier_key,          # FK ➜ dim_supplier
    "promotion_key": promotion_key,         # FK ➜ dim_promotion
    "channel_key":   channel_key,           # FK ➜ dim_channel
    "units_sold":    units_sold,
    "unit_price":    unit_price,
    "sales_amount":  sales_amount,
    "cost_amount":   cost_amount,
    "on_hand_qty":   on_hand_qty,
})
save(fact_sales, "fact_sales")