# Block 9: fact_inventory_snapshot — periodic snapshot fact (store x product x day)
#          Full referential integrity + normal (Gaussian) distribution.
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

SNAPSHOT_DAYS = 7        # daily stock snapshots for the most recent N days (configurable)
Z = 1.65                 # 95% service level for safety stock

# most-recent N snapshot dates pulled from the date dimension (RI ➜ dim_date)
snap_dates = dim_date.sort_values("date_key").tail(SNAPSHOT_DAYS).reset_index(drop=True)

ns, npd, nday = len(dim_store), len(dim_product), len(snap_dates)
N = ns * npd * nday      # FULL grain: every store x product x day (stockouts never disappear)

# build the complete cartesian grid of row indices
store_idx = np.repeat(np.arange(ns), npd * nday)
prod_idx  = np.tile(np.repeat(np.arange(npd), nday), ns)
date_idx  = np.tile(np.arange(nday), ns * npd)

# FKs — every key sourced from a valid dimension (zero orphans); supplier inherited from product
store_key        = dim_store.store_key.values[store_idx]
traffic          = dim_store.traffic_tier.values[store_idx]
product_key      = dim_product.product_key.values[prod_idx]
supplier_key     = dim_product.supplier_key.values[prod_idx]      # consistent with product
base_demand      = dim_product.base_demand.values[prod_idx]
lead_time        = dim_product.lead_time_days.values[prod_idx]
date_key         = snap_dates.date_key.values[date_idx]

# average daily demand scaled by store foot-traffic tier
tier_mult        = np.select([traffic==1, traffic==2, traffic==3], [0.7, 1.0, 1.6])
avg_daily_demand = base_demand * tier_mult

# planning parameters (reorder point = demand over lead time + safety stock)
demand_std    = avg_daily_demand * 0.30                            # assume CV ~0.30
safety_stock  = np.ceil(Z * demand_std * np.sqrt(lead_time))
reorder_point = np.ceil(avg_daily_demand * lead_time + safety_stock).astype(int)

# MEASURES — on-hand drawn from a NORMAL distribution centred above the reorder point,
# with enough spread to produce realistic stockouts (below ROP) and overstocks (well above)
on_hand_qty = np.clip(np.round(
                rng.normal(loc=reorder_point * 1.5, scale=np.maximum(reorder_point, 1) * 0.8)
              ), 0, None).astype(int)
units_in_transit = np.clip(np.round(
                rng.normal(loc=avg_daily_demand * lead_time * 0.3, scale=np.maximum(avg_daily_demand, 1))
              ), 0, None).astype(int)
days_of_cover = np.where(avg_daily_demand > 0,
                         np.round(on_hand_qty / avg_daily_demand, 1), 999.0)

fact_inventory_snapshot = pd.DataFrame({
    "inventory_key":    np.arange(1, N + 1),   # PK
    "date_key":         date_key,              # FK ➜ dim_date
    "store_key":        store_key,             # FK ➜ dim_store
    "product_key":      product_key,           # FK ➜ dim_product
    "supplier_key":     supplier_key,          # FK ➜ dim_supplier
    "on_hand_qty":      on_hand_qty,
    "units_in_transit": units_in_transit,
    "reorder_point":    reorder_point,
    "days_of_cover":    days_of_cover,
})
save(fact_inventory_snapshot, "fact_inventory_snapshot")
print(f"grain: {ns} stores x {npd} products x {nday} days = {N:,} rows")