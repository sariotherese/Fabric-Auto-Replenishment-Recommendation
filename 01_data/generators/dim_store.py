# Block 2: dim_store
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

mm = [
 ("Makati CBD",14.5547,121.0244),("Makati Poblacion",14.5654,121.0297),("BGC Taguig",14.5510,121.0490),
 ("Taguig FTI",14.5176,121.0509),("Ortigas Pasig",14.5866,121.0614),("Pasig Kapitolyo",14.5710,121.0560),
 ("Mandaluyong",14.5794,121.0359),("San Juan",14.6019,121.0355),("QC Cubao",14.6190,121.0530),
 ("QC Diliman",14.6537,121.0687),("QC Commonwealth",14.6900,121.0830),("QC Eastwood",14.6090,121.0800),
 ("Manila Ermita",14.5826,120.9787),("Manila Binondo",14.6000,120.9740),("Manila Malate",14.5700,120.9870),
 ("Manila Sampaloc",14.6090,120.9930),("Pasay MOA",14.5350,120.9820),("Pasay Tramo",14.5430,121.0000),
 ("Paranaque BF",14.4793,121.0198),("Paranaque Sucat",14.4810,121.0210),("Las Pinas",14.4499,120.9830),
 ("Muntinlupa Alabang",14.4188,121.0410),("Marikina",14.6507,121.1029),("Caloocan South",14.6510,120.9830),
 ("Caloocan North",14.7560,121.0440),("Valenzuela",14.7000,120.9830),("Malabon",14.6620,120.9570),
 ("Navotas",14.6660,120.9420),("Pateros",14.5450,121.0670),("Mandaluyong Boni",14.5760,121.0330),
]
dim_store = pd.DataFrame(mm, columns=["city","latitude","longitude"])
dim_store.insert(0, "store_key", range(1, len(dim_store)+1))          # PK
dim_store["store_name"]   = "SariMart " + dim_store["city"]
dim_store["traffic_tier"] = rng.choice([1,2,3], len(dim_store), p=[0.3,0.4,0.3])  # 3=CBD
save(dim_store, "dim_store")