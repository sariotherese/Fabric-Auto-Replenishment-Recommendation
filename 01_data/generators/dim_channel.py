# Block 7: dim_channel
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

dim_channel = pd.DataFrame({
    "channel_key": [1,2,3],                                # PK
    "channel_name": ["In-Store","Click & Collect","Delivery"],
    "is_digital":   [False, True, True],
})
save(dim_channel, "dim_channel")