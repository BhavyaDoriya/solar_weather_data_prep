# This script converts pv_power_5min.csv from 5 min interval to hourly interval and outputs it as pv_power_hourly.csv
# This conversation is essential to combine it with weather data.

import pandas as pd
from pathlib import Path

# --------------------
# Paths
# --------------------
INPUT_FILE = Path("processed/pv_power_5min.csv")
OUTPUT_FILE = Path("processed/pv_power_hourly.csv")

# --------------------
# Config
# --------------------
CHUNK_SIZE = 1_000_000      # safe for 16 GB RAM
FIVE_MIN_HOURS = 5 / 60    # 0.083333...

# --------------------
# Aggregation store
# --------------------
hourly_chunks = []

# --------------------
# Process in chunks
# --------------------
for chunk in pd.read_csv(INPUT_FILE, chunksize=CHUNK_SIZE):

    # ✅ Deterministic datetime parsing (ISO → datetime64)
    chunk["timestamp"] = pd.to_datetime(
        chunk["timestamp"],
        format="%Y-%m-%d %H:%M:%S",
        errors="coerce"
    )

    # Drop truly bad rows (should be none)
    chunk = chunk.dropna(subset=["timestamp"])

    # ✅ Hour label = START of hour (no shift, no deprecation warning)
    chunk["timestamp_hour"] = chunk["timestamp"].dt.floor("h")

    # MW → MWh for each 5-min interval
    chunk["energy_mwh"] = chunk["power_mw"] * FIVE_MIN_HOURS

    # Aggregate per plant per hour (inside chunk)
    grouped = (
        chunk
        .groupby(
            ["timestamp_hour", "lat", "lon", "capacity_mw"],
            as_index=False
        )
        .agg(energy_mwh=("energy_mwh", "sum"))
    )

    hourly_chunks.append(grouped)

# --------------------
# Safety check
# --------------------
if not hourly_chunks:
    raise RuntimeError("No data aggregated — check input file.")

# --------------------
# Final concat
# --------------------
hourly_df = pd.concat(hourly_chunks, ignore_index=True)

# --------------------
# Final aggregation (across chunks)
# --------------------
hourly_df = (
    hourly_df
    .groupby(
        ["timestamp_hour", "lat", "lon", "capacity_mw"],
        as_index=False
    )
    .agg(energy_mwh=("energy_mwh", "sum"))
)

# --------------------
# Rename column
# --------------------
hourly_df = hourly_df.rename(
    columns={"timestamp_hour": "timestamp"}
)

# --------------------
# Save
# --------------------
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
hourly_df.to_csv(OUTPUT_FILE, index=False)

print("✅ Hourly aggregation complete (NO time shift, NO warnings)")
print("Saved →", OUTPUT_FILE)
print("Shape:", hourly_df.shape)
