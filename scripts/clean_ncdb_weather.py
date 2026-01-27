# This will combine all *.csv files downloaded from ncdb for different locations(mentioned in weather_grids.csv) into 1 csv file
import pandas as pd
from pathlib import Path
import re

IN_DIR = Path("raw/weather")
OUT_FILE = Path("processed/ncdb_weather_hourly.csv")
OUT_FILE.parent.mkdir(parents=True, exist_ok=True)

all_dfs = []

for file in IN_DIR.glob("*.csv"):
    print("Processing:", file.name)

    # Find header row
    with open(file, "r") as f:
        lines = f.readlines()

    header_idx = None
    for i, line in enumerate(lines):
        if line.startswith("Year,"):
            header_idx = i
            break

    if header_idx is None:
        print("❌ Header not found:", file.name)
        continue

    df = pd.read_csv(file, skiprows=header_idx)

    # Build timestamp (safe & unambiguous)
    df["timestamp"] = pd.to_datetime(
        dict(
            year=df["Year"],
            month=df["Month"],
            day=df["Day"],
            hour=df["Hour"],
            minute=df["Minute"],
        ),
        errors="coerce"
    )

    # Extract lat/lon from filename
    match = re.search(r"ncdb_(-?\d+\.?\d*)_(-?\d+\.?\d*)_", file.name)
    if not match:
        print("❌ Lat/Lon not found in filename:", file.name)
        continue

    lat = float(match.group(1))
    lon = float(match.group(2))
    df["lat"] = lat
    df["lon"] = lon

    # Keep required columns only
    df = df[
        [
            "timestamp",
            "lat",
            "lon",
            "GHI",
            "DNI",
            "DHI",
            "Temperature",
            "Wind Speed",
            "Solar Zenith Angle",
        ]
    ]

    # Normalize column names
    df.columns = [
        "timestamp",
        "lat",
        "lon",
        "ghi",
        "dni",
        "dhi",
        "air_temp",
        "wind_speed",
        "solar_zenith",
    ]

    # Drop bad timestamps (should be none)
    df = df.dropna(subset=["timestamp"])

    all_dfs.append(df)

# Safety check
if not all_dfs:
    raise RuntimeError("No weather files were processed.")

weather = pd.concat(all_dfs, ignore_index=True)
weather.to_csv(OUT_FILE, index=False)

print("✅ Cleaned NCDB weather saved to:", OUT_FILE)
print("Shape:", weather.shape)
