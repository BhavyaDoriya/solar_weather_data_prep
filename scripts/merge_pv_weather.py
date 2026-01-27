# Now final step to merge the "Solar Power Data for Integration Studies" data downloaded and processed with "NCDB weather data" downloaded and processed.
# This code will merge pv_weather_hourly.csv with ncdb_weather_hourly.csv based on timestamp and nearby coordinates according to weather grids rule.
# Output: processed/final_dataset_pv_weather_hourly.csv 
# This is the final dataset containing both weather data+solar power generation data
import pandas as pd
from pathlib import Path

PV_FILE = Path("processed/pv_power_hourly.csv")
WX_FILE = Path("processed/ncdb_weather_hourly.csv")
OUT_FILE = Path("processed/final_dataset_pv_weather_hourly.csv")

pv = pd.read_csv(PV_FILE, parse_dates=["timestamp"])
wx = pd.read_csv(WX_FILE, parse_dates=["timestamp"])

# Ensure same grid resolution
pv["lat"] = pv["lat"].round(1)
pv["lon"] = pv["lon"].round(1)
wx["lat"] = wx["lat"].round(1)
wx["lon"] = wx["lon"].round(1)

print("PV rows:", len(pv))
print("WX rows:", len(wx))

merged = pv.merge(
    wx,
    on=["timestamp", "lat", "lon"],
    how="inner"
)

merged.to_csv(OUT_FILE, index=False)

print("✅ Final merged dataset saved →", OUT_FILE)
print("Final rows:", len(merged))
print("Dropped PV rows:", len(pv) - len(merged))
