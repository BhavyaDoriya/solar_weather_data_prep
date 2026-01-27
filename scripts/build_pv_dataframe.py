# After Actual_DPV files are extracted(or you are using all files->this will requires some change in script) you can run this script
# This scripts combines all the csv files into one single csv file that contains timestamp,system size,energy generated,lat,lon by extracting such features from file name, and reading the internal csv data.
# Output: processed/pv_power_5min.csv
import pandas as pd
from pathlib import Path
import re

DATA_DIR = Path("filtered/dpv_actual")#choose your downloaded file path if you are using all files instead of Actual_DPV
OUTPUT_FILE = Path("processed/pv_power_5min.csv")

all_dfs = []

def parse_filename(fname):
    """
    Handles filenames like:
    Actual_25.35_-80.35_2006_DPV_62MW_5_Min.csv
    Actual_35.7_-97.5_2006_DPV_50MW_5min.csv
    """
    # You can change patern based on your file names if you are using all the files of downloads from the https://www.nrel.gov/grid/solar-power-data
    pattern = (
        r"Actual_"
        r"(?P<lat>-?\d+\.?\d*)_"
        r"(?P<lon>-?\d+\.?\d*)_"
        r"2006_DPV_"
        r"(?P<cap>\d+\.?\d*)MW_"
        r"5_?Min\.csv"
    )

    match = re.match(pattern, fname, re.IGNORECASE)
    if not match:
        return None

    return (
        float(match.group("lat")),
        float(match.group("lon")),
        float(match.group("cap")),
    )


for file in DATA_DIR.glob("*.csv"):

    meta = parse_filename(file.name)
    if meta is None:
        print(f"Skipping unrecognized file: {file.name}")
        continue

    lat, lon, capacity_mw = meta

    df = pd.read_csv(file)
    df.columns = [c.strip().lower() for c in df.columns]

    # Detect timestamp column
    if "localtime" in df.columns:
        time_col = "localtime"
    elif "timestamp" in df.columns:
        time_col = "timestamp"
    else:
        raise RuntimeError(f"No time column found in {file.name}")

    # Detect power column
    power_candidates = [c for c in df.columns if "power" in c or "mw" in c]
    if not power_candidates:
        raise RuntimeError(f"No power column found in {file.name}")

    power_col = power_candidates[0]

    df = df.rename(columns={
        time_col: "timestamp",
        power_col: "power_mw"
    })

    #  datetime parsing (MM/DD/YY → ISO)
    df["timestamp"] = pd.to_datetime(
        df["timestamp"],
        format="%m/%d/%y %H:%M",
        errors="coerce"
    )

    # Drop junk rows
    df = df.dropna(subset=["timestamp"])

    # Attach metadata
    df["lat"] = lat
    df["lon"] = lon
    df["capacity_mw"] = capacity_mw
    df["plant_id"] = file.stem

    all_dfs.append(df)

if not all_dfs:
    raise RuntimeError("No CSV files were processed.")

final_df = pd.concat(all_dfs, ignore_index=True)

OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
final_df.to_csv(OUTPUT_FILE, index=False)

print(f"Saved 5-minute combined data → {OUTPUT_FILE}")
print("Shape:", final_df.shape)
print("Unique plants:", final_df["plant_id"].nunique())
