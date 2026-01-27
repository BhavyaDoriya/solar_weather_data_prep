# This is step-1
# Download required data from solar power integration studies data from : https://www.nrel.gov/grid/solar-power-data
# (choose whatever state data you would like)
# After downloading put them in raw/solar/
# Now processing on downloaded data:
# This script extracts DPV Actual CSV files from a raw data directory
# and copies them into a filtered destination directory.
# The reason is simple. I am working on training the data based on Actual_**.csv files 
# not 4 hour ahead or day ahead forecast files, i am using DPV files only. 
# To understand the DPV,DA,HA4,UPV please checkout official website listed above
# You should not run following code if you want to use HA4_DPV,HA4_UPV,DA_DPV,DA_UPV all csvs
# If you really want to work on all files just change the code in build_pv_dataframe.py which i will guide you with in that file.


# output: filtered/dpv_actual/*Actual_**_DPV**.csv filea
import shutil
from pathlib import Path

RAW = Path("raw")
DEST = Path("filtered/dpv_actual")
DEST.mkdir(parents=True, exist_ok=True)

for state_dir in RAW.iterdir():
    for f in state_dir.glob("*.csv"):
        if "Actual" in f.name and "_DPV_" in f.name:
            shutil.copy(f, DEST / f.name)
