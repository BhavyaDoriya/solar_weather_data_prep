import subprocess
import sys

scripts = [
    "scripts/extract_dpv.py",
    "scripts/build_pv_dataframe.py",
    "scripts/aggregate_5min_to_hourly.py",
    "scripts/extract_weather_grids.py",
    "scripts/download_ncdb_weather.py",
    "scripts/clean_ncdb_weather.py",
    "scripts/merge_pv_weather.py",
]

for script in scripts:
    print(f"\n▶ Running {script}")
    result = subprocess.run([sys.executable, script])

    if result.returncode != 0:
        print(f"\n❌ Failed at {script}")
        sys.exit(1)

print("\n✅ All scripts completed successfully")
