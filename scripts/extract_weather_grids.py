# This script is used to reduce overload on api while fetching weather data
# see, if we look at the coordinates listed in pv_powe_hourly.csv there will be many unique coordinates
# and if we fetch weather data for each coordinate it will cause api calling issue and it is not practical as well.
# so we are creating weather grids which will combine nearby coordinates as one general coordinate 
# considering nearby coordinates(in this case coordinates in range of 10km) as 1 coordinate we can
# reduce the number of calls we would require to fetch the weather data since weather is same in nearby locations
import pandas as pd

df = pd.read_csv("processed/pv_power_hourly.csv")

df["grid_lat"] = df["lat"].round(1)
df["grid_lon"] = df["lon"].round(1)

grids = (
    df[["grid_lat", "grid_lon"]]
    .drop_duplicates()
    .reset_index(drop=True)
)

grids.to_csv("processed/weather_grids.csv", index=False)
print("Total weather grids:", len(grids))
