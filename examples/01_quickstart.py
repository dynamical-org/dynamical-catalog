"""Quickstart: open a dataset and read a single value."""

import dynamical

# Open the GFS analysis dataset (lazily — no data downloaded yet)
ds = dynamical.open("noaa-gfs-analysis")

# Read 2m temperature at a specific place and time
temp = ds["temperature_2m"].sel(
    time="2025-06-01T12",
    latitude=40.7,    # New York City
    longitude=-74.0,
    method="nearest",
).compute()

print(f"Temperature in NYC on 2025-06-01 12Z: {temp.values:.1f} K")
