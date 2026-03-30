"""Open a dataset via Icechunk for transactional, versioned access."""

import dynamical

# Open the same dataset through Icechunk instead of Zarr v3
ds = dynamical.open("noaa-gfs-forecast", engine="icechunk")

# Usage is identical — it's still an xarray Dataset
temp = ds["temperature_2m"].sel(
    init_time="2025-06-01T00",
    latitude=35.7,     # Tokyo
    longitude=139.7,
    method="nearest",
)

max_temp = temp.max().compute()
print(f"Max forecast temperature in Tokyo: {max_temp.values:.1f} K")

# Or get the raw icechunk store
store = dynamical.get_store("noaa-gfs-forecast", engine="icechunk")
print(f"Store type: {type(store).__name__}")
