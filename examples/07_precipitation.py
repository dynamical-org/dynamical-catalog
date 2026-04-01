"""Query high-resolution precipitation data from MRMS."""

import dynamical_catalog

dynamical_catalog.identify("dynamical-catalog example")

# Open the MRMS hourly precipitation analysis (~1km resolution, CONUS only)
ds = dynamical_catalog.open("noaa-mrms-conus-analysis-hourly")

# Get precipitation at a point over a time range
precip = ds["precipitation_surface"].sel(
    latitude=39.1,  # Kansas City, MO
    longitude=-94.6,
    method="nearest",
    time=slice("2025-06-01", "2025-06-07"),
)

total = precip.sum().compute()
print(f"Total precipitation in Kansas City, June 1-7 2025: {total.values:.1f} mm")
