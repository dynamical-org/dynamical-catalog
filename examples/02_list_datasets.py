"""Browse the catalog and inspect dataset metadata."""

import dynamical

dynamical.identify("dynamical-py example")

# List all available dataset IDs
print("Available datasets:")
for dataset_id in dynamical.list():
    print(f"  {dataset_id}")

# Use tab-completable catalog to inspect a dataset
entry = dynamical.catalog.noaa_gfs_forecast
print(f"\nDataset: {entry.name}")
print(f"Description: {entry.description}")
print(f"Zarr URL: {entry.zarr_url}")
print(f"Icechunk available: {entry.icechunk_config is not None}")
