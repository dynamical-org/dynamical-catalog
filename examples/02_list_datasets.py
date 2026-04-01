"""Browse the catalog and inspect dataset metadata."""

import dynamical_catalog

dynamical_catalog.identify("dynamical-catalog example")

# List all available dataset IDs
print("Available datasets:")
for dataset_id in dynamical_catalog.list():
    print(f"  {dataset_id}")

# Use tab-completable catalog to inspect a dataset
entry = dynamical_catalog.catalog.noaa_gfs_forecast
print(f"\nDataset: {entry.name}")
print(f"Description: {entry.description}")
print(f"Zarr URL: {entry.zarr_url}")
print(f"Icechunk available: {entry.icechunk_config is not None}")
