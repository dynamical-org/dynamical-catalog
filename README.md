# dynamical-catalog

Load [dynamical.org](https://dynamical.org) weather datasets in one line.

## Install

```bash
pip install dynamical-catalog
```

## Usage

```python
import dynamical_catalog

# Optional: let us know who you are so we can improve the catalog!
dynamical_catalog.identify("you@example.com")

# Open a dataset as xarray (zarr v3)
ds = dynamical_catalog.open("noaa-gfs-forecast")

# Open via icechunk
ds = dynamical_catalog.open("noaa-gfs-forecast", engine="icechunk")

# Get the underlying zarr store
store = dynamical_catalog.get_store("noaa-gfs-forecast")

# Tab-completable catalog
ds = dynamical_catalog.catalog.noaa_gfs_forecast.open()

# List all available datasets
dynamical_catalog.list()
```
