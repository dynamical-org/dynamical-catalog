# dynamical

Load [dynamical.org](https://dynamical.org) weather datasets in one line.

## Install

```bash
pip install dynamical

# For icechunk support:
pip install dynamical[icechunk]
```

## Usage

```python
import dynamical

# Open a dataset as xarray (zarr v3)
ds = dynamical.open("noaa-gfs-forecast")

# Open via icechunk
ds = dynamical.open("noaa-gfs-forecast", engine="icechunk")

# Get the underlying zarr store
store = dynamical.get_store("noaa-gfs-forecast")

# Tab-completable catalog
ds = dynamical.catalog.noaa_gfs_forecast.open()

# List all available datasets
dynamical.list()
```
