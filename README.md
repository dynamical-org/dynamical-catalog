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

# Open a dataset as xarray (via its icechunk repository)
ds = dynamical_catalog.open("noaa-gfs-forecast")

# Get the underlying zarr store
store = dynamical_catalog.get_store("noaa-gfs-forecast")

# List all available datasets
dynamical_catalog.list()
```
