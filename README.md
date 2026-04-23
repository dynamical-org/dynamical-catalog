# dynamical-catalog

Load [dynamical.org](https://dynamical.org) weather datasets in one line.

See [dynamical.org/catalog](https://dynamical.org/catalog/) for dataset documentation.

## Install

```bash
pip install dynamical-catalog
```

```bash
uv add dynamical-catalog
```

## Usage

```python
import dynamical_catalog

# Optional: let us know who you are so we can improve the catalog!
dynamical_catalog.identify("you@example.com")

# Open a dataset as an xarray Dataset via its icechunk repository
ds = dynamical_catalog.open("noaa-gfs-forecast")

# Additional arguments are passed through to `xr.open_zarr`
ds = dynamical_catalog.open("noaa-gfs-forecast", chunks=None)

# Get the underlying Zarr store if you want even more control
store = dynamical_catalog.get_store("noaa-gfs-forecast")
ds = xr.open_zarr(store)

# List all available datasets
dynamical_catalog.list()
```
