"""Get the underlying zarr Store for direct access."""

import zarr

import dynamical_catalog

dynamical_catalog.identify("dynamical-catalog example")

# get_store returns a zarr.abc.Store — the primitive underlying open()
store = dynamical_catalog.get_store("noaa-gfs-forecast")

# Open it with zarr directly
group = zarr.open_group(store)
print(f"Variables: {list(group.keys())}")

# Or pass it to xarray yourself with custom options
import xarray as xr  # noqa: E402

ds = xr.open_zarr(store, chunks={"latitude": 100, "longitude": 100})
print(ds)
