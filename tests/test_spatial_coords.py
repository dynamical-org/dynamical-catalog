import pytest
import xarray as xr

from tests.test_integration import _assert_spatial_coords


@pytest.mark.parametrize(
    "ds",
    [
        pytest.param(
            xr.Dataset(coords={"latitude": [30, 40], "longitude": [-100, -90]}),
            id="geographic-dimensions",
        ),
        pytest.param(
            xr.Dataset(
                coords={
                    "y": ("y", [30, 40], {"standard_name": "latitude", "axis": "Y"}),
                    "x": ("x", [0, 180], {"standard_name": "longitude", "axis": "X"}),
                }
            ),
            id="native-geographic-xy",
        ),
        pytest.param(
            xr.Dataset(
                coords={
                    "y": [0, 1000],
                    "x": [0, 1000],
                    "latitude": (("y", "x"), [[30, 30], [40, 40]]),
                    "longitude": (("y", "x"), [[-100, -90], [-100, -90]]),
                }
            ),
            id="projected-with-geographic-coordinates",
        ),
    ],
)
def test_accepts_geographic_coordinates(ds: xr.Dataset) -> None:
    _assert_spatial_coords(ds, "example")


@pytest.mark.parametrize("missing", ["latitude", "longitude"])
def test_rejects_projected_axis_without_geographic_coordinate(missing: str) -> None:
    ds = xr.Dataset(
        coords={
            "y": (
                "y",
                [0, 1000],
                {"standard_name": "projection_y_coordinate", "axis": "Y", "units": "m"},
            ),
            "x": (
                "x",
                [0, 1000],
                {"standard_name": "projection_x_coordinate", "axis": "X", "units": "m"},
            ),
        }
    )
    present = "longitude" if missing == "latitude" else "latitude"
    ds = ds.assign_coords({present: (("y", "x"), [[0, 1], [2, 3]])})

    with pytest.raises(AssertionError, match=f"example missing {missing} coordinate"):
        _assert_spatial_coords(ds, "example")


def test_requires_geographic_variables_to_be_coordinates() -> None:
    ds = xr.Dataset(
        data_vars={
            "lat": ("y", [30, 40], {"standard_name": "latitude"}),
            "lon": ("x", [0, 180], {"standard_name": "longitude"}),
        },
        coords={"y": [0, 1], "x": [0, 1]},
    )

    with pytest.raises(AssertionError, match="example missing latitude coordinate"):
        _assert_spatial_coords(ds, "example")


def test_requires_spatial_dimensions() -> None:
    ds = xr.Dataset(coords={"latitude": 40, "longitude": -90})

    with pytest.raises(AssertionError, match="example missing spatial dims"):
        _assert_spatial_coords(ds, "example")
