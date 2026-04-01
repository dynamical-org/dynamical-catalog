"""Work with forecast data: init_time and lead_time dimensions."""

import dynamical_catalog

dynamical_catalog.identify("dynamical-catalog example")

# Open the GFS forecast dataset
ds = dynamical_catalog.open("noaa-gfs-forecast")

# Select one forecast initialization and a location
forecast = ds["temperature_2m"].sel(
    init_time="2025-06-01T00",
    latitude=51.5,  # London
    longitude=-0.1,
    method="nearest",
)

# Get the max temperature across all lead times in this forecast
max_temp = forecast.max().compute()
print(f"Max forecast temperature: {max_temp.values:.1f} K")

# Get the full time series for this forecast (lazy until .compute())
print(f"\nForecast shape: {forecast.sizes}")
print(f"Lead times: {forecast.lead_time.values[:5]} ...")
