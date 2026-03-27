"""Work with ensemble forecast data (GEFS and ECMWF IFS ENS)."""

import dynamical

# Open the GEFS 35-day ensemble forecast
ds = dynamical.open("noaa-gefs-forecast-35-day")

# Select one initialization and location
forecast = ds["temperature_2m"].sel(
    init_time="2025-06-01T00",
    latitude=48.9,     # Paris
    longitude=2.3,
    method="nearest",
)

# Compute the ensemble spread at lead_time = 5 days
five_day = forecast.sel(lead_time="120h")
ensemble_mean = five_day.mean(dim="ensemble_member").compute()
ensemble_std = five_day.std(dim="ensemble_member").compute()

print(f"5-day forecast ensemble mean: {ensemble_mean.values:.1f} K")
print(f"5-day forecast ensemble std:  {ensemble_std.values:.1f} K")
