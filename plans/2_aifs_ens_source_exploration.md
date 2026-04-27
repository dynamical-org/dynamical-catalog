# Source Data Exploration: ECMWF AIFS ENS

Following [`reformatters/docs/source_data_exploration_guide.md`](https://github.com/dynamical-org/reformatters/blob/main/docs/source_data_exploration_guide.md).

ECMWF's AI Forecasting System ensemble (AIFS-ENS) is a 50-member + control machine-learning ensemble run operationally on ECMWF's open-data feed. Verified directly against sample files in the `ecmwf-forecasts` S3 bucket on 2026-04-27.

---

## Dataset: ECMWF AIFS-ENS (ENFO product, 0.25°)

### Source Information
- **Summary of data organization**: One init time has its own directory. Per init time, three GRIB2 files per lead-time step:
  - `*-{F}h-enfo-cf.grib2` — control forecast (single member), all variables and pressure levels in one file (~80 MB).
  - `*-{F}h-enfo-pf.grib2` — perturbed forecast, **all 50 ensemble members concatenated** in one file, all variables and pressure levels (~4.1 GB). Members are interleaved (records for member 1 are not contiguous), so single-member extraction requires multiple byte ranges from the index.
  - `*-{F}h-enfo-ep.grib2` — pre-aggregated ensemble products (mean / std / probability of exceedance). Only two files per init: `240h` covers steps 0–240h, `360h` covers steps 246–360h.
  - Each `.grib2` has a sibling `.index` file (JSON-Lines, ECMWF style).
- **File format**: GRIB2 (edition 2). Index files are JSON-per-line (one JSON object per GRIB message).
- **Temporal coverage**: 2025-07-02 00z (first AIFS-ENS init available in the bucket) → present. Confirmed by binary-search of S3 listings; `aifs-ens/` is absent on 2025-07-01 and earlier and present on every init from 2025-07-02 00z onward.
- **Temporal frequency**:
  - Init times: every 6 hours (00z / 06z / 12z / 18z).
  - Lead-time steps: every 6 hours from 0h to 360h inclusive — **61 steps** (0, 6, 12, …, 360). No mixed-frequency tail like IFS HRES has.
  - The two `enfo-ep` files (240h, 360h) internally also cover 24-hour-window probabilities every 6h (e.g. `0-24`, `6-30`, …, `336-360`).
- **Latency**: For the 2026-04-26 00z run, all 61 cf step files were last-modified between 05:38 and 05:44 UTC — i.e. step 0h appeared ~5h38m after init and the last step (360h) ~5h44m after init. The whole forecast publishes within roughly a 6-minute window ~5h40m after init time.
- **Access notes**:
  - Public S3 bucket, region `eu-west-1`, anonymous reads allowed.
  - HTTPS works (`https://ecmwf-forecasts.s3.amazonaws.com/...`) and returns S3 ListBucketV2 XML for `?list-type=2&prefix=...`.
  - No `s3://` credentials needed (`anon=true`).
  - Bucket throttles aggressive parallel readers with `503 SlowDown`; use modest concurrency or back-off retries.
- **Browse root**: <https://ecmwf-forecasts.s3.amazonaws.com/> (XML listing), human-friendly index at <https://data.ecmwf.int/forecasts/>.
- **URL format**:
```
s3://ecmwf-forecasts/{YYYYMMDD}/{HH}z/aifs-ens/0p25/enfo/{YYYYMMDDHHMMSS}-{F}h-enfo-{cf|pf|ep}.grib2
s3://ecmwf-forecasts/{YYYYMMDD}/{HH}z/aifs-ens/0p25/enfo/{YYYYMMDDHHMMSS}-{F}h-enfo-{cf|pf|ep}.index
s3://ecmwf-forecasts/{YYYYMMDD}/{HH}z/aifs-ens/0p25/enfo/README.txt
s3://ecmwf-forecasts/{YYYYMMDD}/{HH}z/aifs-ens/0p25/enfo/LICENCE.txt
```
where `HH ∈ {00,06,12,18}`, `F ∈ {0,6,12,…,360}` for cf/pf and `F ∈ {240,360}` for ep.

- **Example URLs**:
```
https://ecmwf-forecasts.s3.amazonaws.com/20250702/00z/aifs-ens/0p25/enfo/20250702000000-0h-enfo-cf.grib2
https://ecmwf-forecasts.s3.amazonaws.com/20250702/00z/aifs-ens/0p25/enfo/20250702000000-0h-enfo-cf.index
https://ecmwf-forecasts.s3.amazonaws.com/20260426/00z/aifs-ens/0p25/enfo/20260426000000-360h-enfo-pf.grib2
https://ecmwf-forecasts.s3.amazonaws.com/20260426/00z/aifs-ens/0p25/enfo/20260426000000-360h-enfo-ep.grib2
```

### GRIB Index
- **Index files available**: Yes — one `.index` per `.grib2`, identical basename.
- **Index style**: ECMWF (one JSON object per line; offsets are byte ranges into the matching `.grib2`).
- **Example line** (from `20260426000000-0h-enfo-cf.index`):
```
{"domain": "g", "date": "20260426", "time": "0000", "expver": "0001", "class": "ai", "type": "cf", "stream": "enfo", "step": "0", "levelist": "600", "levtype": "pl", "param": "q", "model": "aifs-ens", "_offset": 0, "_length": 730670}
```
- **Member-level pf example**:
```
{"domain": "g", "date": "20260426", "time": "0000", "expver": "0001", "class": "ai", "type": "pf", "stream": "enfo", "levtype": "sfc", "number": "23", "step": "0", "param": "tp", "model": "aifs-ens", "_offset": 0, "_length": 227}
```
- **Notes**: `class: ai` flags the ML-model stream (vs `od` for IFS). MARS `type` distinguishes products inside a file:
  - cf file: `type=cf`
  - pf file: `type=pf`, additional `number` field (1–50)
  - ep file: mixed `type ∈ {em, es, ep}` (ensemble mean / std-dev / probability)

### Coordinate Reference System
- **Common name**: WGS84-style geographic lat/lon on a spherical earth (ECMWF default sphere, R = 6,371,229 m). Equivalent for downstream use to plain `EPSG:4326` for most regridding purposes — but note the spherical (not WGS84-ellipsoid) datum.
- **PROJ string / EPSG**: `+proj=longlat +R=6371229 +no_defs` (no exact EPSG; closest standard match is `EPSG:4326`). GDAL reports:
```
GEOGCS["Coordinate System imported from GRIB file",
  DATUM["unnamed", SPHEROID["Sphere", 6371229, 0]],
  PRIMEM["Greenwich", 0],
  UNIT["degree", 0.0174532925199433]]
```

### Dimensions & Dimension Coordinates

| Dimension | Min | Max | Step | Size | Notes |
|-----------|-----|-----|------|------|-------|
| `init_time` (run reference time) | 2025-07-02T00:00Z | present | 6 h | growing | One forecast every 00/06/12/18 UTC |
| `lead_time` (step) | 0 h | 360 h | 6 h | 61 | Same grid for cf and pf; ep adds 24-h-window steps internally |
| `latitude` | −90.0° | +90.0° | 0.25° (north→south stored) | 721 | Pixel centers; first row = +90, last = −90 |
| `longitude` | −180.0° | +179.75° | 0.25° | 1440 | Pixel centers; row starts at −180.0 (full global, no wrap) |
| `ensemble_member` | 1 | 50 | 1 | 50 | Only in pf file; control is its own file (members 0 by convention) |
| `pressure_level` | 50 hPa | 1000 hPa | irregular | 13 | 50, 100, 150, 200, 250, 300, 400, 500, 600, 700, 850, 925, 1000 |
| `soil_layer` | 1 | 2 | 1 | 2 | MARS `levtype=sol`; only `sot` (soil temperature) |

We use pixel centers for spatial coordinates (consistent with rasterio bounds: `BoundingBox(left=-180.125, bottom=-90.125, right=179.875, top=90.125)` → centers at −180.0/+90.0).

### Data Variables

Verified by opening `recent-cf-0h.grib2` (2026-04-26 00z, step 0h) with `xarray.open_dataset(..., engine="cfgrib")`. The cf and per-member pf files contain the same 103 messages; later steps drop 4 static fields (`lsm`, `sdor`, `slor`, surface `z`).

Pressure-level (13 levels each):

| Param | shortName | Units | long_name |
|-------|-----------|-------|-----------|
| t | t | K | Temperature |
| u | u | m s⁻¹ | U component of wind |
| v | v | m s⁻¹ | V component of wind |
| w | w | Pa s⁻¹ | Vertical velocity (omega) |
| z | z | m² s⁻² | Geopotential |
| q | q | kg kg⁻¹ | Specific humidity |

Surface / single-level:

| Variable | Level | Units | Available from | Notes |
|----------|-------|-------|----------------|-------|
| temperature_2m (`2t`) | 2 m AGL | K | 2025-07-02 00z | Instantaneous |
| dew_point_temperature_2m (`2d`) | 2 m AGL | K | 2025-07-02 00z | Instantaneous |
| wind_u_10m (`10u`) | 10 m AGL | m s⁻¹ | 2025-07-02 00z | Instantaneous |
| wind_v_10m (`10v`) | 10 m AGL | m s⁻¹ | 2025-07-02 00z | Instantaneous |
| wind_u_100m (`100u`) | 100 m AGL | m s⁻¹ | 2025-07-02 00z | Instantaneous |
| wind_v_100m (`100v`) | 100 m AGL | m s⁻¹ | 2025-07-02 00z | Instantaneous |
| precipitation_surface (`tp`) | surface | kg m⁻² (= mm) | 2025-07-02 00z | **Accumulated from forecast start (step 0)**, monotonically non-decreasing across steps; differentiate to get per-step accumulation |
| snowfall_surface (`sf`) | surface | kg m⁻² | 2025-07-02 00z | Accumulated from step 0, same convention as `tp` |
| runoff_surface (`rowe`) | surface | kg m⁻² | 2025-07-02 00z | Accumulated from step 0 |
| downward_short_wave_radiation_flux_surface (`ssrd`) | surface | J m⁻² | 2025-07-02 00z | **Accumulated** energy from step 0; divide by Δt for mean flux |
| downward_long_wave_radiation_flux_surface (`strd`) | surface | J m⁻² | 2025-07-02 00z | Accumulated, same convention as `ssrd` |
| pressure_surface (`sp`) | surface | Pa | 2025-07-02 00z | Instantaneous |
| pressure_reduced_to_mean_sea_level (`msl`) | MSL | Pa | 2025-07-02 00z | Instantaneous |
| total_cloud_cover_atmosphere (`tcc`) | entireAtmosphere | % (0–100) | 2025-07-02 00z | Instantaneous |
| high_cloud_cover (`hcc`) | highCloudLayer | % | 2025-07-02 00z | Instantaneous |
| middle_cloud_cover (`mcc`) | middleCloudLayer | % | 2025-07-02 00z | Instantaneous |
| low_cloud_cover (`lcc`) | lowCloudLayer | % | 2025-07-02 00z | Instantaneous |
| total_column_water (`tcw`) | entireAtmosphere | kg m⁻² | 2025-07-02 00z | Instantaneous |
| skin_temperature (`skt`) | surface | K | 2025-07-02 00z | Instantaneous |
| soil_temperature (`sot`) | sol levels 1, 2 | K | 2025-07-02 00z | 2 layers; ECMWF `levtype=sol` |
| land_sea_mask (`lsm`) | surface | 0–1 | step 0h only | Static; only published in step-0 file |
| orography_geopotential (`z`) | surface | m² s⁻² | step 0h only | Static; surface geopotential = orography × g |
| std_dev_subgrid_orography (`sdor`) | surface | m | step 0h only | Static |
| slope_subgrid_orography (`slor`) | surface | numeric | step 0h only | Static |

Variables not available in AIFS-ENS (gaps relative to the canonical surface variable list in the guide):
- `relative_humidity_2m` — not produced. (Can be derived from `2t`, `2d`, `sp` via Magnus / Tetens.)
- `specific_humidity_2m` — not produced as a 2 m field (PL `q` is available at 1000 hPa).

EP file (pre-aggregated ensemble products, two files per init: `…-240h-enfo-ep.grib2` covers steps 0–240h, `…-360h-enfo-ep.grib2` covers 246–360h):

| MARS type | Product | Variables | Steps |
|-----------|---------|-----------|-------|
| `em` | Ensemble mean | `msl` (sfc), `t` at 250/500/850 hPa, `ws` (wind speed) at 250/850 hPa, `z` at 300/500/1000 hPa | every 6 h, 0–360 h |
| `es` | Ensemble std-dev | same set as `em` | every 6 h, 0–360 h |
| `ep` | Probability tp > threshold | `tpg1`, `tpg5`, `tpg10`, `tpg20`, `tpg25`, `tpg50`, `tpg100` (mm) | 24-hour windows: `0-24`, `6-30`, `12-36`, …, `336-360` |

**Temporal availability changes**:
- AIFS-ENS first init: **2025-07-02 00z**. No earlier archive in this bucket. (For an even longer hindcast, ECMWF separately publishes AIFS-ENS reforecasts via MARS, not in this S3 mirror.)
- No schema differences detected between the first available date (2025-07-02) and a recent date (2026-04-26): identical 103-message cf at step 0, identical 99 messages at downstream steps, identical 0.25° grid.

### Sample Files Examined

- **Earliest archive**: `s3://ecmwf-forecasts/20250702/00z/aifs-ens/0p25/enfo/20250702000000-0h-enfo-cf.grib2` (79 MB) and matching `.index`.
- **Earliest cross-init**: `s3://ecmwf-forecasts/20250702/12z/aifs-ens/0p25/enfo/20250702120000-12h-enfo-cf.grib2` (84 MB) — to confirm grid is invariant across init hours.
- **Recent step-0**: `s3://ecmwf-forecasts/20260426/00z/aifs-ens/0p25/enfo/20260426000000-0h-enfo-cf.grib2` + `.index`.
- **Recent end-of-forecast**: `s3://ecmwf-forecasts/20260426/00z/aifs-ens/0p25/enfo/20260426000000-360h-enfo-cf.grib2` + `.index`.
- **Recent ep file (long-range)**: `s3://ecmwf-forecasts/20260426/00z/aifs-ens/0p25/enfo/20260426000000-360h-enfo-ep.grib2` + `.index`.
- **Recent pf index only** (file is 4.1 GB; reformatter will extract per-member byte ranges via index rather than downloading the whole file): `s3://ecmwf-forecasts/20260426/00z/aifs-ens/0p25/enfo/20260426000000-0h-enfo-pf.index` (5,150 lines = 50 members × 103 messages).
- **Discovery boundary**: confirmed `aifs-ens/` directory absent on `20250701/{00,06,12,18}z/` and present on every init from `20250702/00z` onward.

### Notable Observations

- **`pf` member layout is interleaved, not block-striped per member**. A single ensemble member's records span the whole file (member-1 byte range observed: 20.6 MB → 4.07 GB), so any per-member extraction must issue one HTTP byte-range per message (~103 ranges per file), not a single contiguous range. The `.index` is essential — random-access of pf without it is impractical.
- **Static surface fields only ship at step 0**: `lsm`, `sdor`, `slor`, surface `z` (orography). Reformatter must read these once per init and broadcast across `lead_time`, not expect them at every step.
- **All accumulated fields (`tp`, `sf`, `rowe`, `ssrd`, `strd`) accumulate from step 0**, not from previous step. To get per-step accumulations the reformatter needs to subtract step k−1 from step k (and define step 0 = 0). This matches IFS HRES convention but differs from GFS, which resets accumulation at each forecast hour for some products.
- **No mixed-frequency forecast tail**: AIFS-ENS publishes a clean 6-hourly grid all the way to 360h, unlike IFS-ENS (which switches from 6h to 12h beyond 144h on the open data feed). This makes the lead-time axis simpler to model.
- **Two `ep` files cover non-overlapping step ranges**: `…-240h-enfo-ep.grib2` is 0–240h, `…-360h-enfo-ep.grib2` is 246–360h. Don't assume the filename step is the only step inside the file.
- **`class=ai` MARS field**: indexes mark this stream with `"class": "ai"`, distinguishing it from operational physics-based runs (`class=od`). Useful as a positive filter when collocating with IFS ENS.
- **CRS is spherical, not WGS84 ellipsoid**: R = 6,371,229 m. Acceptable as `EPSG:4326` for most regridding but worth noting for sub-km-precise applications.
- **Throttling**: parallel index-fetches occasionally returned `503 SlowDown` with `<Code>SlowDown</Code>` XML bodies overwriting the file. Any production fetcher needs retry/back-off and a "validate file is GRIB / valid JSONL" check.

---

## Implementation hints (for follow-on `TemplateConfig` / `RegionJob` work)

- Suggested zarr dimension order: `(init_time, ensemble_member, lead_time, latitude, longitude)` for ensemble vars, `(init_time, lead_time, pressure_level, latitude, longitude)` for cf-only PL vars, `(init_time, latitude, longitude)` for static fields.
- `ensemble_member` could be modeled with values `0..50`, where `0` = control (cf file) and `1..50` = perturbed (pf file).
- One `RegionJob` ≈ one `(init_time, lead_time)` pair. cf and pf are downloaded independently; ep can be a separate optional product.
- File-naming template for the URL builder:
  `f"{base}/{date:%Y%m%d}/{init:%H}z/aifs-ens/0p25/enfo/{date:%Y%m%d%H%M%S}-{step}h-enfo-{kind}.grib2"`
  with `kind ∈ {"cf","pf","ep"}` and `base = "https://ecmwf-forecasts.s3.amazonaws.com"` (or `s3://ecmwf-forecasts`).
- Use the `.index` first to plan byte-range reads — particularly mandatory for the 4 GB pf file.
