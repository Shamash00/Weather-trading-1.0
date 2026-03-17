"""
GEFS (Global Ensemble Forecast System) AWS S3 Data Access - Research Notes
===========================================================================

Complete reference for accessing GEFS ensemble forecast data from AWS S3.
Compiled for weather trading strategy backtesting on Polymarket.

Last updated: 2026-03-15
"""

# =============================================================================
# 1. TWO SEPARATE DATASETS ON AWS S3
# =============================================================================
#
# DATASET A: OPERATIONAL GEFS FORECASTS (2017-present)
#   Bucket:  s3://noaa-gefs-pds
#   Region:  us-east-1
#   Access:  Public, no authentication needed
#   CLI:     aws s3 ls --no-sign-request s3://noaa-gefs-pds/
#   Browser: https://noaa-gefs-pds.s3.amazonaws.com/index.html
#
# DATASET B: GEFS REFORECAST / RETROSPECTIVE (2000-2019)
#   Bucket:  s3://noaa-gefs-retrospective
#   Region:  us-east-1
#   Access:  Public, no authentication needed
#   CLI:     aws s3 ls --no-sign-request s3://noaa-gefs-retrospective/
#   Browser: https://noaa-gefs-retrospective.s3.amazonaws.com/index.html
#

# =============================================================================
# 2. DATASET A: OPERATIONAL GEFS (noaa-gefs-pds) - DETAILS
# =============================================================================
#
# TEMPORAL COVERAGE: January 1, 2017 - present (continuously updated)
# FORMAT: GRIB2
# ENSEMBLE MEMBERS: 21 total (1 control + 20 perturbations)
# RUNS PER DAY: 4 (00Z, 06Z, 12Z, 18Z)
# FORECAST HORIZON: Up to 384 hours (16 days)
# FORECAST STEP: 6-hour intervals (f000, f006, f012, ... f384)
#
# PATH STRUCTURE (pre-September 2020, GEFSv11):
#   s3://noaa-gefs-pds/gefs.YYYYMMDD/HH/pgrb2a/FILENAME
#   s3://noaa-gefs-pds/gefs.YYYYMMDD/HH/pgrb2b/FILENAME
#
# PATH STRUCTURE (post-September 2020, GEFSv12):
#   s3://noaa-gefs-pds/gefs.YYYYMMDD/HH/atmos/pgrb2ap5/FILENAME   (0.5 deg, ~83 vars)
#   s3://noaa-gefs-pds/gefs.YYYYMMDD/HH/atmos/pgrb2bp5/FILENAME   (0.5 deg, ~500 vars)
#   s3://noaa-gefs-pds/gefs.YYYYMMDD/HH/atmos/pgrb2sp25/FILENAME  (0.25 deg, ~35 vars)
#
# FILE NAMING:
#   Control:    gec00.tHHz.pgrb2a.0p50.fVVV   (or pgrb2s.0p25 for 0.25 deg)
#   Members:    gepWW.tHHz.pgrb2a.0p50.fVVV   (WW = 01-20, VVV = forecast hour)
#   Mean:       geavg.tHHz.pgrb2a.0p50.fVVV
#   Spread:     gespr.tHHz.pgrb2a.0p50.fVVV
#
# RESOLUTION:
#   pgrb2ap5:  0.5 degree (~55 km) - most common variables (~83)
#   pgrb2sp25: 0.25 degree (~28 km) - selected variables (~35)
#   2m temperature IS in both pgrb2ap5 and pgrb2sp25
#
# INDEX FILES:
#   Every GRIB2 file has a companion .idx file enabling byte-range requests
#   e.g., gec00.t00z.pgrb2a.0p50.f006.idx
#   This allows downloading ONLY the 2m temperature variable without the full file!
#
# EXAMPLE FULL URLs:
#   https://noaa-gefs-pds.s3.amazonaws.com/gefs.20260315/00/atmos/pgrb2ap5/gec00.t00z.pgrb2a.0p50.f006
#   https://noaa-gefs-pds.s3.amazonaws.com/gefs.20260315/00/atmos/pgrb2sp25/gec00.t00z.pgrb2s.0p25.f006
#

# =============================================================================
# 3. DATASET B: GEFS REFORECAST (noaa-gefs-retrospective) - DETAILS
# =============================================================================
#
# TEMPORAL COVERAGE: 2000-01-01 through 2019-12-31
# FORMAT: GRIB2
# ENSEMBLE MEMBERS: 5 daily (c00, p01, p02, p03, p04), 11 weekly (+ p05-p10)
# RUNS PER DAY: 1 (00Z only)
# FORECAST HORIZON: 16 days daily, 35 days weekly
# FORECAST STEP: 3-hourly for first 10 days, 6-hourly for days 10-16
#
# RESOLUTION:
#   0.25 degree for first 10 days (days 1-10)
#   0.50 degree for days 10-16
#   Upper air (above 700mb) always 0.5 degree
#
# PATH STRUCTURE:
#   s3://noaa-gefs-retrospective/GEFSv12/reforecast/YYYY/YYYYMMDD00/MEMBER/DAYS/VARIABLE
#
# Example:
#   s3://noaa-gefs-retrospective/GEFSv12/reforecast/2019/2019010100/c00/Days:1-10/tmp_2m
#   s3://noaa-gefs-retrospective/GEFSv12/reforecast/2019/2019010100/p01/Days:1-10/tmp_2m
#
# AVAILABLE VARIABLES (variable_level names for path):
#   tmp_2m      - 2m temperature (THIS IS WHAT WE WANT)
#   tmax_2m     - 2m max temperature
#   tmin_2m     - 2m min temperature
#   apcp_sfc    - accumulated precipitation
#   acpcp_sfc   - convective precipitation
#   cape_sfc    - CAPE
#   cin_sfc     - CIN
#   dswrf_sfc   - downward shortwave radiation
#   dlwrf_sfc   - downward longwave radiation
#   gust_sfc    - wind gust
#   hgt_*       - geopotential height
#   pwat_eatm   - precipitable water
#   rh_2m       - 2m relative humidity
#   spfh_2m     - 2m specific humidity
#   ugrd_10m    - 10m u-wind
#   vgrd_10m    - 10m v-wind
#   pres_sfc    - surface pressure
#   tcdc_eatm   - total cloud cover
#   soilw_*     - soil moisture
#   tsoil_*     - soil temperature
#   ... and more (acpcp, cape, cin, dlwrf, dswrf, gflux, gust, hgt, hlcy,
#                  lhtfl, ncpcp, pbl, pres, pvort, pwat, rh, sfcr, shtfl,
#                  soilw, spfh, tcdc, tmax, tmin, tmp, tozne, tsoil, uflx,
#                  ugrd, ulwrf, uswrf, vflx, vgrd, vvel, watr, weasd)
#

# =============================================================================
# 4. PYTHON LIBRARIES FOR ACCESSING THIS DATA
# =============================================================================
#
# RECOMMENDED STACK:
#   pip install herbie-data   # Best high-level tool for GEFS access
#   pip install xarray cfgrib eccodes  # For reading GRIB2 into xarray
#   pip install s3fs boto3    # For direct S3 access
#   pip install pandas numpy  # Standard data processing
#

# =============================================================================
# 5. PRACTICAL CODE EXAMPLES
# =============================================================================

import warnings
warnings.filterwarnings('ignore')


# -----------------------------------------------------------------------------
# EXAMPLE 1: Using Herbie to get 2m temperature from operational GEFS
# -----------------------------------------------------------------------------
def example_herbie_operational():
    """Download 2m temperature for all ensemble members from operational GEFS."""
    from herbie import Herbie
    import pandas as pd
    import xarray as xr

    date = "2026-03-14 00:00"
    fxx = 24  # 24-hour forecast

    all_members = []

    # Loop over all 31 ensemble members (control + 30 perturbations for GEFSv12)
    for member in range(0, 31):
        try:
            H = Herbie(
                date,
                model="gefs",
                product="atmos.5",   # 0.5 degree, ~83 variables
                member=member,
                fxx=fxx,
            )
            # Download only 2m temperature using byte-range subsetting
            ds = H.xarray("TMP:2 m above ground")
            all_members.append(ds)
            print(f"Member {member:02d}: OK")
        except Exception as e:
            print(f"Member {member:02d}: FAILED - {e}")

    # Combine all members
    if all_members:
        combined = xr.concat(all_members, dim="member")
        print(f"Shape: {combined['t2m'].shape}")
        return combined


# -----------------------------------------------------------------------------
# EXAMPLE 2: Using Herbie to get reforecast data (2000-2019)
# -----------------------------------------------------------------------------
def example_herbie_reforecast():
    """Download 2m temperature reforecast for backtesting."""
    from herbie import Herbie

    H = Herbie(
        "2019-07-15",
        model="gefs_reforecast",
        fxx=24,                   # 24-hour forecast lead time
        member=0,                 # Control member (c00)
        variable_level="tmp_2m",  # 2m temperature
    )

    # Download the file
    local_path = H.download(verbose=True)
    print(f"Downloaded to: {local_path}")

    # Read into xarray
    ds = H.xarray(":TMP:2 m above ground:")
    print(ds)

    # Loop over all 5 members for a given date
    for member in range(5):  # c00, p01, p02, p03, p04
        H = Herbie(
            "2019-07-15",
            model="gefs_reforecast",
            fxx=24,
            member=member,
            variable_level="tmp_2m",
        )
        ds = H.xarray(":TMP:2 m above ground:")
        print(f"Member {member}: shape = {ds['t2m'].shape}")


# -----------------------------------------------------------------------------
# EXAMPLE 3: Direct S3 access with s3fs (no Herbie dependency)
# -----------------------------------------------------------------------------
def example_direct_s3_access():
    """Access GEFS data directly from S3 without Herbie."""
    import s3fs
    import xarray as xr
    import tempfile
    import os

    # Create anonymous S3 filesystem (no credentials needed)
    fs = s3fs.S3FileSystem(anon=True)

    # List available dates in the operational bucket
    dates = fs.ls('noaa-gefs-pds/')
    print(f"Available date folders: {dates[:5]}...")

    # Download a specific file
    # Operational GEFS, 2026-03-14, 00Z run, control member, 24h forecast
    s3_path = (
        "noaa-gefs-pds/gefs.20260314/00/atmos/pgrb2ap5/"
        "gec00.t00z.pgrb2a.0p50.f024"
    )

    with tempfile.NamedTemporaryFile(suffix='.grib2', delete=False) as tmp:
        fs.get(s3_path, tmp.name)
        # Read with cfgrib, filtering for 2m temperature only
        ds = xr.open_dataset(
            tmp.name,
            engine='cfgrib',
            backend_kwargs={
                'filter_by_keys': {
                    'shortName': '2t',  # 2m temperature
                    'typeOfLevel': 'heightAboveGround',
                    'level': 2,
                }
            }
        )
        print(ds)
        os.unlink(tmp.name)

    return ds


# -----------------------------------------------------------------------------
# EXAMPLE 4: Direct S3 access for reforecast data
# -----------------------------------------------------------------------------
def example_direct_s3_reforecast():
    """Access GEFS reforecast data directly from S3."""
    import s3fs
    import xarray as xr
    import tempfile
    import os

    fs = s3fs.S3FileSystem(anon=True)

    # List years available in reforecast
    years = fs.ls('noaa-gefs-retrospective/GEFSv12/reforecast/')
    print(f"Available years: {years}")

    # List dates in a given year
    dates_2019 = fs.ls('noaa-gefs-retrospective/GEFSv12/reforecast/2019/')
    print(f"Dates in 2019: {len(dates_2019)} entries")

    # Download 2m temperature for a specific date/member
    s3_path = (
        "noaa-gefs-retrospective/GEFSv12/reforecast/2019/"
        "2019071500/c00/Days:1-10/tmp_2m"
    )

    with tempfile.NamedTemporaryFile(suffix='.grib2', delete=False) as tmp:
        fs.get(s3_path, tmp.name)
        ds = xr.open_dataset(tmp.name, engine='cfgrib')
        print(ds)
        os.unlink(tmp.name)


# -----------------------------------------------------------------------------
# EXAMPLE 5: Extract data for specific cities from a GEFS grid
# -----------------------------------------------------------------------------
def example_extract_city_temperatures():
    """Extract 2m temperature for specific cities from GEFS ensemble."""
    from herbie import Herbie
    import xarray as xr
    import pandas as pd

    # 20 cities for Polymarket weather trading
    cities = {
        'New York':    {'lat': 40.78, 'lon': -73.97},
        'Los Angeles': {'lat': 33.94, 'lon': -118.39},
        'Chicago':     {'lat': 41.96, 'lon': -87.93},
        'Miami':       {'lat': 25.79, 'lon': -80.29},
        'London':      {'lat': 51.47, 'lon': -0.45},
        'Paris':       {'lat': 48.86, 'lon': 2.35},
        'Tokyo':       {'lat': 35.76, 'lon': 139.70},
        'Sydney':      {'lat': -33.95, 'lon': 151.17},
        'Toronto':     {'lat': 43.68, 'lon': -79.63},
        'Dubai':       {'lat': 25.25, 'lon': 55.36},
        'Singapore':   {'lat': 1.36, 'lon': 103.98},
        'Mumbai':      {'lat': 19.09, 'lon': 72.87},
        'Sao Paulo':   {'lat': -23.63, 'lon': -46.66},
        'Mexico City': {'lat': 19.44, 'lon': -99.07},
        'Berlin':      {'lat': 52.47, 'lon': 13.40},
        'Rome':        {'lat': 41.80, 'lon': 12.24},
        'Seoul':       {'lat': 37.57, 'lon': 126.98},
        'Bangkok':     {'lat': 13.92, 'lon': 100.61},
        'Cairo':       {'lat': 30.13, 'lon': 31.40},
        'Moscow':      {'lat': 55.97, 'lon': 37.41},
    }

    date = "2026-03-14 00:00"
    fxx = 24
    results = {}

    for member in range(0, 31):
        try:
            H = Herbie(date, model="gefs", product="atmos.5", member=member, fxx=fxx)
            ds = H.xarray("TMP:2 m above ground")

            member_temps = {}
            for city, coords in cities.items():
                # Use nearest-neighbor interpolation to extract point value
                t2m = ds['t2m'].sel(
                    latitude=coords['lat'],
                    longitude=coords['lon'] % 360,  # Convert to 0-360 if needed
                    method='nearest'
                )
                # Convert Kelvin to Celsius
                member_temps[city] = float(t2m.values) - 273.15

            results[f'member_{member:02d}'] = member_temps
        except Exception as e:
            print(f"Member {member}: {e}")

    df = pd.DataFrame(results).T
    print(df.describe())  # Shows mean, std, min, max across ensemble
    return df


# -----------------------------------------------------------------------------
# EXAMPLE 6: Byte-range download (most efficient for single variables)
# -----------------------------------------------------------------------------
def example_byte_range_download():
    """
    Download ONLY the 2m temperature bytes from a GEFS GRIB2 file.
    This uses the .idx index file to find the byte range for TMP:2 m,
    then downloads only those bytes. MUCH faster than downloading full file.
    """
    import requests
    import xarray as xr
    import tempfile
    import os

    base_url = (
        "https://noaa-gefs-pds.s3.amazonaws.com/"
        "gefs.20260314/00/atmos/pgrb2ap5/"
        "gec00.t00z.pgrb2a.0p50.f024"
    )

    # Step 1: Download the index file
    idx_url = base_url + ".idx"
    idx_response = requests.get(idx_url)
    idx_lines = idx_response.text.strip().split('\n')

    # Step 2: Find the byte range for 2m temperature
    start_byte = None
    end_byte = None
    for i, line in enumerate(idx_lines):
        if 'TMP' in line and '2 m above ground' in line:
            start_byte = int(line.split(':')[1])
            # End byte is the start of the next record
            if i + 1 < len(idx_lines):
                end_byte = int(idx_lines[i + 1].split(':')[1]) - 1
            break

    if start_byte is not None:
        # Step 3: Download only the TMP:2m bytes
        headers = {'Range': f'bytes={start_byte}-{end_byte}' if end_byte else f'bytes={start_byte}-'}
        data_response = requests.get(base_url, headers=headers)

        with tempfile.NamedTemporaryFile(suffix='.grib2', delete=False) as tmp:
            tmp.write(data_response.content)
            tmp_path = tmp.name

        ds = xr.open_dataset(tmp_path, engine='cfgrib')
        print(f"Downloaded {len(data_response.content) / 1024:.1f} KB instead of full file")
        print(ds)
        os.unlink(tmp_path)
        return ds


# =============================================================================
# 6. SUMMARY TABLE
# =============================================================================
#
# +-------------------+--------------------------------+------------------------------------+
# | Feature           | Operational (noaa-gefs-pds)    | Reforecast (noaa-gefs-retrospective)|
# +-------------------+--------------------------------+------------------------------------+
# | Time range        | 2017-01-01 to present          | 2000-01-01 to 2019-12-31           |
# | Format            | GRIB2                          | GRIB2                              |
# | Ensemble members  | 31 (1 ctrl + 30 pert)          | 5 daily / 11 weekly                |
# | Runs per day      | 4 (00Z, 06Z, 12Z, 18Z)        | 1 (00Z only)                       |
# | Forecast horizon  | 16 days (384 hours)            | 16 days (35 days weekly)           |
# | Resolution        | 0.25 or 0.50 degree            | 0.25 (d1-10) / 0.50 (d10-16)      |
# | Has 2m temp?      | YES                            | YES (tmp_2m)                       |
# | Authentication    | None (public)                  | None (public)                      |
# | .idx files?       | YES (byte-range possible)      | NO (download full variable file)   |
# | Python tool       | Herbie (model="gefs")          | Herbie (model="gefs_reforecast")   |
# +-------------------+--------------------------------+------------------------------------+
#
# NOTE ON MEMBER COUNTS:
#   - Pre-Sept 2020 (GEFSv11): 21 members (1 control + 20 perturbations)
#   - Post-Sept 2020 (GEFSv12): 31 members (1 control + 30 perturbations)
#   - Reforecast (GEFSv12): 5 members daily, 11 members weekly
#

# =============================================================================
# 7. KEY CONSIDERATIONS FOR POLYMARKET BACKTESTING
# =============================================================================
#
# FOR RECENT BACKTESTING (2020-present):
#   Use noaa-gefs-pds with all 31 ensemble members.
#   This is the same model version producing current forecasts.
#   Data available 4x daily, all ensemble members, 0.25 or 0.5 degree.
#   Use Herbie with model="gefs" and loop members 0-30.
#
# FOR LONG HISTORICAL BACKTESTING (2000-2019):
#   Use noaa-gefs-retrospective reforecast data.
#   Only 5 ensemble members (fewer than operational), but consistent model.
#   Use Herbie with model="gefs_reforecast" and variable_level="tmp_2m".
#
# FOR PRE-2017 BUT POST-2000:
#   ONLY the reforecast dataset covers this period.
#   The operational archive on AWS starts at 2017.
#
# RECOMMENDED APPROACH FOR BACKTESTING:
#   1. Use operational data (2020-present) for recent backtest with full ensemble
#   2. Use reforecast data (2000-2019) for longer historical analysis
#   3. Be aware the reforecast has fewer members (5 vs 31) and only 1 run/day
#   4. Use byte-range downloads with .idx files for efficiency (operational only)
#   5. For reforecast, each variable is its own file so downloads are already scoped
#
# DATA VOLUME ESTIMATES:
#   - One pgrb2ap5 file (all vars, one member, one forecast hour): ~15-30 MB
#   - Just 2m temperature extracted via byte range: ~0.5-1 MB
#   - One reforecast tmp_2m file (all forecast hours, one member): ~5-15 MB
#   - Full day, all members, all forecast hours, just 2m temp:
#       Operational: ~31 members * 65 forecast hours * ~0.5 MB = ~1 GB/day
#       Reforecast: ~5 members * 1 file each * ~10 MB = ~50 MB/day
#

# =============================================================================
# 8. ALSO AVAILABLE ON AZURE AND GOOGLE CLOUD
# =============================================================================
#
# Azure (via NODD / Planetary Computer):
#   Container: https://noaagefs.blob.core.windows.net/gefs
#   Path: gefs.YYYYMMDD/HH/atmos/pgrb2ap5/FILENAME
#   SAS token: https://planetarycomputer.microsoft.com/api/sas/v1/token/noaagefs/gefs
#
# Google Cloud: Also available via NODD but less documented.
#

if __name__ == "__main__":
    print("GEFS AWS S3 Research - Reference Script")
    print("=" * 50)
    print()
    print("Operational GEFS:  s3://noaa-gefs-pds  (2017-present)")
    print("GEFS Reforecast:   s3://noaa-gefs-retrospective  (2000-2019)")
    print()
    print("Run individual example functions to test access.")
    print("Recommended: pip install herbie-data xarray cfgrib s3fs")
