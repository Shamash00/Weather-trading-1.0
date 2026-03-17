"""
Scarica storico GEFS ensemble da AWS S3 per backtesting.

Per ogni data: scarica TMAX:2m (temperatura massima in finestre da 6 ore)
per tutti i 31 membri ensemble, estrae i valori alle 20 citta,
e calcola la daily max per "domani" (1GG) per ogni membro.

Dati: noaa-gefs-pds bucket su AWS (pubblico, gratuito)
Formato: GRIB2, scaricato via byte-range (solo TMAX:2m ~0.5MB per file)

Uso: python scarica_gefs_storico.py [--start 2021-04-05] [--end 2025-08-24]
"""

import pandas as pd
import numpy as np
import os
import sys
import pickle
import shutil
import warnings
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.filterwarnings("ignore")
os.environ["PYTHONIOENCODING"] = "utf-8"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(BASE_DIR, "_gefs_storico")
GRIB_CACHE = os.path.join(BASE_DIR, "_gefs_cache")
os.makedirs(CACHE_DIR, exist_ok=True)

CITIES = {
    "Ankara":      {"lat": 40.128082, "lon": 32.995083},
    "Atlanta":     {"lat": 33.62972,  "lon": -84.44224},
    "BuenosAires": {"lat": -34.822222,"lon": -58.535833},
    "Chicago":     {"lat": 41.96019,  "lon": -87.93162},
    "Dallas":      {"lat": 32.8519,   "lon": -96.8555},
    "Londra":      {"lat": 51.505278, "lon": 0.055278},
    "Lucknow":     {"lat": 26.7606,   "lon": 80.8893},
    "Miami":       {"lat": 25.78805,  "lon": -80.31694},
    "Monaco":      {"lat": 48.353783, "lon": 11.786086},
    "New York":    {"lat": 40.77945,  "lon": -73.88027},
    "Parigi":      {"lat": 49.012779, "lon": 2.55},
    "SaoPaulo":    {"lat": -23.432075,"lon": -46.469511},
    "Seattle":     {"lat": 47.4444,   "lon": -122.3138},
    "Seoul":       {"lat": 37.469075, "lon": 126.450517},
    "Shanghai":    {"lat": 31.143378, "lon": 121.805214},
    "Singapore":   {"lat": 1.350189,  "lon": 103.994433},
    "TelAviv":     {"lat": 32.011389, "lon": 34.886667},
    "Tokyo":       {"lat": 35.552258, "lon": 139.779694},
    "Toronto":     {"lat": 43.677223, "lon": -79.630556},
    "Wellington":  {"lat": -41.3333333,"lon": 174.8},
}

# GEFS uses 0-360 longitude
CITIES_GEFS = {}
for name, c in CITIES.items():
    lon_360 = c["lon"] if c["lon"] >= 0 else c["lon"] + 360
    CITIES_GEFS[name] = {"lat": c["lat"], "lon": lon_360}

# Forecast hours for 1GG daily max (00Z run: hours 24-48 cover "tomorrow")
FXX_1GG = [24, 30, 36, 42, 48]

# Members: control (0) + perturbations (1-30)
N_MEMBERS = 31

# Parallel workers for downloading
N_WORKERS = 6


def download_grib(date_str, member, fxx):
    """Download GRIB2 file (HTTP only, no decoding). Returns Herbie object or None."""
    from herbie import Herbie

    try:
        H = Herbie(date_str, model="gefs", fxx=fxx, member=member,
                   product="atmos.5", verbose=False)
        H.download(":TMAX:2 m above ground", verbose=False)
        return member, fxx, H, None
    except Exception as e:
        return member, fxx, None, str(e)


def decode_and_extract(H):
    """Decode a downloaded GRIB2 and extract city values. Must run sequentially."""
    try:
        ds = H.xarray(":TMAX:2 m above ground", verbose=False)
        vals = {}
        for city, coords in CITIES_GEFS.items():
            try:
                val = ds["tmax"].sel(
                    latitude=coords["lat"],
                    longitude=coords["lon"],
                    method="nearest"
                )
                temp_k = float(val)
                temp_f = (temp_k - 273.15) * 9 / 5 + 32
                vals[city] = round(temp_f, 1)
            except Exception:
                pass
        return vals
    except Exception:
        return {}


def process_date(date_str):
    """Download and process all GEFS members for a single date.

    Step 1: Download GRIB2 files in parallel (HTTP is thread-safe)
    Step 2: Decode GRIB2 sequentially (eccodes is NOT thread-safe on Windows)
    """
    output_file = os.path.join(CACHE_DIR, f"{date_str}.pkl")
    if os.path.exists(output_file):
        return "cached"

    # Build all (member, fxx) jobs
    jobs = [(date_str, m, f) for m in range(N_MEMBERS) for f in FXX_1GG]

    # Step 1: Parallel download (HTTP only)
    downloaded = []  # (member, fxx, Herbie_obj)
    errors = 0

    with ThreadPoolExecutor(max_workers=N_WORKERS) as pool:
        futures = {pool.submit(download_grib, *j): j for j in jobs}
        for future in as_completed(futures):
            member, fxx, H, err = future.result()
            if err or H is None:
                errors += 1
            else:
                downloaded.append((member, fxx, H))

    # Step 2: Sequential GRIB2 decode + city extraction
    city_member_vals = {city: {m: [] for m in range(N_MEMBERS)} for city in CITIES}

    for member, fxx, H in downloaded:
        vals = decode_and_extract(H)
        if not vals:
            errors += 1
        for city, v in vals.items():
            city_member_vals[city][member].append(v)

    # Build result: daily max per member per city
    result = {
        "date": date_str,
        "forecast_date": (datetime.strptime(date_str, "%Y-%m-%d")
                          + timedelta(days=1)).strftime("%Y-%m-%d"),
        "n_members": N_MEMBERS,
        "fxx_hours": FXX_1GG,
        "cities": {},
    }

    for city in CITIES:
        members_max = {}
        for m in range(N_MEMBERS):
            if city_member_vals[city][m]:
                members_max[m] = max(city_member_vals[city][m])

        if members_max:
            vals = list(members_max.values())
            result["cities"][city] = {
                "members_f": vals,
                "mean_f": round(np.mean(vals), 1),
                "std_f": round(np.std(vals), 1),
                "n_members": len(vals),
            }

    with open(output_file, "wb") as f:
        pickle.dump(result, f)

    # Clean up GRIB cache for this date to save disk space (~30MB/day)
    date_cache = os.path.join(GRIB_CACHE, "gefs", date_str.replace("-", ""))
    if os.path.exists(date_cache):
        shutil.rmtree(date_cache, ignore_errors=True)

    return f"ok ({errors} err, {len(result['cities'])} citta)"


# ── Main ──
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=str, default="2021-04-05")
    parser.add_argument("--end", type=str, default="2025-08-24")
    args = parser.parse_args()

    start = datetime.strptime(args.start, "%Y-%m-%d").date()
    end = datetime.strptime(args.end, "%Y-%m-%d").date()

    dates = []
    d = start
    while d <= end:
        dates.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)

    # Check which dates are already cached
    cached = [d for d in dates if os.path.exists(os.path.join(CACHE_DIR, f"{d}.pkl"))]
    to_do = [d for d in dates if d not in cached]

    print(f"GEFS Storico Ensemble Downloader (parallelo, {N_WORKERS} workers)")
    print(f"Periodo: {start} -> {end} ({len(dates)} giorni)")
    print(f"Gia scaricati: {len(cached)}")
    print(f"Da scaricare: {len(to_do)}")
    print(f"Downloads per giorno: {N_MEMBERS} x {len(FXX_1GG)} = {N_MEMBERS * len(FXX_1GG)}")
    est_min_per_day = 1.7
    print(f"Stima tempo: ~{len(to_do) * est_min_per_day / 60:.0f} ore "
          f"(~{est_min_per_day:.1f} min/giorno con {N_WORKERS} workers)")
    print(f"Output: {CACHE_DIR}")
    print(f"Lo script e' resumabile: rieseguilo per completare date mancanti.")
    print()

    if not to_do:
        print("Nessun dato da scaricare!")
        sys.exit(0)

    start_time = datetime.now()

    for i, date_str in enumerate(to_do):
        elapsed = (datetime.now() - start_time).total_seconds()
        if i > 0:
            rate = elapsed / i
            remaining = rate * (len(to_do) - i)
            eta = f"~{remaining / 60:.0f} min"
        else:
            eta = "..."

        print(f"[{i + 1}/{len(to_do)}] {date_str}  (ETA: {eta})", end="  ", flush=True)

        try:
            status = process_date(date_str)
            print(status, flush=True)
        except Exception as e:
            print(f"ERRORE: {e}", flush=True)

    elapsed_total = (datetime.now() - start_time).total_seconds()
    print(f"\nCompletato in {elapsed_total / 60:.1f} minuti")
    print(f"File salvati in: {CACHE_DIR}")

    n_files = len([f for f in os.listdir(CACHE_DIR) if f.endswith(".pkl")])
    print(f"Totale giorni nello storico: {n_files}")
