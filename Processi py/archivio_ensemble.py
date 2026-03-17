"""
Archivio giornaliero delle previsioni ensemble.
Scarica i dati da Open-Meteo Ensemble API per tutte le citta' Polymarket
e li salva in un database SQLite per calibrazione futura.

Eseguire una volta al giorno (idealmente via Task Scheduler / cron).
"""

import requests
import sqlite3
import os
import time
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "ensemble_archive.db")

# Stesse citta' del tuo confronto_tutti_modelli.py
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
    "NewYork":     {"lat": 40.77945,  "lon": -73.88027},
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

# Modelli ensemble disponibili su Open-Meteo
# Globali (funzionano per tutte le citta')
ENSEMBLE_MODELS_GLOBAL = [
    "icon_seamless_eps",   # DWD, 40 membri
    "ecmwf_ifs025",        # ECMWF IFS, 51 membri
    "ecmwf_aifs025",       # ECMWF AIFS (AI), 51 membri
    "gfs_seamless",        # NOAA GFS, 31 membri
    "gem_global",          # Canada GEM, 21 membri
]

# Regionali (solo per citta' europee)
ENSEMBLE_MODELS_EUROPE = [
    "icon_eu_eps",         # DWD Europa, 40 membri
]

EUROPEAN_CITIES = {"Ankara", "Londra", "Monaco", "Parigi", "TelAviv"}

API_URL = "https://ensemble-api.open-meteo.com/v1/ensemble"


def init_db():
    """Crea il database e le tabelle se non esistono."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS ensemble_daily (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fetch_date TEXT NOT NULL,
            city TEXT NOT NULL,
            model TEXT NOT NULL,
            forecast_date TEXT NOT NULL,
            member_id INTEGER NOT NULL,
            temperature_max_c REAL,
            temperature_min_c REAL,
            UNIQUE(fetch_date, city, model, forecast_date, member_id)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS ensemble_hourly (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fetch_date TEXT NOT NULL,
            city TEXT NOT NULL,
            model TEXT NOT NULL,
            forecast_datetime TEXT NOT NULL,
            member_id INTEGER NOT NULL,
            temperature_2m_c REAL,
            UNIQUE(fetch_date, city, model, forecast_datetime, member_id)
        )
    """)

    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_daily_lookup
        ON ensemble_daily(city, model, forecast_date)
    """)

    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_hourly_lookup
        ON ensemble_hourly(city, model, forecast_datetime)
    """)

    conn.commit()
    return conn


def fetch_ensemble(lat, lon, model):
    """Scarica previsione ensemble per un modello e una posizione."""
    try:
        r = requests.get(API_URL, params={
            "latitude": lat,
            "longitude": lon,
            "daily": "temperature_2m_max,temperature_2m_min",
            "models": model,
            "timezone": "auto",
        }, timeout=60)

        if r.status_code == 429:
            print(f"    Rate limit! Attendo 60s...")
            time.sleep(60)
            r = requests.get(API_URL, params={
                "latitude": lat,
                "longitude": lon,
                "daily": "temperature_2m_max,temperature_2m_min",
                "models": model,
                "timezone": "auto",
            }, timeout=60)

        if r.status_code != 200:
            print(f"    HTTP {r.status_code}")
            return None

        return r.json()

    except Exception as e:
        print(f"    Errore: {e}")
        return None


def parse_and_store_daily(conn, fetch_date, city, model, data):
    """Parsa la risposta JSON e salva i dati daily nel DB."""
    if "daily" not in data:
        return 0

    daily = data["daily"]
    times = daily.get("time", [])
    if not times:
        return 0

    # Identifica le colonne dei membri per temperature_2m_max
    max_cols = {}  # member_id -> column_key
    min_cols = {}

    # Member 00 (control run) = campo senza suffisso
    if "temperature_2m_max" in daily:
        max_cols[0] = "temperature_2m_max"
    if "temperature_2m_min" in daily:
        min_cols[0] = "temperature_2m_min"

    # Members 01-99
    for key in daily:
        if key.startswith("temperature_2m_max_member"):
            m_id = int(key.replace("temperature_2m_max_member", ""))
            max_cols[m_id] = key
        elif key.startswith("temperature_2m_min_member"):
            m_id = int(key.replace("temperature_2m_min_member", ""))
            min_cols[m_id] = key

    rows = []
    for i, t in enumerate(times):
        all_member_ids = set(max_cols.keys()) | set(min_cols.keys())
        for m_id in sorted(all_member_ids):
            t_max = None
            t_min = None
            if m_id in max_cols:
                vals = daily[max_cols[m_id]]
                if i < len(vals):
                    t_max = vals[i]
            if m_id in min_cols:
                vals = daily[min_cols[m_id]]
                if i < len(vals):
                    t_min = vals[i]

            if t_max is not None or t_min is not None:
                rows.append((fetch_date, city, model, t, m_id, t_max, t_min))

    if rows:
        c = conn.cursor()
        c.executemany("""
            INSERT OR IGNORE INTO ensemble_daily
            (fetch_date, city, model, forecast_date, member_id,
             temperature_max_c, temperature_min_c)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, rows)
        conn.commit()

    return len(rows)


def main():
    fetch_date = datetime.now().strftime("%Y-%m-%d")
    print(f"=== Archivio Ensemble - {fetch_date} ===\n")

    conn = init_db()
    total_rows = 0

    for city, coords in CITIES.items():
        print(f"\n{city}:")

        models = ENSEMBLE_MODELS_GLOBAL[:]
        if city in EUROPEAN_CITIES:
            models += ENSEMBLE_MODELS_EUROPE

        for model in models:
            data = fetch_ensemble(coords["lat"], coords["lon"], model)

            if data is None:
                print(f"  {model}: FALLITO")
                continue

            n = parse_and_store_daily(conn, fetch_date, city, model, data)
            print(f"  {model}: {n} record salvati")
            total_rows += n

            time.sleep(2)  # rate limit

    conn.close()
    print(f"\n=== Totale: {total_rows} record salvati in {DB_PATH} ===")

    # Statistiche archivio
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM ensemble_daily")
    total = c.fetchone()[0]
    c.execute("SELECT MIN(fetch_date), MAX(fetch_date) FROM ensemble_daily")
    date_range = c.fetchone()
    c.execute("SELECT COUNT(DISTINCT fetch_date) FROM ensemble_daily")
    n_days = c.fetchone()[0]
    conn.close()

    print(f"\nArchivio totale: {total} record, {n_days} giorni")
    print(f"Range: {date_range[0]} -> {date_range[1]}")


if __name__ == "__main__":
    main()
