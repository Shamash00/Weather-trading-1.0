"""
Polymarket Weather Trading Bot - LIVE TRADING

Bot che piazza ordini reali su Polymarket basandosi sulle previsioni
meteo ensemble + mixture model calibrato.

Flusso:
1. Scarica mercati temperatura da Polymarket
2. Per ogni mercato: calcola probabilita (ensemble + mixture calibrato)
3. Confronta con odds Polymarket → calcola edge
4. Se edge > soglia → piazza ordine (BUY YES o BUY NO)

Uso:
    python bot_trading.py                     # avvia trading bot
    python bot_trading.py --dry-run           # simula senza piazzare ordini
    python bot_trading.py --once              # un solo ciclo e esci
    python bot_trading.py --min-edge 8        # edge minimo 8pp (default: 5)

Variabili d'ambiente richieste:
    POLY_API_KEY        API key Polymarket
    POLY_API_SECRET     API secret Polymarket
    POLY_PASSPHRASE     Passphrase Polymarket
    POLY_PRIVATE_KEY    Private key wallet Polygon
"""

import requests
import json
import time as time_mod
import logging
import argparse
import numpy as np
import os
import re
import sys
import traceback
from datetime import datetime, timezone, timedelta, date
from pathlib import Path
from scipy.stats import norm

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════════

CHECK_INTERVAL = 300        # Secondi tra ogni ciclo di trading (5 min)
DAYS_AHEAD = 2              # Considera mercati fino a N giorni avanti

# Soglie di trading
MIN_EDGE_PP = 5.0           # Edge minimo in percentage points per piazzare ordine
MAX_BET_USD = 20.0          # Puntata massima per singolo ordine ($)
MIN_BET_USD = 1.0           # Puntata minima
MAX_EXPOSURE_USD = 200.0    # Esposizione massima totale ($)
KELLY_FRACTION = 0.25       # Frazione di Kelly (conservativo: 1/4 Kelly)

# API endpoints
GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"
ENSEMBLE_API = "https://ensemble-api.open-meteo.com/v1/ensemble"
DETERMINISTIC_API = "https://api.open-meteo.com/v1/forecast"

BASE_DIR = Path(__file__).parent
OPTIMIZATION_FILE = BASE_DIR / "Ottimizzazione Ensemble Stagioni.xlsx"
TOP_MODELS_FILE_V2 = BASE_DIR / "Top Modelli Deterministici per Citta.xlsx"
MODEL_STATS_FILE = BASE_DIR / "model_error_stats.pkl"
STATE_FILE = BASE_DIR / "trading_state.json"

# ── Modelli ──────────────────────────────────────────────────────────────────

ALL_DETERMINISTIC_MODELS = [
    "best_match",
    "bom_access_global",
    "ecmwf_aifs025_single", "ecmwf_ifs025",
    "gem_global", "gem_regional",
    "gfs_global", "gfs_graphcast025", "gfs_hrrr",
    "icon_d2", "icon_eu", "icon_global",
    "jma_gsm",
    "kma_gdps",
    "knmi_harmonie_arome_europe", "knmi_harmonie_arome_netherlands",
    "dmi_harmonie_arome_europe",
    "meteofrance_arome_france", "meteofrance_arome_france_hd",
    "meteofrance_arpege_europe", "meteofrance_arpege_world",
    "metno_seamless",
    "ukmo_global_deterministic_10km", "ukmo_uk_deterministic_2km",
]

ENSEMBLE_MODELS = [
    "ecmwf_ifs025_ensemble",
    "ecmwf_aifs025_ensemble",
    "icon_global_eps",
    "icon_eu_eps",
    "icon_d2_eps",
    "ncep_gefs025",
    "ncep_gefs05",
    "ncep_aigefs025",
    "gem_global_ensemble",
    "bom_access_global_ensemble",
    "ukmo_global_ensemble_20km",
    "ukmo_uk_ensemble_2km",
]

# ── Coordinate stazioni + timezone ───────────────────────────────────────────

CITY_DATA = {
    "Ankara":       {"lat": 40.128082,  "lon": 32.995083,   "tz": "Europe/Istanbul"},
    "Atlanta":      {"lat": 33.62972,   "lon": -84.44224,   "tz": "America/New_York"},
    "Austin":       {"lat": 30.18311,   "lon": -97.67989,   "tz": "America/Chicago"},
    "Beijing":      {"lat": 40.080111,  "lon": 116.584556,  "tz": "Asia/Shanghai"},
    "Buenos Aires": {"lat": -34.822222, "lon": -58.535833,  "tz": "America/Argentina/Buenos_Aires"},
    "Chengdu":      {"lat": 30.578528,  "lon": 103.947086,  "tz": "Asia/Shanghai"},
    "Chicago":      {"lat": 41.96019,   "lon": -87.93162,   "tz": "America/Chicago"},
    "Chongqing":    {"lat": 29.719217,  "lon": 106.641678,  "tz": "Asia/Shanghai"},
    "Dallas":       {"lat": 32.8519,    "lon": -96.8555,    "tz": "America/Chicago"},
    "Denver":       {"lat": 39.84657,   "lon": -104.65623,  "tz": "America/Denver"},
    "Houston":      {"lat": 29.64586,   "lon": -95.28212,   "tz": "America/Chicago"},
    "London":       {"lat": 51.505278,  "lon": 0.055278,    "tz": "Europe/London"},
    "Lucknow":      {"lat": 26.7606,    "lon": 80.8893,     "tz": "Asia/Kolkata"},
    "Madrid":       {"lat": 40.493556,  "lon": -3.566764,   "tz": "Europe/Madrid"},
    "Miami":        {"lat": 25.78805,   "lon": -80.31694,   "tz": "America/New_York"},
    "Milan":        {"lat": 45.630606,  "lon": 8.728111,    "tz": "Europe/Rome"},
    "Milano":       {"lat": 45.630606,  "lon": 8.728111,    "tz": "Europe/Rome"},
    "Monaco":       {"lat": 48.353783,  "lon": 11.786086,   "tz": "Europe/Berlin"},
    "Munich":       {"lat": 48.353783,  "lon": 11.786086,   "tz": "Europe/Berlin"},
    "New York":     {"lat": 40.77945,   "lon": -73.88027,   "tz": "America/New_York"},
    "NYC":          {"lat": 40.77945,   "lon": -73.88027,   "tz": "America/New_York"},
    "Paris":        {"lat": 49.012779,  "lon": 2.55,        "tz": "Europe/Paris"},
    "San Francisco":{"lat": 37.61962,   "lon": -122.36562,  "tz": "America/Los_Angeles"},
    "Sao Paulo":    {"lat": -23.432075, "lon": -46.469511,  "tz": "America/Sao_Paulo"},
    "São Paulo":    {"lat": -23.432075, "lon": -46.469511,  "tz": "America/Sao_Paulo"},
    "Seattle":      {"lat": 47.4444,    "lon": -122.3138,   "tz": "America/Los_Angeles"},
    "Seoul":        {"lat": 37.469075,  "lon": 126.450517,  "tz": "Asia/Seoul"},
    "Shanghai":     {"lat": 31.143378,  "lon": 121.805214,  "tz": "Asia/Shanghai"},
    "Shenzhen":     {"lat": 22.639258,  "lon": 113.810664,  "tz": "Asia/Shanghai"},
    "Singapore":    {"lat": 1.350189,   "lon": 103.994433,  "tz": "Asia/Singapore"},
    "Taipei":       {"lat": 25.077731,  "lon": 121.232822,  "tz": "Asia/Taipei"},
    "Tel Aviv":     {"lat": 32.011389,  "lon": 34.886667,   "tz": "Asia/Jerusalem"},
    "Tokyo":        {"lat": 35.552258,  "lon": 139.779694,  "tz": "Asia/Tokyo"},
    "Toronto":      {"lat": 43.677223,  "lon": -79.630556,  "tz": "America/Toronto"},
    "Warsaw":       {"lat": 52.16575,   "lon": 20.967122,   "tz": "Europe/Warsaw"},
    "Wellington":   {"lat": -41.333333, "lon": 174.8,       "tz": "Pacific/Auckland"},
    "Wuhan":        {"lat": 30.783758,  "lon": 114.2081,    "tz": "Asia/Shanghai"},
}

CITY_NAME_TO_OPT = {
    "Ankara": "Ankara", "Atlanta": "Atlanta", "Austin": "Austin",
    "Beijing": "Beijing", "Buenos Aires": "BuenosAires",
    "Chengdu": "Chengdu", "Chicago": "Chicago", "Chongqing": "Chongqing",
    "Dallas": "Dallas", "Denver": "Denver", "Houston": "Houston",
    "London": "Londra", "Lucknow": "Lucknow", "Madrid": "Madrid",
    "Miami": "Miami", "Milan": "Milano", "Milano": "Milano",
    "Munich": "Monaco", "Monaco": "Monaco",
    "New York": "New York", "NYC": "New York",
    "Paris": "Parigi",
    "San Francisco": "SanFrancisco",
    "Sao Paulo": "SaoPaulo", "São Paulo": "SaoPaulo",
    "Seattle": "Seattle", "Seoul": "Seoul",
    "Shanghai": "Shanghai", "Shenzhen": "Shenzhen",
    "Singapore": "Singapore", "Taipei": "Taipei",
    "Tel Aviv": "TelAviv", "Tokyo": "Tokyo",
    "Toronto": "Toronto", "Warsaw": "Warsaw",
    "Wellington": "Wellington", "Wuhan": "Wuhan",
}

SOUTHERN_HEMISPHERE_CITIES = {"BuenosAires", "SaoPaulo", "Wellington"}
IGNORED_CITIES = {"Hong Kong", "Los Angeles"}


# ═══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════════════════════

def setup_logging():
    logger = logging.getLogger("trading")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    fh = logging.FileHandler(BASE_DIR / "trading.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger

log = setup_logging()


# ═══════════════════════════════════════════════════════════════════════════════
# STATE
# ═══════════════════════════════════════════════════════════════════════════════

def load_state() -> dict:
    if STATE_FILE.exists():
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"orders": [], "total_exposure": 0.0, "pnl": 0.0}


def save_state(state: dict):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False, default=str)


# ═══════════════════════════════════════════════════════════════════════════════
# UTILS
# ═══════════════════════════════════════════════════════════════════════════════

def get_season(month: int, opt_city: str) -> str:
    if opt_city in SOUTHERN_HEMISPHERE_CITIES:
        if month in (12, 1, 2): return "Estate"
        elif month in (3, 4, 5): return "Autunno"
        elif month in (6, 7, 8): return "Inverno"
        else: return "Primavera"
    else:
        if month in (12, 1, 2): return "Inverno"
        elif month in (3, 4, 5): return "Primavera"
        elif month in (6, 7, 8): return "Estate"
        else: return "Autunno"


def match_city(city_name: str) -> dict | None:
    if city_name in IGNORED_CITIES:
        return None
    if city_name in CITY_DATA:
        return CITY_DATA[city_name]
    for key, data in CITY_DATA.items():
        if key.lower() == city_name.lower():
            return data
    for key, data in CITY_DATA.items():
        if key.lower() in city_name.lower() or city_name.lower() in key.lower():
            return data
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIG LOADERS (stesse del bot.py)
# ═══════════════════════════════════════════════════════════════════════════════

_det_config_cache = None

def load_deterministic_config() -> dict:
    global _det_config_cache
    if _det_config_cache is not None:
        return _det_config_cache

    import openpyxl
    if not OPTIMIZATION_FILE.exists():
        log.warning(f"File ottimizzazione non trovato: {OPTIMIZATION_FILE}")
        _det_config_cache = {}
        return _det_config_cache

    wb = openpyxl.load_workbook(OPTIMIZATION_FILE, read_only=True, data_only=True)
    if "Riepilogo" not in wb.sheetnames:
        _det_config_cache = {}
        wb.close()
        return _det_config_cache

    ws = wb["Riepilogo"]
    config = {}
    for r in range(2, ws.max_row + 1):
        city = ws.cell(r, 1).value
        if not city:
            continue
        orizzonte = ws.cell(r, 4).value
        if orizzonte != "1GG":
            continue
        season = ws.cell(r, 3).value
        method = ws.cell(r, 5).value
        verde = ws.cell(r, 7).value or 0
        mae = ws.cell(r, 8).value or 1.0
        models_str = ws.cell(r, 13).value or ""
        models = [m.strip() for m in models_str.split(",")
                  if m.strip() and m.strip() != "cma_grapes_global"]
        config[(str(city).strip(), str(season).strip())] = {
            "method": method, "models": models, "corrections": {},
            "mae": float(mae), "verde": float(verde),
        }
    wb.close()
    _det_config_cache = config
    log.info(f"Config deterministici caricata: {len(config)} combinazioni")
    return config


_model_stats_cache = None

def load_model_stats() -> dict:
    global _model_stats_cache
    if _model_stats_cache is not None:
        return _model_stats_cache
    if not MODEL_STATS_FILE.exists():
        log.warning(f"Model stats non trovato: {MODEL_STATS_FILE}")
        _model_stats_cache = {}
        return _model_stats_cache
    import pickle
    with open(MODEL_STATS_FILE, "rb") as f:
        _model_stats_cache = pickle.load(f)
    log.info(f"Model stats caricati: {len(_model_stats_cache.get('stats', {}))} combinazioni")
    return _model_stats_cache


_top_models_cache = None

def load_top_models() -> dict:
    global _top_models_cache
    if _top_models_cache is not None:
        return _top_models_cache
    import openpyxl
    if not TOP_MODELS_FILE_V2.exists():
        _top_models_cache = {}
        return _top_models_cache

    wb = openpyxl.load_workbook(TOP_MODELS_FILE_V2, read_only=True, data_only=True)
    result = {}
    sheet_map = {}
    for sn in wb.sheetnames:
        if "Top 5" in sn and "1GG" in sn:
            sheet_map["top5"] = sn
        elif "Top 10" in sn and "1GG" in sn:
            sheet_map["top10"] = sn

    for key, sheet_name in sheet_map.items():
        ws = wb[sheet_name]
        for r in range(2, ws.max_row + 1):
            city = ws.cell(r, 1).value
            if not city:
                continue
            city = str(city).strip()
            if city not in result:
                result[city] = {"top5": [], "top10": []}
            models = []
            col = 2
            while True:
                model = ws.cell(r, col).value
                if not model:
                    break
                models.append(str(model).strip())
                col += 3
            result[city][key] = models
    wb.close()
    _top_models_cache = result
    log.info(f"Top modelli caricati: {len(result)} citta")
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# POLYMARKET - FETCH MERCATI
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_temp_events(closed: bool = False) -> list[dict]:
    all_events = []
    offset = 0
    try:
        while True:
            resp = requests.get(
                f"{GAMMA_API}/events",
                params={"tag_slug": "weather", "closed": str(closed).lower(),
                        "limit": 200, "offset": offset},
                timeout=20,
            )
            resp.raise_for_status()
            batch = resp.json()
            temp_events = [e for e in batch if "highest temperature" in e.get("title", "").lower()]
            all_events.extend(temp_events)
            if len(batch) < 200:
                break
            offset += 200
        return all_events
    except requests.RequestException as e:
        log.warning(f"Gamma API error: {e}")
        return all_events


def city_from_title(title: str) -> str:
    m = re.search(r"Highest temperature in (.+?) on ", title, re.IGNORECASE)
    return m.group(1).strip() if m else ""


def date_from_title(title: str, end_date_hint: str | None = None) -> str | None:
    m = re.search(r"on (\w+ \d+)", title, re.IGNORECASE)
    if not m:
        return None
    if end_date_hint:
        try:
            hint_year = int(end_date_hint[:4])
            dt = datetime.strptime(f"{m.group(1)} {hint_year}", "%B %d %Y")
            return dt.strftime("%Y-%m-%d")
        except (ValueError, IndexError):
            pass
    now = datetime.now()
    for year in [now.year, now.year + 1, now.year - 1]:
        try:
            dt = datetime.strptime(f"{m.group(1)} {year}", "%B %d %Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def parse_bucket(label: str) -> dict | None:
    lbl = label.lower()
    unit = "F" if re.search(r'[°ºo]F|°f|\d\s*F\b', label) else "C"
    is_lower = "or below" in lbl or "or lower" in lbl or "less than" in lbl
    is_upper = "or higher" in lbl or "or above" in lbl
    range_match = re.search(r'(\d+)\s*[-–]\s*(\d+)', label)
    nums = re.findall(r"-?\d+", label)
    if not nums:
        return None
    if is_lower:
        return {"unit": unit, "low": None, "high": int(nums[0]),
                "is_lower": True, "is_upper": False, "label": label}
    elif is_upper:
        return {"unit": unit, "low": int(nums[0]), "high": None,
                "is_lower": False, "is_upper": True, "label": label}
    elif range_match:
        return {"unit": unit, "low": int(range_match.group(1)),
                "high": int(range_match.group(2)),
                "is_lower": False, "is_upper": False, "label": label}
    else:
        val = int(nums[0])
        return {"unit": unit, "low": val, "high": val,
                "is_lower": False, "is_upper": False, "label": label}


def temp_sort_key(label: str) -> float:
    nums = re.findall(r"-?\d+\.?\d*", label)
    if not nums:
        return 0.0
    val = float(nums[0])
    lbl = label.lower()
    if "or higher" in lbl or "or above" in lbl:
        return val + 0.5
    if "or below" in lbl or "or lower" in lbl:
        return val - 0.5
    return val


def parse_markets_from_events(events: list[dict], days_ahead: int) -> list[dict]:
    now = datetime.now(timezone.utc)
    cutoff = now + timedelta(days=days_ahead)
    markets = {}

    for event in events:
        title = event.get("title", "")
        city = city_from_title(title)
        event_end = event.get("endDate", "")
        target_date = date_from_title(title, end_date_hint=event_end)
        if not city or not target_date:
            continue

        for m in event.get("markets") or []:
            end_str = m.get("endDate") or event_end
            end = None
            if end_str:
                try:
                    end = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                except ValueError:
                    pass
            if not end or end < now or end > cutoff:
                continue

            label = m.get("groupItemTitle") or m.get("question", "")
            prob_raw = m.get("lastTradePrice") or m.get("bestAsk") or 0
            try:
                prob = float(prob_raw)
            except (ValueError, TypeError):
                prob = 0.0

            parsed = parse_bucket(label)
            key = f"{city}_{target_date}"

            if key not in markets:
                markets[key] = {
                    "city": city, "target_date": target_date, "end_dt": end,
                    "event_slug": event.get("slug", ""),
                    "buckets": [],
                }

            markets[key]["buckets"].append({
                "label": label, "prob": prob, "parsed": parsed,
                "token_id": m.get("clobTokenIds"),
                "condition_id": m.get("conditionId"),
                "market_slug": m.get("slug", ""),
            })

    result = list(markets.values())
    for mkt in result:
        mkt["buckets"].sort(key=lambda b: temp_sort_key(b["label"]))
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# METEO - FETCH & CALCOLO PROBABILITA
# ═══════════════════════════════════════════════════════════════════════════════

def calc_bucket_prob_from_celsius(values_c: np.ndarray, bucket: dict) -> float:
    if len(values_c) == 0:
        return 0.0
    if bucket["unit"] == "F":
        values = values_c * 9.0 / 5.0 + 32.0
    else:
        values = values_c
    rounded = np.round(values).astype(int)
    total = len(rounded)
    if bucket["is_lower"]:
        return int(np.sum(rounded <= bucket["high"])) / total
    elif bucket["is_upper"]:
        return int(np.sum(rounded >= bucket["low"])) / total
    else:
        mask = (rounded >= bucket["low"]) & (rounded <= bucket["high"])
        return int(np.sum(mask)) / total


def fetch_ensemble_for_city(lat, lon, forecast_date) -> dict:
    all_members = {}
    for model in ENSEMBLE_MODELS:
        try:
            r = requests.get(ENSEMBLE_API, params={
                "latitude": lat, "longitude": lon,
                "daily": "temperature_2m_max", "models": model,
                "timezone": "auto",
                "start_date": forecast_date, "end_date": forecast_date,
            }, timeout=30)
            if r.status_code != 200:
                continue
            data = r.json()
            daily = data.get("daily", {})
            times = daily.get("time", [])
            if forecast_date not in times:
                continue
            idx = times.index(forecast_date)
            values = []
            if "temperature_2m_max" in daily:
                v = daily["temperature_2m_max"][idx]
                if v is not None:
                    values.append(v)
            for dkey in sorted(daily.keys()):
                if dkey.startswith("temperature_2m_max_member"):
                    v = daily[dkey][idx]
                    if v is not None:
                        values.append(v)
            if values:
                all_members[model] = values
            time_mod.sleep(0.3)
        except Exception:
            continue
    return all_members


def calc_ensemble_probs(all_members: dict, parsed_buckets: list[dict]) -> dict:
    if not all_members:
        return {}
    all_vals = []
    for vals in all_members.values():
        all_vals.extend(vals)
    arr = np.array(all_vals)
    probs = {}
    for b in parsed_buckets:
        if b is None:
            continue
        probs[b["label"]] = calc_bucket_prob_from_celsius(arr, b)
    return probs


def fetch_deterministic_for_city(lat, lon, forecast_date, models) -> dict:
    if not models:
        return {}
    results = {}
    batch_size = 10
    for i in range(0, len(models), batch_size):
        batch = models[i:i + batch_size]
        try:
            r = requests.get(DETERMINISTIC_API, params={
                "latitude": lat, "longitude": lon,
                "daily": "temperature_2m_max",
                "models": ",".join(batch),
                "start_date": forecast_date, "end_date": forecast_date,
                "timezone": "UTC",
            }, timeout=30)
            if r.status_code != 200:
                continue
            data = r.json()
            daily = data.get("daily", {})
            times = daily.get("time", [])
            if forecast_date not in times:
                continue
            idx = times.index(forecast_date)
            if len(batch) == 1:
                key = "temperature_2m_max"
                if key in daily and daily[key][idx] is not None:
                    results[batch[0]] = daily[key][idx]
            else:
                for model in batch:
                    key = f"temperature_2m_max_{model}"
                    if key in daily and daily[key][idx] is not None:
                        results[model] = daily[key][idx]
            time_mod.sleep(0.3)
        except Exception:
            continue
    return results


def mixture_bucket_probs(raw_forecasts: dict, city_opt: str, season: str,
                          parsed_buckets: list[dict]) -> dict | None:
    """Calcola probabilita per bucket usando mixture-of-normals calibrato."""
    model_stats_data = load_model_stats()
    if not model_stats_data or "stats" not in model_stats_data:
        return None

    all_stats = model_stats_data["stats"]
    iso_model = model_stats_data.get("iso_enhanced")

    if not parsed_buckets:
        return None

    unit = parsed_buckets[0]["unit"] if parsed_buckets[0] else "C"

    models_used = []
    for model_name, forecast_c in raw_forecasts.items():
        if forecast_c is None:
            continue
        key = (city_opt, model_name, season, "D1")
        if key not in all_stats:
            continue
        s = all_stats[key]
        models_used.append({
            "model": model_name, "forecast_c": forecast_c,
            "bias": s["bias"], "sigma": s["sigma"],
            "mae": s["mae"], "weight": s["weight"],
            "mu_c": forecast_c - s["bias"],
        })

    if len(models_used) < 3:
        return None

    total_w = sum(m["weight"] for m in models_used)
    for m in models_used:
        m["weight_norm"] = m["weight"] / total_w

    corrected_means = [m["mu_c"] for m in models_used]
    inter_model_spread = float(np.std(corrected_means))

    for m in models_used:
        m["sigma_enh"] = np.sqrt(m["sigma"]**2 + inter_model_spread**2)

    weighted_mean_c = sum(m["mu_c"] * m["weight_norm"] for m in models_used)

    probs = {}
    for b in parsed_buckets:
        if b is None:
            continue
        p = 0.0
        for m in models_used:
            if unit == "F":
                mu = m["mu_c"] * 9 / 5 + 32
                sigma = m["sigma_enh"] * 9 / 5
            else:
                mu = m["mu_c"]
                sigma = m["sigma_enh"]
            sigma = max(sigma, 0.3)
            w = m["weight_norm"]
            if b["is_lower"]:
                p += w * norm.cdf((b["high"] + 0.5 - mu) / sigma)
            elif b["is_upper"]:
                p += w * (1 - norm.cdf((b["low"] - 0.5 - mu) / sigma))
            else:
                p += w * (norm.cdf((b["high"] + 0.5 - mu) / sigma) -
                          norm.cdf((b["low"] - 0.5 - mu) / sigma))
        probs[b["label"]] = max(0.0, p)

    total = sum(probs.values())
    if total > 0:
        probs = {k: v / total for k, v in probs.items()}

    # Calibrazione isotonica
    if iso_model is not None:
        labels = list(probs.keys())
        raw_p = np.array([probs[l] for l in labels])
        cal_p = iso_model.predict(raw_p)
        cal_p = cal_p / cal_p.sum()
        probs = dict(zip(labels, cal_p))

    return {
        "probs": probs,
        "n_models": len(models_used),
        "spread": inter_model_spread,
        "mean_c": weighted_mean_c,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# CALCOLO PROBABILITA COMPLETO PER UN MERCATO
# ═══════════════════════════════════════════════════════════════════════════════

def compute_market_probs(city: str, target_date: str,
                          parsed_buckets: list[dict]) -> dict | None:
    """Calcola le probabilita per un mercato usando il mixture model."""
    opt_city = CITY_NAME_TO_OPT.get(city)
    if not opt_city:
        return None

    city_data = match_city(city)
    if not city_data:
        return None

    month = int(target_date.split("-")[1])
    season = get_season(month, opt_city)

    # Mixture model: scarica tutti i deterministici
    log.info(f"  {city} {target_date}: fetching {len(ALL_DETERMINISTIC_MODELS)} modelli...")
    raw = fetch_deterministic_for_city(
        city_data["lat"], city_data["lon"], target_date, ALL_DETERMINISTIC_MODELS)
    log.info(f"  {city} {target_date}: {len(raw)}/{len(ALL_DETERMINISTIC_MODELS)} ricevuti")

    if len(raw) < 3:
        log.warning(f"  {city} {target_date}: troppi pochi modelli, skip")
        return None

    result = mixture_bucket_probs(raw, opt_city, season, parsed_buckets)
    if result:
        log.info(f"  {city} {target_date}: mixture OK - {result['n_models']} modelli, "
                 f"spread={result['spread']:.2f}°C, media={result['mean_c']:.1f}°C")
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# POSITION SIZING (Kelly frazionario)
# ═══════════════════════════════════════════════════════════════════════════════

def kelly_bet_size(my_prob: float, market_prob: float, bankroll: float) -> float:
    """
    Calcola la dimensione della scommessa con Kelly frazionario.
    Per BUY YES: pago market_prob, vinco 1 con prob my_prob.
    """
    if market_prob <= 0 or market_prob >= 1 or my_prob <= 0:
        return 0.0

    # Odds decimali: quanto vinco per $1 puntato
    odds = (1.0 / market_prob) - 1.0  # profitto netto se vinco

    # Kelly: f = (p * odds - (1-p)) / odds
    edge = my_prob * odds - (1 - my_prob)
    if edge <= 0:
        return 0.0

    kelly_full = edge / odds
    bet = bankroll * kelly_full * KELLY_FRACTION

    return min(max(bet, 0), MAX_BET_USD)


# ═══════════════════════════════════════════════════════════════════════════════
# POLYMARKET CLOB - PIAZZAMENTO ORDINI
# ═══════════════════════════════════════════════════════════════════════════════

def init_clob_client():
    """Inizializza il client CLOB di Polymarket."""
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds

        api_key = os.environ.get("POLY_API_KEY", "")
        api_secret = os.environ.get("POLY_API_SECRET", "")
        passphrase = os.environ.get("POLY_PASSPHRASE", "")
        private_key = os.environ.get("POLY_PRIVATE_KEY", "")

        if not all([api_key, api_secret, passphrase, private_key]):
            log.warning("Credenziali Polymarket mancanti - ordini disabilitati")
            return None

        creds = ApiCreds(
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=passphrase,
        )

        client = ClobClient(
            CLOB_API,
            key=private_key,
            chain_id=137,  # Polygon mainnet
            creds=creds,
        )

        log.info("Client CLOB Polymarket inizializzato")
        return client
    except Exception as e:
        log.error(f"Errore init CLOB client: {e}")
        return None


def place_order(client, token_id: str, side: str, size_usd: float,
                price: float, dry_run: bool = True) -> dict | None:
    """
    Piazza un ordine su Polymarket.
    side: "BUY" o "SELL"
    price: prezzo YES (0-1)
    size_usd: quanto puntare in $
    """
    if dry_run:
        log.info(f"    [DRY-RUN] {side} ${size_usd:.2f} @ {price:.3f} (token: {token_id[:16]}...)")
        return {"dry_run": True, "side": side, "size": size_usd, "price": price}

    if client is None:
        log.warning("    Client CLOB non disponibile, ordine saltato")
        return None

    try:
        from py_clob_client.clob_types import OrderArgs
        from py_clob_client.order_builder.constants import BUY, SELL

        order_args = OrderArgs(
            price=price,
            size=size_usd,
            side=BUY if side == "BUY" else SELL,
            token_id=token_id,
        )

        signed_order = client.create_order(order_args)
        result = client.post_order(signed_order)

        log.info(f"    ORDINE PIAZZATO: {side} ${size_usd:.2f} @ {price:.3f}")
        return result

    except Exception as e:
        log.error(f"    Errore piazzamento ordine: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# TRADING LOGIC
# ═══════════════════════════════════════════════════════════════════════════════

def find_trades(market: dict, model_probs: dict, min_edge: float) -> list[dict]:
    """
    Trova opportunita di trading in un mercato.
    Ritorna lista di trade da eseguire.
    """
    trades = []

    for bucket in market["buckets"]:
        label = bucket["label"]
        pm_prob = bucket["prob"]
        my_prob = model_probs.get(label, 0.0)
        edge_pp = (my_prob - pm_prob) * 100

        if abs(edge_pp) < min_edge:
            continue

        if edge_pp > 0:
            # Il modello dice prob piu alta del mercato → BUY YES
            trades.append({
                "label": label,
                "side": "BUY",
                "target": "YES",
                "my_prob": my_prob,
                "market_prob": pm_prob,
                "edge_pp": edge_pp,
                "token_id": bucket.get("token_id"),
                "condition_id": bucket.get("condition_id"),
            })
        else:
            # Il modello dice prob piu bassa del mercato → BUY NO (= SELL YES)
            trades.append({
                "label": label,
                "side": "BUY",
                "target": "NO",
                "my_prob": 1 - my_prob,
                "market_prob": 1 - pm_prob,
                "edge_pp": abs(edge_pp),
                "token_id": bucket.get("token_id"),
                "condition_id": bucket.get("condition_id"),
            })

    # Ordina per edge decrescente
    trades.sort(key=lambda t: t["edge_pp"], reverse=True)
    return trades


def execute_trades(client, trades: list[dict], state: dict,
                   dry_run: bool = True) -> int:
    """Esegue i trade trovati, rispettando i limiti di rischio."""
    executed = 0
    current_exposure = state.get("total_exposure", 0.0)

    for trade in trades:
        if current_exposure >= MAX_EXPOSURE_USD:
            log.warning(f"  Esposizione massima raggiunta (${current_exposure:.2f}), stop")
            break

        # Token ID: Polymarket restituisce [yes_token, no_token] come JSON string
        token_ids = trade.get("token_id")
        if not token_ids:
            continue

        try:
            if isinstance(token_ids, str):
                token_ids = json.loads(token_ids)

            if trade["target"] == "YES":
                token_id = token_ids[0]
                price = trade["market_prob"]
            else:
                token_id = token_ids[1]
                price = trade["market_prob"]  # gia 1 - pm_prob per NO
        except (json.JSONDecodeError, IndexError, TypeError):
            log.warning(f"  Token ID non valido per {trade['label']}")
            continue

        # Position sizing
        remaining = MAX_EXPOSURE_USD - current_exposure
        bet_size = kelly_bet_size(trade["my_prob"], price, remaining)
        bet_size = min(bet_size, MAX_BET_USD, remaining)

        if bet_size < MIN_BET_USD:
            continue

        log.info(f"  >>> {trade['target']} {trade['label']}: "
                 f"mio={trade['my_prob']:.0%} mkt={price:.0%} "
                 f"edge={trade['edge_pp']:+.1f}pp bet=${bet_size:.2f}")

        result = place_order(client, token_id, "BUY", bet_size, price, dry_run)

        if result:
            current_exposure += bet_size
            executed += 1

            # Salva ordine nello state
            state.setdefault("orders", []).append({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "label": trade["label"],
                "target": trade["target"],
                "my_prob": round(trade["my_prob"], 4),
                "market_prob": round(price, 4),
                "edge_pp": round(trade["edge_pp"], 1),
                "bet_size": round(bet_size, 2),
                "dry_run": dry_run,
            })

    state["total_exposure"] = round(current_exposure, 2)
    return executed


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN LOOP
# ═══════════════════════════════════════════════════════════════════════════════

def run_cycle(state: dict, client, min_edge: float,
              days_ahead: int, dry_run: bool) -> int:
    """Esegue un ciclo completo di trading."""
    actions = 0

    log.info("Scarico mercati Polymarket...")
    events = fetch_temp_events(closed=False)
    if not events:
        log.info("  Nessun evento trovato")
        return 0

    markets = parse_markets_from_events(events, days_ahead)
    log.info(f"  {len(events)} eventi, {len(markets)} mercati attivi")

    for mkt in markets:
        city = mkt["city"]
        target_date = mkt["target_date"]

        if not match_city(city):
            continue

        parsed_buckets = [b.get("parsed") for b in mkt["buckets"] if b.get("parsed")]
        if not parsed_buckets:
            continue

        log.info(f"Analisi: {city} - {target_date} ({len(mkt['buckets'])} bucket)")

        # Calcola probabilita
        result = compute_market_probs(city, target_date, parsed_buckets)
        if not result:
            continue

        # Trova trade
        trades = find_trades(mkt, result["probs"], min_edge)
        if not trades:
            log.info(f"  Nessun edge > {min_edge}pp")
            continue

        log.info(f"  {len(trades)} opportunita trovate!")

        # Esegui trade
        n = execute_trades(client, trades, state, dry_run)
        actions += n

    save_state(state)
    return actions


def main():
    parser = argparse.ArgumentParser(description="Polymarket Weather Trading Bot")
    parser.add_argument("--dry-run", action="store_true", default=True,
                        help="Simula ordini senza piazzarli (default: True)")
    parser.add_argument("--live", action="store_true",
                        help="Piazza ordini reali (disattiva dry-run)")
    parser.add_argument("--once", action="store_true",
                        help="Esegui un solo ciclo e esci")
    parser.add_argument("--min-edge", type=float, default=MIN_EDGE_PP,
                        help=f"Edge minimo in pp (default: {MIN_EDGE_PP})")
    parser.add_argument("--max-bet", type=float, default=MAX_BET_USD,
                        help=f"Puntata massima $ (default: {MAX_BET_USD})")
    parser.add_argument("--days", type=int, default=DAYS_AHEAD,
                        help=f"Giorni avanti (default: {DAYS_AHEAD})")

    args = parser.parse_args()
    dry_run = not args.live

    global MAX_BET_USD
    MAX_BET_USD = args.max_bet

    mode = "DRY-RUN" if dry_run else "LIVE"
    log.info(f"{'='*60}")
    log.info(f"  POLYMARKET WEATHER TRADING BOT - {mode}")
    log.info(f"  Min edge: {args.min_edge}pp | Max bet: ${args.max_bet}")
    log.info(f"  Max exposure: ${MAX_EXPOSURE_USD} | Kelly: {KELLY_FRACTION}")
    log.info(f"{'='*60}")

    if not dry_run:
        log.warning(">>> MODALITA LIVE - ORDINI REALI <<<")

    # Init CLOB client
    client = None
    if not dry_run:
        client = init_clob_client()
        if client is None:
            log.error("Impossibile inizializzare CLOB client, esco")
            sys.exit(1)

    state = load_state()

    if args.once:
        run_cycle(state, client, args.min_edge, args.days, dry_run)
        return

    # Loop continuo
    while True:
        try:
            run_cycle(state, client, args.min_edge, args.days, dry_run)
        except Exception as e:
            log.error(f"Errore nel ciclo: {e}")
            log.debug(traceback.format_exc())

        log.info(f"Prossimo ciclo tra {CHECK_INTERVAL}s...")
        time_mod.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()
