"""
Scarica storico prezzi Polymarket per i mercati meteo "Highest temperature in [City]".

Per ogni citta e data, recupera:
- I mercati (un mercato per ogni bucket di temperatura, es. "11C", "12C", ...)
- Lo storico prezzi di ogni token "Yes" (= probabilita implicita del bucket)
- I dati di risoluzione (quale bucket ha vinto)

Salva tutto in:
  1) SQLite database (polymarket_storico.db) per query facili
  2) CSV giornaliero per analisi con pandas

API usate:
  - Gamma API /events         -> lista eventi meteo con mercati
  - Gamma API /public-search  -> ricerca eventi per keyword
  - CLOB API  /prices-history -> storico prezzi per token

Uso:
  python scarica_polymarket_storico.py                          # scarica ultime 2 settimane
  python scarica_polymarket_storico.py --days 60                # ultimi 60 giorni
  python scarica_polymarket_storico.py --start 2026-01-01       # da data specifica
  python scarica_polymarket_storico.py --city Londra            # solo una citta
  python scarica_polymarket_storico.py --update                 # aggiorna solo dati mancanti
"""

import requests
import sqlite3
import pandas as pd
import os
import sys
import time
import json
import re
import argparse
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "polymarket_storico.db")
CSV_DIR = os.path.join(BASE_DIR, "_polymarket_storico")
os.makedirs(CSV_DIR, exist_ok=True)

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"

# Mapping citta progetto -> keyword ricerca Polymarket
# I nomi nei mercati Polymarket sono in inglese
CITY_SEARCH_NAMES = {
    "Ankara":      "Ankara",
    "Atlanta":     "Atlanta",
    "BuenosAires": "Buenos Aires",
    "Chicago":     "Chicago",
    "Dallas":      "Dallas",
    "Londra":      "London",
    "Lucknow":     "Lucknow",
    "Miami":       "Miami",
    "Monaco":      "Munich",
    "New York":    "New York",
    "Parigi":      "Paris",
    "SaoPaulo":    "São Paulo",
    "Seattle":     "Seattle",
    "Seoul":       "Seoul",
    "Shanghai":    "Shanghai",
    "Singapore":   "Singapore",
    "TelAviv":     "Tel Aviv",
    "Tokyo":       "Tokyo",
    "Toronto":     "Toronto",
    "Wellington":  "Wellington",
}


# ── Database setup ──────────────────────────────────────────────────────────

def init_db():
    """Crea le tabelle se non esistono."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS events (
            event_id       INTEGER PRIMARY KEY,
            city           TEXT NOT NULL,
            city_local     TEXT NOT NULL,
            forecast_date  TEXT NOT NULL,
            slug           TEXT,
            volume         REAL,
            liquidity      REAL,
            closed         INTEGER,
            end_date       TEXT,
            fetched_at     TEXT NOT NULL,
            UNIQUE(city, forecast_date)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS markets (
            market_id      INTEGER PRIMARY KEY,
            event_id       INTEGER NOT NULL,
            question       TEXT,
            bucket_label   TEXT NOT NULL,
            bucket_temp_c  INTEGER,
            outcome_yes_token TEXT,
            outcome_no_token  TEXT,
            condition_id   TEXT,
            resolved       INTEGER DEFAULT 0,
            winner         TEXT,
            last_price_yes REAL,
            volume         REAL,
            FOREIGN KEY (event_id) REFERENCES events(event_id)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS price_history (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            market_id      INTEGER NOT NULL,
            token_id       TEXT NOT NULL,
            timestamp      INTEGER NOT NULL,
            price          REAL NOT NULL,
            FOREIGN KEY (market_id) REFERENCES markets(market_id),
            UNIQUE(market_id, timestamp)
        )
    """)

    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_events_city_date
        ON events(city, forecast_date)
    """)
    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_price_history_market
        ON price_history(market_id, timestamp)
    """)

    conn.commit()
    return conn


# ── Parsing helpers ─────────────────────────────────────────────────────────

def parse_bucket_label(question):
    """
    Estrae il bucket di temperatura dalla domanda Polymarket.
    Es: "Will the highest temperature in London be 14°C on March 17?"
        -> ("14C", 14)
    Es: "...be 8°C or below..." -> ("8C or below", 8)
    Es: "...be 18°C or higher..." -> ("18C or higher", 18)
    """
    q = question.lower()

    # Pattern: N°C or below / or lower
    m = re.search(r'(\d+)\s*°?\s*c\s+or\s+(below|lower)', q)
    if m:
        temp = int(m.group(1))
        return f"{temp}C or below", temp

    # Pattern: N°C or higher / or above
    m = re.search(r'(\d+)\s*°?\s*c\s+or\s+(higher|above)', q)
    if m:
        temp = int(m.group(1))
        return f"{temp}C or higher", temp

    # Pattern: N°C (exact)
    m = re.search(r'(\d+)\s*°?\s*c', q)
    if m:
        temp = int(m.group(1))
        return f"{temp}C", temp

    return question, None


def parse_forecast_date_from_slug(slug):
    """
    Estrae la data dal slug dell'evento.
    Es: "highest-temperature-in-london-on-march-17-2026" -> "2026-03-17"
    """
    months = {
        'january': 1, 'february': 2, 'march': 3, 'april': 4,
        'may': 5, 'june': 6, 'july': 7, 'august': 8,
        'september': 9, 'october': 10, 'november': 11, 'december': 12
    }
    pattern = r'on-(\w+)-(\d+)-(\d{4})'
    m = re.search(pattern, slug)
    if m:
        month_name = m.group(1).lower()
        day = int(m.group(2))
        year = int(m.group(3))
        month = months.get(month_name)
        if month:
            return f"{year}-{month:02d}-{day:02d}"
    return None


# ── API calls ───────────────────────────────────────────────────────────────

def search_weather_events(city_en, start_date=None, end_date=None, limit=100):
    """
    Cerca eventi "highest temperature in [city]" su Polymarket.
    Usa la Gamma API /events paginata, cercando sia chiusi che aperti.
    """
    events = []
    seen_ids = set()
    city_slug = city_en.lower().replace(" ", "-").replace("ã", "a")

    for closed_flag in ("true", "false"):
        offset = 0
        while len(events) < limit:
            params = {
                "limit": 50,
                "offset": offset,
                "order": "endDate",
                "ascending": "false",
                "closed": closed_flag,
            }
            if start_date:
                params["end_date_min"] = f"{start_date}T00:00:00Z"
            if end_date:
                params["end_date_max"] = f"{end_date}T23:59:59Z"

            try:
                r = requests.get(f"{GAMMA_API}/events", params=params, timeout=30)
                r.raise_for_status()
                data = r.json()
            except Exception as e:
                print(f"  Errore API events: {e}")
                break

            if not data:
                break

            for ev in data:
                slug = ev.get("slug", "")
                eid = ev.get("id")
                if f"highest-temperature-in-{city_slug}" in slug and eid not in seen_ids:
                    events.append(ev)
                    seen_ids.add(eid)

            offset += len(data)
            if len(data) < 50:
                break
            time.sleep(0.3)

    return events


def search_weather_events_by_search(city_en, limit=50):
    """
    Metodo alternativo: usa la public-search API per trovare eventi.
    Utile come fallback se il listing diretto non trova tutto.
    """
    try:
        params = {
            "q": f"highest temperature {city_en}",
            "limit_per_type": limit,
            "keep_closed_markets": 1,
        }
        r = requests.get(f"{GAMMA_API}/public-search", params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        return data.get("events", [])
    except Exception as e:
        print(f"  Errore public-search: {e}")
        return []


def search_weather_events_by_slug(city_en, start_date, end_date):
    """
    Metodo piu robusto: costruisce gli slug giorno per giorno e li cerca
    direttamente tramite /events?slug=...
    Slug pattern: highest-temperature-in-{city}-on-{month}-{day}-{year}
    """
    months_en = {
        1: 'january', 2: 'february', 3: 'march', 4: 'april',
        5: 'may', 6: 'june', 7: 'july', 8: 'august',
        9: 'september', 10: 'october', 11: 'november', 12: 'december'
    }

    city_slug = city_en.lower().replace(" ", "-").replace("ã", "a")
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    events = []
    current = start

    while current <= end:
        month_name = months_en[current.month]
        slug = f"highest-temperature-in-{city_slug}-on-{month_name}-{current.day}-{current.year}"

        try:
            r = requests.get(f"{GAMMA_API}/events", params={"slug": slug}, timeout=30)
            r.raise_for_status()
            data = r.json()
            if data:
                events.extend(data)
        except Exception as e:
            print(f"  Errore slug lookup {slug}: {e}")

        current += timedelta(days=1)
        time.sleep(0.3)

    return events


def fetch_event_markets(event_id):
    """
    Recupera tutti i mercati (bucket) di un evento.
    Ogni evento temperatura ha ~10-15 mercati (uno per bucket °C).
    """
    try:
        r = requests.get(f"{GAMMA_API}/events", params={"id": event_id}, timeout=30)
        r.raise_for_status()
        data = r.json()
        if data and len(data) > 0:
            return data[0].get("markets", [])
    except Exception as e:
        print(f"  Errore fetch mercati evento {event_id}: {e}")
    return []


def fetch_price_history(token_id, start_ts=None, end_ts=None, interval="max"):
    """
    Scarica lo storico prezzi per un token dalla CLOB API.

    Params:
        token_id: ID del token Yes (= probabilita implicita del bucket)
        start_ts: Unix timestamp inizio
        end_ts: Unix timestamp fine
        interval: "max", "all", "1d", "1h", "6h", "1m", "1w"
                  "max" restituisce la massima granularita disponibile

    Returns: lista di {t: timestamp, p: price}
    """
    params = {
        "market": token_id,
        "interval": interval,
    }
    if start_ts:
        params["startTs"] = int(start_ts)
    if end_ts:
        params["endTs"] = int(end_ts)

    try:
        r = requests.get(f"{CLOB_API}/prices-history", params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        return data.get("history", [])
    except Exception as e:
        print(f"  Errore price history per token {token_id[:20]}...: {e}")
        return []


# ── Main scraper logic ──────────────────────────────────────────────────────

def process_event(conn, event, city_local, city_en, skip_prices=False):
    """
    Processa un singolo evento: salva evento, mercati, e storico prezzi.
    """
    c = conn.cursor()

    event_id = event["id"]
    slug = event.get("slug", "")
    forecast_date = parse_forecast_date_from_slug(slug)

    if not forecast_date:
        print(f"    Impossibile estrarre data da slug: {slug}")
        return False

    # Controlla se gia' presente
    c.execute("SELECT event_id FROM events WHERE event_id = ?", (event_id,))
    if c.fetchone():
        return False  # gia' scaricato

    volume = float(event.get("volume", 0) or 0)
    liquidity = float(event.get("liquidity", 0) or 0)
    closed = 1 if event.get("closed", False) else 0
    end_date = event.get("endDate", "")

    c.execute("""
        INSERT OR IGNORE INTO events
        (event_id, city, city_local, forecast_date, slug, volume, liquidity, closed, end_date, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (event_id, city_en, city_local, forecast_date, slug, volume, liquidity,
          closed, end_date, datetime.now().isoformat()))

    # Recupera mercati
    markets = event.get("markets", [])
    if not markets:
        markets = fetch_event_markets(event_id)
        time.sleep(0.3)

    n_prices = 0

    for mkt in markets:
        market_id = mkt["id"]
        question = mkt.get("question", "")
        bucket_label, bucket_temp = parse_bucket_label(question)

        # Estrai token IDs
        clob_tokens = json.loads(mkt.get("clobTokenIds", "[]")) if isinstance(mkt.get("clobTokenIds"), str) else mkt.get("clobTokenIds", [])
        outcomes = json.loads(mkt.get("outcomes", "[]")) if isinstance(mkt.get("outcomes"), str) else mkt.get("outcomes", [])
        outcome_prices = json.loads(mkt.get("outcomePrices", "[]")) if isinstance(mkt.get("outcomePrices"), str) else mkt.get("outcomePrices", [])

        yes_token = clob_tokens[0] if len(clob_tokens) > 0 else ""
        no_token = clob_tokens[1] if len(clob_tokens) > 1 else ""
        condition_id = mkt.get("conditionId", "")

        # Determina se risolto e chi ha vinto
        resolved = 1 if mkt.get("closed", False) and mkt.get("resolutionSource", "") else 0
        winner = ""
        if outcome_prices:
            try:
                prices = [float(p) for p in outcome_prices]
                if prices[0] > 0.95:
                    winner = "Yes"
                    resolved = 1
                elif prices[0] < 0.05 and len(prices) > 1 and prices[1] > 0.95:
                    winner = "No"
                    resolved = 1
            except (ValueError, IndexError):
                pass

        last_price_yes = float(outcome_prices[0]) if outcome_prices else None
        mkt_volume = float(mkt.get("volume", 0) or 0)

        c.execute("""
            INSERT OR IGNORE INTO markets
            (market_id, event_id, question, bucket_label, bucket_temp_c,
             outcome_yes_token, outcome_no_token, condition_id,
             resolved, winner, last_price_yes, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (market_id, event_id, question, bucket_label, bucket_temp,
              yes_token, no_token, condition_id, resolved, winner,
              last_price_yes, mkt_volume))

        # Scarica storico prezzi del token Yes
        if yes_token and not skip_prices:
            history = fetch_price_history(yes_token, interval="max")
            for point in history:
                try:
                    c.execute("""
                        INSERT OR IGNORE INTO price_history
                        (market_id, token_id, timestamp, price)
                        VALUES (?, ?, ?, ?)
                    """, (market_id, yes_token, int(point["t"]), float(point["p"])))
                    n_prices += 1
                except (KeyError, ValueError):
                    pass
            time.sleep(0.5)  # Rate limiting

    conn.commit()
    print(f"    {forecast_date}: {len(markets)} bucket, {n_prices} prezzi storici")
    return True


def export_csv(conn, city_local, forecast_date=None):
    """
    Esporta i dati in CSV per facile analisi con pandas.
    Un file per citta con tutte le date.
    """
    query = """
        SELECT
            e.city_local,
            e.city,
            e.forecast_date,
            m.bucket_label,
            m.bucket_temp_c,
            m.resolved,
            m.winner,
            m.last_price_yes,
            m.volume as market_volume,
            e.volume as event_volume
        FROM events e
        JOIN markets m ON m.event_id = e.event_id
        WHERE e.city_local = ?
        ORDER BY e.forecast_date, m.bucket_temp_c
    """
    df = pd.read_sql_query(query, conn, params=(city_local,))

    if df.empty:
        return

    csv_path = os.path.join(CSV_DIR, f"polymarket_{city_local}.csv")
    df.to_csv(csv_path, index=False)
    print(f"  CSV: {csv_path} ({len(df)} righe)")


def export_price_timeseries(conn, city_local):
    """
    Esporta le serie temporali dei prezzi per analisi dettagliata.
    """
    query = """
        SELECT
            e.city_local,
            e.forecast_date,
            m.bucket_label,
            m.bucket_temp_c,
            ph.timestamp,
            ph.price,
            m.winner
        FROM price_history ph
        JOIN markets m ON m.market_id = ph.market_id
        JOIN events e ON e.event_id = m.event_id
        WHERE e.city_local = ?
        ORDER BY e.forecast_date, m.bucket_temp_c, ph.timestamp
    """
    df = pd.read_sql_query(query, conn, params=(city_local,))

    if df.empty:
        return

    csv_path = os.path.join(CSV_DIR, f"polymarket_prezzi_{city_local}.csv")
    df.to_csv(csv_path, index=False)
    print(f"  Prezzi CSV: {csv_path} ({len(df)} righe)")


# ── Summary & stats ─────────────────────────────────────────────────────────

def print_summary(conn):
    """Stampa un riepilogo dei dati scaricati."""
    c = conn.cursor()

    print("\n" + "=" * 70)
    print("RIEPILOGO DATI POLYMARKET SCARICATI")
    print("=" * 70)

    c.execute("SELECT COUNT(DISTINCT event_id) FROM events")
    n_events = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM markets")
    n_markets = c.fetchone()[0]

    c.execute("SELECT COUNT(*) FROM price_history")
    n_prices = c.fetchone()[0]

    c.execute("SELECT COUNT(DISTINCT event_id) FROM events WHERE closed = 1")
    n_resolved = c.fetchone()[0]

    print(f"\n  Eventi totali:    {n_events}")
    print(f"  Mercati (bucket): {n_markets}")
    print(f"  Prezzi storici:   {n_prices}")
    print(f"  Eventi risolti:   {n_resolved}")

    print(f"\n  {'Citta':<16} {'Eventi':>7} {'Risolti':>8} {'Date':>25}")
    print(f"  {'-'*16} {'-'*7} {'-'*8} {'-'*25}")

    c.execute("""
        SELECT city_local,
               COUNT(DISTINCT event_id) as n,
               SUM(closed) as resolved,
               MIN(forecast_date) as min_date,
               MAX(forecast_date) as max_date
        FROM events
        GROUP BY city_local
        ORDER BY city_local
    """)

    for row in c.fetchall():
        city, n, resolved, min_d, max_d = row
        print(f"  {city:<16} {n:>7} {resolved:>8} {min_d} -> {max_d}")

    print()


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Scarica storico prezzi Polymarket per mercati meteo"
    )
    parser.add_argument("--days", type=int, default=14,
                        help="Numero di giorni indietro da scaricare (default: 14)")
    parser.add_argument("--start", type=str, default=None,
                        help="Data inizio (YYYY-MM-DD), sovrascrive --days")
    parser.add_argument("--end", type=str, default=None,
                        help="Data fine (YYYY-MM-DD), default: oggi")
    parser.add_argument("--city", type=str, default=None,
                        help="Scarica solo una citta (nome locale, es: Londra)")
    parser.add_argument("--update", action="store_true",
                        help="Scarica solo eventi non ancora presenti nel DB")
    parser.add_argument("--search-method", choices=["slug", "events", "search", "all"],
                        default="slug",
                        help="Metodo di ricerca: slug (diretto per data, piu affidabile), "
                             "events (listing paginato), search (public-search), "
                             "all (tutti e 3)")
    parser.add_argument("--no-prices", action="store_true",
                        help="Non scaricare lo storico prezzi (solo mercati)")
    args = parser.parse_args()

    # Date range
    end_date = args.end or datetime.now(tz=None).strftime("%Y-%m-%d")
    if args.start:
        start_date = args.start
    else:
        start_date = (datetime.now(tz=None) - timedelta(days=args.days)).strftime("%Y-%m-%d")

    print(f"=== Scarica storico Polymarket meteo ===")
    print(f"  Periodo: {start_date} -> {end_date}")

    # Filtra citta
    if args.city:
        if args.city not in CITY_SEARCH_NAMES:
            print(f"  Citta '{args.city}' non trovata. Disponibili:")
            for c in sorted(CITY_SEARCH_NAMES.keys()):
                print(f"    - {c}")
            sys.exit(1)
        cities = {args.city: CITY_SEARCH_NAMES[args.city]}
    else:
        cities = CITY_SEARCH_NAMES

    conn = init_db()
    total_new = 0

    for city_local, city_en in sorted(cities.items()):
        print(f"\n--- {city_local} ({city_en}) ---")

        events = []
        seen_ids = set()

        def merge_events(new_events):
            for ev in new_events:
                eid = ev.get("id")
                if eid and eid not in seen_ids:
                    slug = ev.get("slug", "")
                    fd = parse_forecast_date_from_slug(slug)
                    if fd and start_date <= fd <= end_date:
                        events.append(ev)
                        seen_ids.add(eid)

        # Metodo primario: lookup diretto per slug (piu affidabile)
        if args.search_method in ("slug", "all"):
            ev_slug = search_weather_events_by_slug(city_en, start_date, end_date)
            merge_events(ev_slug)
            print(f"  Trovati {len(ev_slug)} eventi via slug lookup")

        # Metodo 2: listing paginato
        if args.search_method in ("events", "all"):
            ev1 = search_weather_events(city_en, start_date, end_date)
            before = len(events)
            merge_events(ev1)
            print(f"  +{len(events) - before} eventi via events API")
            time.sleep(0.3)

        # Metodo 3: public-search
        if args.search_method in ("search", "all"):
            ev2 = search_weather_events_by_search(city_en)
            before = len(events)
            merge_events(ev2)
            print(f"  +{len(events) - before} eventi via public-search")
            time.sleep(0.3)

        print(f"  Totale eventi: {len(events)}")

        if not events:
            print("  Nessun evento trovato")
            continue

        # Processa ogni evento
        for ev in events:
            new = process_event(conn, ev, city_local, city_en,
                                skip_prices=args.no_prices)
            if new:
                total_new += 1

        # Esporta CSV
        export_csv(conn, city_local)
        if not args.no_prices:
            export_price_timeseries(conn, city_local)

    # Riepilogo
    print_summary(conn)
    print(f"Nuovi eventi scaricati: {total_new}")
    print(f"Database: {DB_PATH}")

    conn.close()


if __name__ == "__main__":
    main()
