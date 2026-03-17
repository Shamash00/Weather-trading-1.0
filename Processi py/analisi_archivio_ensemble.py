"""
Analisi dell'archivio ensemble: confronta previsioni ensemble storiche
con temperature realmente osservate per calcolare bias, spread-skill,
e calibrare le probabilita' future.

Prerequisiti:
  - archivio_ensemble.py deve aver raccolto dati per almeno ~30 giorni
  - Le temperature storiche FM-15 devono essere disponibili
"""

import sqlite3
import pandas as pd
import numpy as np
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "ensemble_archive.db")
STORICO_PATH = os.path.join(
    BASE_DIR, "Temperature Storiche",
    "Temperature Storiche FM-15 Tutte le Citta 2021-2025 Daily Max.xlsx"
)


def load_ensemble_archive(city, model=None):
    """Carica dati ensemble dall'archivio per una citta'."""
    conn = sqlite3.connect(DB_PATH)

    query = """
        SELECT fetch_date, forecast_date, member_id, temperature_max_c
        FROM ensemble_daily
        WHERE city = ?
    """
    params = [city]

    if model:
        query += " AND model = ?"
        params.append(model)

    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df


def compute_ensemble_stats_per_day(df):
    """Per ogni (fetch_date, forecast_date), calcola statistiche ensemble."""
    stats = df.groupby(["fetch_date", "forecast_date"]).agg(
        n_members=("temperature_max_c", "count"),
        ens_mean=("temperature_max_c", "mean"),
        ens_median=("temperature_max_c", "median"),
        ens_std=("temperature_max_c", "std"),
        ens_min=("temperature_max_c", "min"),
        ens_max=("temperature_max_c", "max"),
        ens_p10=("temperature_max_c", lambda x: np.percentile(x.dropna(), 10)),
        ens_p25=("temperature_max_c", lambda x: np.percentile(x.dropna(), 25)),
        ens_p75=("temperature_max_c", lambda x: np.percentile(x.dropna(), 75)),
        ens_p90=("temperature_max_c", lambda x: np.percentile(x.dropna(), 90)),
    ).reset_index()

    # Lead time in giorni
    stats["fetch_date"] = pd.to_datetime(stats["fetch_date"])
    stats["forecast_date"] = pd.to_datetime(stats["forecast_date"])
    stats["lead_days"] = (stats["forecast_date"] - stats["fetch_date"]).dt.days

    return stats


def evaluate_vs_observations(stats_df, obs_df, temp_col="Max_Temperatura_F",
                             convert_f_to_c=True):
    """
    Confronta le previsioni ensemble con le osservazioni reali.
    Calcola bias, MAE, spread-skill ratio, CRPS.
    """
    obs_df = obs_df.copy()
    obs_df["Data"] = pd.to_datetime(obs_df["Data"])

    if convert_f_to_c:
        obs_df["obs_c"] = (obs_df[temp_col] - 32) * 5 / 9
    else:
        obs_df["obs_c"] = obs_df[temp_col]

    merged = stats_df.merge(
        obs_df[["Data", "obs_c"]],
        left_on="forecast_date",
        right_on="Data",
        how="inner"
    )

    if merged.empty:
        print("  Nessun match tra previsioni e osservazioni!")
        return None

    # Metriche
    merged["error"] = merged["ens_mean"] - merged["obs_c"]
    merged["abs_error"] = merged["error"].abs()
    merged["spread"] = merged["ens_std"]

    # Spread-skill ratio: spread dovrebbe essere ~ abs_error
    # Ratio ~1.0 = ben calibrato, <1.0 = sotto-dispersivo, >1.0 = sovra-dispersivo
    merged["spread_skill_ratio"] = merged["spread"] / merged["abs_error"].replace(0, np.nan)

    # Percentuale di osservazioni che cadono nell'intervallo P10-P90
    merged["in_p10_p90"] = (
        (merged["obs_c"] >= merged["ens_p10"]) &
        (merged["obs_c"] <= merged["ens_p90"])
    ).astype(int)

    # Percentuale nell'intervallo P25-P75
    merged["in_p25_p75"] = (
        (merged["obs_c"] >= merged["ens_p25"]) &
        (merged["obs_c"] <= merged["ens_p75"])
    ).astype(int)

    return merged


def print_calibration_report(merged, city, model):
    """Stampa report di calibrazione."""
    print(f"\n{'='*60}")
    print(f"  {city} - {model}")
    print(f"{'='*60}")

    for lead in sorted(merged["lead_days"].unique()):
        subset = merged[merged["lead_days"] == lead]
        if len(subset) < 5:
            continue

        bias = subset["error"].mean()
        mae = subset["abs_error"].mean()
        avg_spread = subset["spread"].mean()
        ssr = subset["spread_skill_ratio"].median()
        pct_in_80 = subset["in_p10_p90"].mean() * 100  # Dovrebbe essere ~80%
        pct_in_50 = subset["in_p25_p75"].mean() * 100  # Dovrebbe essere ~50%
        n = len(subset)

        print(f"\n  Lead +{lead} giorni ({n} campioni):")
        print(f"    Bias medio:           {bias:+.2f} C")
        print(f"    MAE:                  {mae:.2f} C")
        print(f"    Spread medio:         {avg_spread:.2f} C")
        print(f"    Spread/Skill ratio:   {ssr:.2f}  {'OK' if 0.8 <= ssr <= 1.2 else 'SOTTO-DISPERSIVO' if ssr < 0.8 else 'SOVRA-DISPERSIVO'}")
        print(f"    Obs in P10-P90:       {pct_in_80:.0f}%  (atteso: 80%)")
        print(f"    Obs in P25-P75:       {pct_in_50:.0f}%  (atteso: 50%)")

        # Suggerimento calibrazione
        if pct_in_80 < 70:
            inflation = 80 / max(pct_in_80, 1) - 1
            print(f"    >> SOTTO-DISPERSIVO: inflaziona lo spread del {inflation*100:.0f}%")
        elif pct_in_80 > 90:
            print(f"    >> SOVRA-DISPERSIVO: riduci lo spread")


def main():
    if not os.path.exists(DB_PATH):
        print(f"Database non trovato: {DB_PATH}")
        print("Esegui prima archivio_ensemble.py per raccogliere dati.")
        return

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Info archivio
    c.execute("SELECT COUNT(*) FROM ensemble_daily")
    total = c.fetchone()[0]
    c.execute("SELECT COUNT(DISTINCT fetch_date) FROM ensemble_daily")
    n_days = c.fetchone()[0]
    c.execute("SELECT COUNT(DISTINCT city) FROM ensemble_daily")
    n_cities = c.fetchone()[0]
    c.execute("SELECT COUNT(DISTINCT model) FROM ensemble_daily")
    n_models = c.fetchone()[0]

    print(f"Archivio: {total} record, {n_days} giorni, {n_cities} citta', {n_models} modelli\n")

    if n_days < 10:
        print("Servono almeno ~10 giorni di archivio per un'analisi significativa.")
        print("Continua ad eseguire archivio_ensemble.py quotidianamente.")
        conn.close()
        return

    # Carica osservazioni storiche
    if os.path.exists(STORICO_PATH):
        xl = pd.ExcelFile(STORICO_PATH)
    else:
        print(f"File storiche non trovato: {STORICO_PATH}")
        print("L'analisi verra' eseguita senza confronto con osservazioni.")
        xl = None

    # Analisi per citta'/modello
    c.execute("SELECT DISTINCT city FROM ensemble_daily")
    cities = [r[0] for r in c.fetchall()]

    c.execute("SELECT DISTINCT model FROM ensemble_daily")
    models = [r[0] for r in c.fetchall()]
    conn.close()

    for city in cities:
        for model in models:
            df = load_ensemble_archive(city, model)
            if df.empty:
                continue

            stats = compute_ensemble_stats_per_day(df)

            if xl and city in xl.sheet_names:
                obs = pd.read_excel(xl, sheet_name=city)
                merged = evaluate_vs_observations(stats, obs)
                if merged is not None and len(merged) > 0:
                    print_calibration_report(merged, city, model)
            else:
                # Solo statistiche ensemble senza osservazioni
                print(f"\n{city} - {model}: {len(stats)} previsioni archiviate (no obs per confronto)")


if __name__ == "__main__":
    main()
