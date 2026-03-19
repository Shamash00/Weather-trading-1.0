"""
Precompute Model Error Statistics
==================================
Genera model_error_stats.pkl con bias, sigma, MAE per modello × città × stagione,
più i modelli di calibrazione isotonica. Usato dal bot per il mixture model.

Eseguire periodicamente (settimanale/mensile) per aggiornare le statistiche.
"""

import pickle
import numpy as np
import pandas as pd
from scipy.stats import norm
from sklearn.isotonic import IsotonicRegression
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
CONFRONTO_FILE = BASE_DIR / "Confronto Tutti Modelli vs Storiche Daily Max.xlsx"
OUTPUT_FILE = BASE_DIR / "model_error_stats.pkl"

SOUTH_CITIES = {'BuenosAires', 'SaoPaulo', 'Wellington'}

EXCLUDE_MODELS = {'cma_grapes_global'}

SEAMLESS_PAIRS = {
    'gfs_seamless': 'gfs_global',
    'gem_seamless': 'gem_global',
    'icon_seamless': 'icon_global',
    'jma_seamless': 'jma_gsm',
    'kma_seamless': 'kma_gdps',
    'dmi_seamless': 'dmi_harmonie_arome_europe',
    'knmi_seamless': 'knmi_harmonie_arome_europe',
    'meteofrance_seamless': 'meteofrance_arome_france',
    'ukmo_seamless': 'ukmo_global_deterministic_10km',
}

MIN_DATA_POINTS = 30


def get_season(date, city):
    month = date.month
    if city in SOUTH_CITIES:
        if month in (6, 7, 8): return 'Inverno'
        elif month in (9, 10, 11): return 'Primavera'
        elif month in (12, 1, 2): return 'Estate'
        else: return 'Autunno'
    else:
        if month in (12, 1, 2): return 'Inverno'
        elif month in (3, 4, 5): return 'Primavera'
        elif month in (6, 7, 8): return 'Estate'
        else: return 'Autunno'


def main():
    print("Precompute Model Error Statistics")
    print("=" * 60)

    # 1. Load data
    print("Loading data...")
    xls = pd.ExcelFile(CONFRONTO_FILE)
    cities = [s for s in xls.sheet_names if s != 'Riepilogo']

    all_data = {}
    for city in cities:
        df = pd.read_excel(xls, sheet_name=city)
        df = df[df['Data'].apply(lambda x: hasattr(x, 'year'))].copy()
        df['Data'] = pd.to_datetime(df['Data'])
        df = df.sort_values('Data').reset_index(drop=True)
        all_data[city] = df

    print(f"  {len(cities)} cities loaded")

    # 2. Compute per-model stats for D1 and D2
    stats = {}
    for lead_time in ['D1', 'D2']:
        prefix = f'{lead_time}c_'
        for city, df in all_data.items():
            model_cols = [c for c in df.columns if c.startswith(prefix)]

            filtered_cols = []
            for col in model_cols:
                model_name = col.replace(prefix, '')
                if model_name in EXCLUDE_MODELS:
                    continue
                if model_name in SEAMLESS_PAIRS:
                    base_col = prefix + SEAMLESS_PAIRS[model_name]
                    if base_col in model_cols:
                        continue
                filtered_cols.append(col)

            for col in filtered_cols:
                model_name = col.replace(prefix, '')

                for season in ['Inverno', 'Primavera', 'Estate', 'Autunno']:
                    mask = df['Data'].apply(lambda d: get_season(d, city)) == season
                    errors = df.loc[mask, col].dropna()

                    if len(errors) < MIN_DATA_POINTS:
                        continue

                    bias = float(errors.mean())
                    sigma = float(errors.std())
                    mae = float(errors.abs().mean())

                    if sigma < 0.1:
                        sigma = 0.5

                    stats[(city, model_name, season, lead_time)] = {
                        'bias': bias,
                        'sigma': sigma,
                        'mae': mae,
                        'verde_pct': float((errors.abs() <= 1).mean() * 100),
                        'n': len(errors),
                        'weight': 1.0 / max(mae, 0.01),
                    }

    print(f"  {len(stats)} model/city/season/lead combos computed")

    # 3. Train isotonic regression for calibration
    # Use the full dataset to build calibration data
    print("Training isotonic regression...")

    cal_probs = []
    cal_outcomes = []
    cal_probs_enh = []

    for city, df in all_data.items():
        prefix = 'D1c_'
        model_cols = [c for c in df.columns if c.startswith(prefix)]
        filtered_models = []
        for col in model_cols:
            model_name = col.replace(prefix, '')
            if model_name in EXCLUDE_MODELS:
                continue
            if model_name in SEAMLESS_PAIRS:
                base_col = prefix + SEAMLESS_PAIRS[model_name]
                if base_col in model_cols:
                    continue
            filtered_models.append(model_name)

        for idx in range(180, len(df)):
            row = df.iloc[idx]
            actual_C = row['Registrata_C']
            if pd.isna(actual_C):
                continue
            actual_bucket = int(round(actual_C))
            date = row['Data']
            season = get_season(date, city)

            forecasts = []
            biases = []
            sigmas = []
            weights = []

            for model_name in filtered_models:
                col = prefix + model_name
                delta = row.get(col)
                if pd.isna(delta):
                    continue
                forecast_C = actual_C + delta

                key = (city, model_name, season, 'D1')
                if key not in stats:
                    continue

                s = stats[key]
                forecasts.append(forecast_C)
                biases.append(s['bias'])
                sigmas.append(s['sigma'])
                weights.append(s['weight'])

            if len(forecasts) < 2:
                continue

            total_w = sum(weights)
            weights = [w / total_w for w in weights]

            corrected_means = [f - b for f, b in zip(forecasts, biases)]
            spread = float(np.std(corrected_means))
            enhanced_sigmas = [np.sqrt(s**2 + spread**2) for s in sigmas]

            forecast_mean = np.average(corrected_means, weights=weights)
            bucket_min = int(forecast_mean) - 8
            bucket_max = int(forecast_mean) + 8

            for k in range(bucket_min, bucket_max + 1):
                # Base prob
                p_base = sum(
                    w * (norm.cdf((k + 0.5 - (f - b)) / s) - norm.cdf((k - 0.5 - (f - b)) / s))
                    for f, b, s, w in zip(forecasts, biases, sigmas, weights)
                )
                # Enhanced prob
                p_enh = sum(
                    w * (norm.cdf((k + 0.5 - (f - b)) / se) - norm.cdf((k - 0.5 - (f - b)) / se))
                    for f, b, se, w in zip(forecasts, biases, enhanced_sigmas, weights)
                )

                cal_probs.append(p_base)
                cal_probs_enh.append(p_enh)
                cal_outcomes.append(1 if k == actual_bucket else 0)

    cal_probs = np.array(cal_probs)
    cal_probs_enh = np.array(cal_probs_enh)
    cal_outcomes = np.array(cal_outcomes)

    print(f"  Calibration data: {len(cal_probs)} samples")

    iso_base = IsotonicRegression(y_min=0.001, y_max=0.999, out_of_bounds='clip')
    iso_base.fit(cal_probs, cal_outcomes)

    iso_enhanced = IsotonicRegression(y_min=0.001, y_max=0.999, out_of_bounds='clip')
    iso_enhanced.fit(cal_probs_enh, cal_outcomes)

    print("  Isotonic regression fitted")

    # 4. Save
    output = {
        'stats': stats,
        'iso_base': iso_base,
        'iso_enhanced': iso_enhanced,
        'exclude_models': EXCLUDE_MODELS,
        'seamless_pairs': SEAMLESS_PAIRS,
        'generated_at': pd.Timestamp.now().isoformat(),
    }

    with open(OUTPUT_FILE, 'wb') as f:
        pickle.dump(output, f)

    print(f"\nSaved to: {OUTPUT_FILE}")
    print(f"  Stats entries: {len(stats)}")
    print(f"  File size: {OUTPUT_FILE.stat().st_size / 1024:.0f} KB")

    # Print summary
    d1_stats = {k: v for k, v in stats.items() if k[3] == 'D1'}
    cities_in_stats = set(k[0] for k in d1_stats)
    models_in_stats = set(k[1] for k in d1_stats)
    print(f"  D1 stats: {len(d1_stats)} entries, {len(cities_in_stats)} cities, {len(models_in_stats)} models")


if __name__ == '__main__':
    main()
