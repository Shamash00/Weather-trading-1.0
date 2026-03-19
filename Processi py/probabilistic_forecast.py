"""
Pipeline Probabilistica per Weather Trading
============================================
Trasforma previsioni deterministiche in distribuzioni di probabilità per bucket Polymarket.

Fasi:
1. Carica storico forecast-vs-osservazioni (Confronto Tutti Modelli)
2. Stima bias e sigma per modello × città × stagione × lead time
3. Per ogni data storica, costruisce distribuzione combinata (mixture)
4. Calcola probabilità per bucket integrando la distribuzione
5. Calibra con isotonic regression
6. Valuta con Brier score, log loss, RPS
7. Confronta con Polymarket (se dati disponibili)

Output: "Analisi Probabilistica.xlsx"
"""

import warnings
warnings.filterwarnings('ignore')

import numpy as np
import pandas as pd
from scipy.stats import norm, t as student_t
from sklearn.isotonic import IsotonicRegression
from pathlib import Path
import datetime

# ============================================================
# CONFIG
# ============================================================
BASE_DIR = Path(__file__).resolve().parent.parent
CONFRONTO_FILE = BASE_DIR / "Confronto Tutti Modelli vs Storiche Daily Max.xlsx"
OUTPUT_FILE = BASE_DIR / "Analisi Probabilistica.xlsx"

# Minimum data points per model/city/season for reliable stats
MIN_DATA_POINTS = 30
# Minimum training window (days) before we start predicting
MIN_TRAIN_DAYS = 180
# Models to exclude (too noisy or redundant seamless duplicates)
EXCLUDE_MODELS = {'cma_grapes_global'}  # std ~10, unreliable
# Seamless models duplicate their base model - keep only base or seamless
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

# Southern hemisphere cities (inverted seasons)
SOUTH_CITIES = {'BuenosAires', 'SaoPaulo', 'Wellington'}

# Season definitions
def get_season(date, city):
    month = date.month
    if city in SOUTH_CITIES:
        # Inverted
        if month in (6, 7, 8): return 'Inverno'
        elif month in (9, 10, 11): return 'Primavera'
        elif month in (12, 1, 2): return 'Estate'
        else: return 'Autunno'
    else:
        if month in (12, 1, 2): return 'Inverno'
        elif month in (3, 4, 5): return 'Primavera'
        elif month in (6, 7, 8): return 'Estate'
        else: return 'Autunno'


# ============================================================
# FASE 1: Carica dati
# ============================================================
def load_data():
    """Carica il confronto tutti modelli e restituisce un dict city -> DataFrame."""
    print("=" * 60)
    print("FASE 1: Caricamento dati")
    print("=" * 60)

    xls = pd.ExcelFile(CONFRONTO_FILE)
    cities = [s for s in xls.sheet_names if s != 'Riepilogo']

    all_data = {}
    for city in cities:
        df = pd.read_excel(xls, sheet_name=city)
        # Remove summary rows at the bottom
        df = df[df['Data'].apply(lambda x: hasattr(x, 'year'))].copy()
        df['Data'] = pd.to_datetime(df['Data'])
        df = df.sort_values('Data').reset_index(drop=True)
        all_data[city] = df

    total_rows = sum(len(df) for df in all_data.values())
    print(f"  Caricate {len(cities)} città, {total_rows} righe totali")
    print(f"  Città: {', '.join(cities)}")

    return all_data


# ============================================================
# FASE 2: Stima errore per modello × città × stagione
# ============================================================
def estimate_error_distributions(all_data, lead_time='D1'):
    """
    Per ogni modello × città × stagione, stima bias e sigma dell'errore.

    L'errore è D1c (o D2c) = round(forecast) - actual_C
    Quindi: actual = forecast - error
    Se bias = mean(error), sigma = std(error), allora:
        actual | forecast ~ N(forecast - bias, sigma²)
    """
    print("\n" + "=" * 60)
    print(f"FASE 2: Stima distribuzioni errore ({lead_time})")
    print("=" * 60)

    prefix = f'{lead_time}c_'
    stats_rows = []

    for city, df in all_data.items():
        model_cols = [c for c in df.columns if c.startswith(prefix)]

        # Filter out excluded and seamless duplicates
        filtered_cols = []
        for col in model_cols:
            model_name = col.replace(prefix, '')
            if model_name in EXCLUDE_MODELS:
                continue
            # Keep seamless only if base doesn't exist
            if model_name in SEAMLESS_PAIRS:
                base_col = prefix + SEAMLESS_PAIRS[model_name]
                if base_col in model_cols:
                    continue  # Skip seamless, keep base
            filtered_cols.append(col)

        for col in filtered_cols:
            model_name = col.replace(prefix, '')

            for season in ['Inverno', 'Primavera', 'Estate', 'Autunno']:
                mask = df['Data'].apply(lambda d: get_season(d, city)) == season
                errors = df.loc[mask, col].dropna()

                if len(errors) < MIN_DATA_POINTS:
                    continue

                bias = errors.mean()
                sigma = errors.std()
                mae = errors.abs().mean()
                verde_pct = (errors.abs() <= 1).mean() * 100
                n = len(errors)

                # Shapiro-Wilk test for normality (subsample if too large)
                if len(errors) > 5000:
                    sample = errors.sample(5000, random_state=42)
                else:
                    sample = errors

                stats_rows.append({
                    'Citta': city,
                    'Modello': model_name,
                    'Stagione': season,
                    'Lead': lead_time,
                    'N': n,
                    'Bias': round(bias, 3),
                    'Sigma': round(sigma, 3),
                    'MAE': round(mae, 3),
                    'Verde%': round(verde_pct, 1),
                    # Weight: inverse MAE (higher = better model)
                    'Peso_inv_MAE': round(1.0 / max(mae, 0.01), 4),
                })

    stats_df = pd.DataFrame(stats_rows)
    print(f"  Calcolate {len(stats_df)} combinazioni modello×città×stagione")

    # Normalize weights within each city × season
    for (city, season), group in stats_df.groupby(['Citta', 'Stagione']):
        total_w = group['Peso_inv_MAE'].sum()
        stats_df.loc[group.index, 'Peso_norm'] = (group['Peso_inv_MAE'] / total_w).round(4)

    # Print summary
    summary = stats_df.groupby('Citta').agg(
        N_modelli=('Modello', 'nunique'),
        Media_Verde=('Verde%', 'mean'),
        Migliore=('Verde%', 'max')
    ).round(1)
    print("\n  Riepilogo per città:")
    print(summary.to_string())

    return stats_df


# ============================================================
# FASE 3-4: Calcola probabilità bucket per ogni data storica
# ============================================================
def bucket_probability(mu, sigma, bucket_center):
    """
    Probabilità che la temperatura cada nel bucket [center-0.5, center+0.5).
    P(k-0.5 <= T < k+0.5) = Phi((k+0.5-mu)/sigma) - Phi((k-0.5-mu)/sigma)
    """
    if sigma < 0.01:
        sigma = 0.01  # Avoid division by zero
    return norm.cdf((bucket_center + 0.5 - mu) / sigma) - norm.cdf((bucket_center - 0.5 - mu) / sigma)


def compute_mixture_bucket_probs(forecasts, biases, sigmas, weights, bucket_range):
    """
    Calcola la probabilità per ogni bucket usando una mistura di normali.

    p(T) = sum_i w_i * N(T | forecast_i - bias_i, sigma_i²)
    P(bucket=k) = sum_i w_i * [Phi((k+0.5-mu_i)/sigma_i) - Phi((k-0.5-mu_i)/sigma_i)]

    Returns: dict {bucket: probability}
    """
    probs = {}
    for k in bucket_range:
        p = 0.0
        for f, b, s, w in zip(forecasts, biases, sigmas, weights):
            mu = f - b  # bias-corrected forecast (expected actual)
            p += w * bucket_probability(mu, s, k)
        probs[k] = p

    # Normalize (should already be ~1.0 if bucket_range is wide enough)
    total = sum(probs.values())
    if total > 0:
        probs = {k: v / total for k, v in probs.items()}

    return probs


def run_backtest(all_data, stats_df, lead_time='D1'):
    """
    Per ogni data storica, calcola le probabilità dei bucket usando
    un expanding window (solo dati passati per stimare bias/sigma).
    """
    print("\n" + "=" * 60)
    print(f"FASE 3-4: Backtest probabilistico ({lead_time})")
    print("=" * 60)

    prefix = f'{lead_time}c_'
    results = []

    for city, df in all_data.items():
        print(f"\n  {city}...", end='', flush=True)

        model_cols = [c for c in df.columns if c.startswith(prefix)]
        # Same filtering as in estimate_error_distributions
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

        n_predicted = 0

        for idx in range(MIN_TRAIN_DAYS, len(df)):
            row = df.iloc[idx]
            date = row['Data']
            actual_C = row['Registrata_C']
            season = get_season(date, city)

            if pd.isna(actual_C):
                continue

            # Expanding window: use only past data
            train_df = df.iloc[:idx]
            train_mask = train_df['Data'].apply(lambda d: get_season(d, city)) == season
            train_season = train_df[train_mask]

            if len(train_season) < MIN_DATA_POINTS:
                continue

            # For each available model, compute bias/sigma from past data only
            forecasts = []
            biases = []
            sigmas = []
            weights = []
            model_names = []

            for model_name in filtered_models:
                col = prefix + model_name

                # Current forecast
                delta = row.get(col)
                if pd.isna(delta):
                    continue

                forecast_C = actual_C + delta  # Reconstruct forecast

                # Historical errors for this model/season (train only)
                hist_errors = train_season[col].dropna()
                if len(hist_errors) < MIN_DATA_POINTS:
                    continue

                bias = hist_errors.mean()
                sigma = hist_errors.std()
                mae = hist_errors.abs().mean()

                if sigma < 0.1 or mae < 0.01:
                    continue

                forecasts.append(forecast_C)
                biases.append(bias)
                sigmas.append(sigma)
                weights.append(1.0 / mae)
                model_names.append(model_name)

            if len(forecasts) < 2:
                continue

            # Normalize weights
            total_w = sum(weights)
            weights = [w / total_w for w in weights]

            # Determine bucket range
            forecast_mean = np.average(
                [f - b for f, b in zip(forecasts, biases)],
                weights=weights
            )
            bucket_min = int(forecast_mean) - 8
            bucket_max = int(forecast_mean) + 8
            bucket_range = range(bucket_min, bucket_max + 1)

            # Compute mixture probabilities
            probs = compute_mixture_bucket_probs(
                forecasts, biases, sigmas, weights, bucket_range
            )

            # Also compute spread-adjusted sigma
            # When models disagree more, increase uncertainty
            corrected_means = [f - b for f, b in zip(forecasts, biases)]
            inter_model_spread = np.std(corrected_means)

            # Enhanced mixture: inflate sigma by inter-model spread
            enhanced_sigmas = [np.sqrt(s**2 + inter_model_spread**2) for s in sigmas]
            probs_enhanced = compute_mixture_bucket_probs(
                forecasts, biases, enhanced_sigmas, weights, bucket_range
            )

            # Results
            actual_bucket = int(round(actual_C))
            top_bucket = max(probs, key=probs.get)
            top_bucket_enhanced = max(probs_enhanced, key=probs_enhanced.get)

            p_actual = probs.get(actual_bucket, 0.0)
            p_actual_enhanced = probs_enhanced.get(actual_bucket, 0.0)

            # P(±1): probability within 1°C of actual
            p_pm1 = sum(probs.get(actual_bucket + d, 0.0) for d in [-1, 0, 1])
            p_pm1_enhanced = sum(probs_enhanced.get(actual_bucket + d, 0.0) for d in [-1, 0, 1])

            # Top 2 buckets
            sorted_buckets = sorted(probs.items(), key=lambda x: -x[1])
            top2 = [sorted_buckets[0][0], sorted_buckets[1][0]] if len(sorted_buckets) > 1 else [sorted_buckets[0][0]]

            sorted_enhanced = sorted(probs_enhanced.items(), key=lambda x: -x[1])
            top2_enh = [sorted_enhanced[0][0], sorted_enhanced[1][0]] if len(sorted_enhanced) > 1 else [sorted_enhanced[0][0]]

            results.append({
                'Data': date,
                'Citta': city,
                'Stagione': season,
                'Registrata_C': actual_C,
                'Actual_Bucket': actual_bucket,
                'N_Modelli': len(forecasts),
                'Spread_Modelli': round(inter_model_spread, 2),
                # --- Base mixture ---
                'Top_Bucket': top_bucket,
                'P_Top': round(sorted_buckets[0][1], 4),
                'P_Actual': round(p_actual, 4),
                'P_pm1': round(p_pm1, 4),
                'Hit_Top1': int(top_bucket == actual_bucket),
                'Hit_Top2': int(actual_bucket in top2),
                'Errore_Top': top_bucket - actual_bucket,
                # --- Enhanced (spread-adjusted) ---
                'Top_Bucket_Enh': top_bucket_enhanced,
                'P_Top_Enh': round(sorted_enhanced[0][1], 4),
                'P_Actual_Enh': round(p_actual_enhanced, 4),
                'P_pm1_Enh': round(p_pm1_enhanced, 4),
                'Hit_Top1_Enh': int(top_bucket_enhanced == actual_bucket),
                'Hit_Top2_Enh': int(actual_bucket in top2_enh),
                # --- Full distribution (top 5 for reference) ---
                'Distrib_Top5': str({k: round(v, 3) for k, v in sorted_buckets[:5]}),
                # --- Raw data ---
                '_probs': probs,
                '_probs_enhanced': probs_enhanced,
            })
            n_predicted += 1

        print(f" {n_predicted} giorni")

    results_df = pd.DataFrame(results)
    print(f"\n  Totale predizioni: {len(results_df)}")

    # Summary
    if len(results_df) > 0:
        print("\n  Riepilogo backtest:")
        summary = results_df.groupby('Citta').agg(
            N=('Hit_Top1', 'count'),
            Hit_Top1=('Hit_Top1', 'mean'),
            Hit_Top2=('Hit_Top2', 'mean'),
            P_Actual_media=('P_Actual', 'mean'),
            Hit_Top1_Enh=('Hit_Top1_Enh', 'mean'),
            Hit_Top2_Enh=('Hit_Top2_Enh', 'mean'),
            P_Actual_Enh_media=('P_Actual_Enh', 'mean'),
        ).round(3)
        summary.columns = ['N', 'Hit%_Top1', 'Hit%_Top2', 'P(Actual)',
                          'Hit%_Top1_Enh', 'Hit%_Top2_Enh', 'P(Actual)_Enh']
        for col in ['Hit%_Top1', 'Hit%_Top2', 'Hit%_Top1_Enh', 'Hit%_Top2_Enh']:
            summary[col] = (summary[col] * 100).round(1)
        print(summary.to_string())

    return results_df


# ============================================================
# FASE 5: Calibrazione
# ============================================================
def calibrate_probabilities(results_df):
    """
    Calibra le probabilità usando isotonic regression.

    Per ogni predizione, prende P(bucket) e il risultato binario (1 se quel bucket ha vinto).
    Poi fita una isotonic regression: P_calibrated = f(P_raw).
    """
    print("\n" + "=" * 60)
    print("FASE 5: Calibrazione (Isotonic Regression)")
    print("=" * 60)

    if len(results_df) == 0:
        print("  Nessun dato per calibrazione")
        return results_df, None

    # Build calibration dataset: for each prediction, expand all bucket probabilities
    cal_probs = []
    cal_outcomes = []
    cal_probs_enh = []

    for _, row in results_df.iterrows():
        probs = row['_probs']
        probs_enh = row['_probs_enhanced']
        actual = row['Actual_Bucket']

        for bucket, p in probs.items():
            cal_probs.append(p)
            cal_outcomes.append(1 if bucket == actual else 0)
            cal_probs_enh.append(probs_enh.get(bucket, 0.0))

    cal_probs = np.array(cal_probs)
    cal_outcomes = np.array(cal_outcomes)
    cal_probs_enh = np.array(cal_probs_enh)

    # Fit isotonic regression (base)
    iso_base = IsotonicRegression(y_min=0.001, y_max=0.999, out_of_bounds='clip')
    iso_base.fit(cal_probs, cal_outcomes)

    # Fit isotonic regression (enhanced)
    iso_enh = IsotonicRegression(y_min=0.001, y_max=0.999, out_of_bounds='clip')
    iso_enh.fit(cal_probs_enh, cal_outcomes)

    # Apply calibration to results - pre-create columns to avoid pandas issues
    p_actual_cal_list = []
    p_actual_enh_cal_list = []
    probs_cal_list = []
    probs_enh_cal_list = []

    for idx, row in results_df.iterrows():
        probs = row['_probs']
        probs_enh = row['_probs_enhanced']
        actual = row['Actual_Bucket']

        # Calibrate base
        buckets = list(probs.keys())
        raw_p = np.array([probs[b] for b in buckets])
        cal_p = iso_base.predict(raw_p)
        cal_p = cal_p / cal_p.sum()  # Renormalize
        cal_probs_dict = dict(zip(buckets, cal_p))

        # Calibrate enhanced
        raw_p_enh = np.array([probs_enh.get(b, 0.0) for b in buckets])
        cal_p_enh = iso_enh.predict(raw_p_enh)
        cal_p_enh = cal_p_enh / cal_p_enh.sum()
        cal_probs_enh_dict = dict(zip(buckets, cal_p_enh))

        p_actual_cal_list.append(round(cal_probs_dict.get(actual, 0.0), 4))
        p_actual_enh_cal_list.append(round(cal_probs_enh_dict.get(actual, 0.0), 4))
        probs_cal_list.append(cal_probs_dict)
        probs_enh_cal_list.append(cal_probs_enh_dict)

    results_df['P_Actual_Cal'] = p_actual_cal_list
    results_df['P_Actual_Enh_Cal'] = p_actual_enh_cal_list
    results_df['_probs_cal'] = probs_cal_list
    results_df['_probs_enh_cal'] = probs_enh_cal_list

    # Calibration reliability check
    print("\n  Reliability diagram (base model):")
    print(f"  {'Bin P_raw':>12}  {'Freq effettiva':>15}  {'N campioni':>12}")
    bins = np.linspace(0, 0.5, 11)
    for i in range(len(bins) - 1):
        mask = (cal_probs >= bins[i]) & (cal_probs < bins[i+1])
        if mask.sum() > 0:
            freq = cal_outcomes[mask].mean()
            print(f"  {bins[i]:.2f}-{bins[i+1]:.2f}      {freq:.4f}          {mask.sum():>6}")

    return results_df, (iso_base, iso_enh)


# ============================================================
# FASE 6: Valutazione probabilistica
# ============================================================
def evaluate_probabilistic(results_df):
    """
    Valuta la qualità delle probabilità con:
    - Log loss (lower is better)
    - Brier score (lower is better)
    - Ranked Probability Score (lower is better)
    - Accuracy (hit rate)
    """
    print("\n" + "=" * 60)
    print("FASE 6: Valutazione probabilistica")
    print("=" * 60)

    if len(results_df) == 0:
        print("  Nessun dato")
        return pd.DataFrame()

    eval_rows = []

    for city, group in results_df.groupby('Citta'):
        for method, probs_key, p_actual_key in [
            ('Base', '_probs', 'P_Actual'),
            ('Enhanced', '_probs_enhanced', 'P_Actual_Enh'),
            ('Calibrato', '_probs_cal', 'P_Actual_Cal'),
            ('Enh+Cal', '_probs_enh_cal', 'P_Actual_Enh_Cal'),
        ]:
            if probs_key not in group.columns:
                continue

            log_losses = []
            brier_scores = []
            rps_scores = []

            for _, row in group.iterrows():
                probs = row[probs_key]
                if not isinstance(probs, dict):
                    continue

                actual = row['Actual_Bucket']
                p_actual = probs.get(actual, 0.001)

                # Log loss
                log_losses.append(-np.log(max(p_actual, 1e-10)))

                # Brier score (multiclass)
                brier = 0
                for bucket, p in probs.items():
                    outcome = 1 if bucket == actual else 0
                    brier += (p - outcome) ** 2
                brier_scores.append(brier)

                # RPS (Ranked Probability Score)
                buckets_sorted = sorted(probs.keys())
                cum_pred = 0
                cum_actual = 0
                rps = 0
                for b in buckets_sorted:
                    cum_pred += probs[b]
                    cum_actual += (1 if b <= actual else 0)
                    # Actually for exact bucket: cum_actual should be 0 before actual, 1 at and after
                    rps += (cum_pred - cum_actual) ** 2
                # Wait, let me fix this. For RPS, cum_actual = 1 if b >= actual_bucket
                # Actually the correct formulation:
                # cum_actual(k) = 1 if actual <= k, else 0
                # RPS = sum over k of (cum_pred(k) - cum_actual(k))^2
                # Let me recalculate properly

                cum_pred = 0
                rps = 0
                for b in buckets_sorted:
                    cum_pred += probs[b]
                    cum_obs = 1.0 if actual <= b else 0.0
                    rps += (cum_pred - cum_obs) ** 2
                if len(buckets_sorted) > 1:
                    rps /= (len(buckets_sorted) - 1)
                rps_scores.append(rps)

            if not log_losses:
                continue

            # Hit rates from the pre-computed columns
            if method == 'Base':
                hit1 = group['Hit_Top1'].mean() * 100
                hit2 = group['Hit_Top2'].mean() * 100
            elif method == 'Enhanced':
                hit1 = group['Hit_Top1_Enh'].mean() * 100
                hit2 = group['Hit_Top2_Enh'].mean() * 100
            else:
                # Recompute for calibrated
                hits1, hits2 = 0, 0
                for _, row in group.iterrows():
                    probs = row[probs_key]
                    if not isinstance(probs, dict):
                        continue
                    actual = row['Actual_Bucket']
                    sorted_b = sorted(probs.items(), key=lambda x: -x[1])
                    if sorted_b[0][0] == actual:
                        hits1 += 1
                    if actual in [sorted_b[0][0], sorted_b[1][0]] if len(sorted_b) > 1 else [sorted_b[0][0]]:
                        hits2 += 1
                n_valid = sum(1 for _, r in group.iterrows() if isinstance(r[probs_key], dict))
                hit1 = hits1 / max(n_valid, 1) * 100
                hit2 = hits2 / max(n_valid, 1) * 100

            eval_rows.append({
                'Citta': city,
                'Metodo': method,
                'N': len(log_losses),
                'Hit%_Top1': round(hit1, 1),
                'Hit%_Top2': round(hit2, 1),
                'Log_Loss': round(np.mean(log_losses), 4),
                'Brier': round(np.mean(brier_scores), 4),
                'RPS': round(np.mean(rps_scores), 4),
                'P(Actual)_Media': round(np.mean([row[p_actual_key] for _, row in group.iterrows() if not pd.isna(row.get(p_actual_key, np.nan))]), 4),
            })

    # Add totals
    eval_df = pd.DataFrame(eval_rows)

    for method in eval_df['Metodo'].unique():
        m = eval_df[eval_df['Metodo'] == method]
        eval_df = pd.concat([eval_df, pd.DataFrame([{
            'Citta': '** TOTALE **',
            'Metodo': method,
            'N': m['N'].sum(),
            'Hit%_Top1': round((m['Hit%_Top1'] * m['N']).sum() / m['N'].sum(), 1),
            'Hit%_Top2': round((m['Hit%_Top2'] * m['N']).sum() / m['N'].sum(), 1),
            'Log_Loss': round((m['Log_Loss'] * m['N']).sum() / m['N'].sum(), 4),
            'Brier': round((m['Brier'] * m['N']).sum() / m['N'].sum(), 4),
            'RPS': round((m['RPS'] * m['N']).sum() / m['N'].sum(), 4),
            'P(Actual)_Media': round((m['P(Actual)_Media'] * m['N']).sum() / m['N'].sum(), 4),
        }])], ignore_index=True)

    print("\n  Metriche per metodo (media pesata su tutte le città):")
    totals = eval_df[eval_df['Citta'] == '** TOTALE **'].set_index('Metodo')
    print(totals[['N', 'Hit%_Top1', 'Hit%_Top2', 'Log_Loss', 'Brier', 'RPS', 'P(Actual)_Media']].to_string())

    return eval_df


# ============================================================
# FASE 7: Confronto vs Polymarket
# ============================================================
def compare_polymarket(results_df):
    """Confronta probabilità calibrate vs prezzi Polymarket (se dati disponibili)."""
    print("\n" + "=" * 60)
    print("FASE 7: Confronto vs Polymarket")
    print("=" * 60)

    import sqlite3
    db_path = BASE_DIR / "polymarket_storico.db"

    if not db_path.exists():
        print("  Database Polymarket non trovato, skip")
        return pd.DataFrame()

    conn = sqlite3.connect(db_path)

    # Load resolved markets
    markets = pd.read_sql('''
        SELECT m.*, e.city, e.city_local, e.forecast_date
        FROM markets m JOIN events e ON m.event_id = e.event_id
        WHERE e.closed = 1
    ''', conn)
    conn.close()

    if len(markets) == 0:
        print("  Nessun mercato risolto trovato")
        return pd.DataFrame()

    print(f"  Trovati {len(markets)} bucket in {markets['event_id'].nunique()} eventi risolti")

    # For each resolved event, check if we have backtest predictions
    comparison_rows = []

    for event_id, event_markets in markets.groupby('event_id'):
        city_local = event_markets.iloc[0]['city_local']
        forecast_date = event_markets.iloc[0]['forecast_date']

        # Find matching backtest prediction
        match = results_df[
            (results_df['Citta'] == city_local) &
            (results_df['Data'].dt.strftime('%Y-%m-%d') == forecast_date)
        ]

        if len(match) == 0:
            continue

        row = match.iloc[0]
        probs_cal = row.get('_probs_enh_cal')
        if not isinstance(probs_cal, dict):
            probs_cal = row.get('_probs_enhanced')
        if not isinstance(probs_cal, dict):
            continue

        winner = event_markets[event_markets['winner'] == 'Yes']
        actual_bucket = row['Actual_Bucket']

        for _, mkt in event_markets.iterrows():
            bucket_temp = mkt['bucket_temp_c']
            poly_price = mkt['last_price_yes']
            is_winner = mkt['winner'] == 'Yes'

            my_prob = probs_cal.get(int(bucket_temp), 0.0) if not pd.isna(bucket_temp) else 0.0
            edge = my_prob - poly_price if not pd.isna(poly_price) else None

            comparison_rows.append({
                'Data': forecast_date,
                'Citta': city_local,
                'Bucket': mkt['bucket_label'],
                'Temp_C': bucket_temp,
                'P_Mio': round(my_prob, 4),
                'P_Polymarket': round(poly_price, 4) if not pd.isna(poly_price) else None,
                'Edge': round(edge, 4) if edge is not None else None,
                'Vincitore': 'SI' if is_winner else 'NO',
            })

    comp_df = pd.DataFrame(comparison_rows)

    if len(comp_df) > 0:
        print(f"\n  Confronto su {len(comp_df)} bucket:")
        winners = comp_df[comp_df['Vincitore'] == 'SI']
        if len(winners) > 0:
            print(f"  Bucket vincenti - P_Mio media: {winners['P_Mio'].mean():.3f}, P_Poly media: {winners['P_Polymarket'].mean():.3f}")
            print(f"  Edge medio su vincenti: {winners['Edge'].mean():.3f}")

    return comp_df


# ============================================================
# OUTPUT: Genera Excel
# ============================================================
def save_results(stats_df, results_df, eval_df, comp_df):
    """Salva tutto in un Excel con più fogli."""
    print("\n" + "=" * 60)
    print("OUTPUT: Generazione Excel")
    print("=" * 60)

    # Clean internal columns before saving
    output_cols = [c for c in results_df.columns if not c.startswith('_')]
    results_clean = results_df[output_cols].copy()

    with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
        # Sheet 1: Statistiche modelli
        stats_df.to_excel(writer, sheet_name='Statistiche Modelli', index=False)

        # Sheet 2: Backtest dettaglio
        results_clean.to_excel(writer, sheet_name='Backtest Dettaglio', index=False)

        # Sheet 3: Backtest per stagione
        if len(results_df) > 0:
            season_summary = results_df.groupby(['Citta', 'Stagione']).agg(
                N=('Hit_Top1', 'count'),
                Hit_Top1=('Hit_Top1', 'mean'),
                Hit_Top2=('Hit_Top2', 'mean'),
                P_Actual=('P_Actual', 'mean'),
                Hit_Top1_Enh=('Hit_Top1_Enh', 'mean'),
                P_Actual_Enh=('P_Actual_Enh', 'mean'),
                Spread_medio=('Spread_Modelli', 'mean'),
                N_Modelli_medio=('N_Modelli', 'mean'),
            ).round(3)
            for col in ['Hit_Top1', 'Hit_Top2', 'Hit_Top1_Enh']:
                season_summary[col] = (season_summary[col] * 100).round(1)
            season_summary.to_excel(writer, sheet_name='Backtest per Stagione')

        # Sheet 4: Metriche
        if len(eval_df) > 0:
            eval_df.to_excel(writer, sheet_name='Metriche', index=False)

        # Sheet 5: Confronto Polymarket
        if len(comp_df) > 0:
            comp_df.to_excel(writer, sheet_name='Confronto Polymarket', index=False)

        # Sheet 6: Matrice Hit% per città × stagione (migliore metodo)
        if len(results_df) > 0:
            pivot = results_df.groupby(['Citta', 'Stagione'])['Hit_Top1_Enh'].mean().unstack()
            pivot = (pivot * 100).round(1)
            pivot['Media'] = pivot.mean(axis=1).round(1)
            pivot.to_excel(writer, sheet_name='Matrice Hit%')

        # Sheet 7: Distribuzione esempio (ultime 20 predizioni per ogni città)
        if len(results_df) > 0:
            examples = []
            for city, group in results_df.groupby('Citta'):
                last_rows = group.tail(5)
                for _, row in last_rows.iterrows():
                    probs = row['_probs_enhanced']
                    if isinstance(probs, dict):
                        sorted_b = sorted(probs.items(), key=lambda x: -x[1])[:8]
                        for rank, (bucket, prob) in enumerate(sorted_b, 1):
                            examples.append({
                                'Citta': city,
                                'Data': row['Data'],
                                'Registrata_C': row['Registrata_C'],
                                'Rank': rank,
                                'Bucket': bucket,
                                'Probabilita': round(prob * 100, 1),
                                'Vincente': 'SI' if bucket == row['Actual_Bucket'] else '',
                            })
            if examples:
                pd.DataFrame(examples).to_excel(writer, sheet_name='Distribuzioni Esempio', index=False)

    print(f"\n  Salvato: {OUTPUT_FILE}")
    print(f"  Fogli: Statistiche Modelli, Backtest Dettaglio, Backtest per Stagione,")
    print(f"         Metriche, Confronto Polymarket, Matrice Hit%, Distribuzioni Esempio")


# ============================================================
# MAIN
# ============================================================
def main():
    print("Pipeline Probabilistica per Weather Trading")
    print("=" * 60)
    start = datetime.datetime.now()

    # 1. Load data
    all_data = load_data()

    # 2. Estimate error distributions (D1 = 1 day ahead)
    stats_df = estimate_error_distributions(all_data, lead_time='D1')

    # 3-4. Backtest
    results_df = run_backtest(all_data, stats_df, lead_time='D1')

    # 5. Calibrate
    results_df, iso_models = calibrate_probabilities(results_df)

    # 6. Evaluate
    eval_df = evaluate_probabilistic(results_df)

    # 7. Compare vs Polymarket
    comp_df = compare_polymarket(results_df)

    # Save
    save_results(stats_df, results_df, eval_df, comp_df)

    elapsed = (datetime.datetime.now() - start).total_seconds()
    print(f"\n{'=' * 60}")
    print(f"Completato in {elapsed:.0f} secondi")
    print(f"{'=' * 60}")


if __name__ == '__main__':
    main()
