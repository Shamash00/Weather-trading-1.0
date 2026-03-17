import pandas as pd
import numpy as np
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
storico_path = os.path.join(BASE_DIR, "Temperature Storiche",
                            "Temperature Storiche FM-15 Tutte le Citta 2021-2025 Daily Max.xlsx")
CITIES = ['Ankara','Atlanta','BuenosAires','Chicago','Dallas','Londra','Lucknow',
          'Miami','Monaco','New York','Parigi','SaoPaulo','Seattle','Seoul',
          'Shanghai','Singapore','TelAviv','Tokyo','Toronto','Wellington']
CACHE_DIR = os.path.join(BASE_DIR, "_cache_modelli")


def compute_daily_max(hourly_df):
    hourly_df["Data"] = hourly_df["time"].dt.date
    prev1 = [c for c in hourly_df.columns if c.startswith("temperature_2m_previous_day1_")]
    agg = {c: "max" for c in prev1}
    daily = hourly_df.groupby("Data").agg(agg).reset_index()
    rename = {c: f"1GG_{c.replace('temperature_2m_previous_day1_','')}" for c in prev1}
    daily.rename(columns=rename, inplace=True)
    return daily


xl_st = pd.ExcelFile(storico_path)

print("=" * 105)
print("CONFRONTO PRECISIONE: Intero vs 1 Decimale vs Preciso")
print()
print("Approcci testati:")
print("  INT:  delta = round(forecast_C) - round(actual_C)    soglia: |d| <= 1")
print("  DEC1: delta = round1(forecast_C) - round1(actual_C)  soglia: |d| <= 1.0")
print("  DEC2: delta = forecast_C_preciso - actual_C_preciso   soglia: |d| <= 1.0")
print("=" * 105)

results = []

for city_name in CITIES:
    if city_name not in xl_st.sheet_names:
        continue
    cache_file = os.path.join(CACHE_DIR, f"{city_name}.pkl")
    if not os.path.exists(cache_file):
        continue
    raw_df = pd.read_pickle(cache_file)
    raw_df["time"] = pd.to_datetime(raw_df["time"])
    daily = compute_daily_max(raw_df)
    df_st = pd.read_excel(xl_st, sheet_name=city_name)
    df_st["Data"] = pd.to_datetime(df_st["Data"]).dt.date
    merged = pd.merge(daily, df_st[["Data", "Max_Temperatura_F"]], on="Data", how="inner")

    model_cols = sorted([c for c in merged.columns if c.startswith("1GG_")])
    min_valid = len(merged) * 0.10
    model_cols = [c for c in model_cols if merged[c].notna().sum() >= min_valid]

    actual_f = merged["Max_Temperatura_F"]
    actual_c_precise = (actual_f - 32) * 5 / 9
    actual_c_round0 = actual_c_precise.round(0)
    actual_c_round1 = actual_c_precise.round(1)

    city_int = []
    city_dec1 = []
    city_dec2 = []

    for c in model_cols:
        forecast_f = merged[c].round(0)
        forecast_c_precise = (forecast_f - 32) * 5 / 9
        forecast_c_round0 = forecast_c_precise.round(0)
        forecast_c_round1 = forecast_c_precise.round(1)

        # INT
        delta_int = forecast_c_round0 - actual_c_round0
        valid = delta_int.dropna()
        if len(valid) == 0:
            continue
        verde_int = (valid.abs() <= 1).sum() / len(valid) * 100

        # DEC1
        delta_dec1 = forecast_c_round1 - actual_c_round1
        valid1 = delta_dec1.dropna()
        verde_dec1 = (valid1.abs() <= 1.0).sum() / len(valid1) * 100

        # DEC2
        delta_dec2 = forecast_c_precise - actual_c_precise
        valid2 = delta_dec2.dropna()
        verde_dec2 = (valid2.abs() <= 1.0).sum() / len(valid2) * 100

        city_int.append(verde_int)
        city_dec1.append(verde_dec1)
        city_dec2.append(verde_dec2)

    results.append({
        "city": city_name,
        "avg_int": np.mean(city_int),
        "avg_dec1": np.mean(city_dec1),
        "avg_dec2": np.mean(city_dec2),
        "best_int": max(city_int),
        "best_dec1": max(city_dec1),
        "best_dec2": max(city_dec2),
    })

print()
print(f"{'Citta':<15} {'--- MEDIA tutti modelli ---':>30}  | {'--- MIGLIOR modello ---':>30}")
print(f"{'':15} {'INT':>8} {'DEC1':>8} {'DEC2':>8}  | {'INT':>8} {'DEC1':>8} {'DEC2':>8}")
print("-" * 90)

s_ai = s_ad1 = s_ad2 = 0
s_bi = s_bd1 = s_bd2 = 0
n = len(results)

for r in results:
    ai, ad1, ad2 = r["avg_int"], r["avg_dec1"], r["avg_dec2"]
    bi, bd1, bd2 = r["best_int"], r["best_dec1"], r["best_dec2"]
    s_ai += ai; s_ad1 += ad1; s_ad2 += ad2
    s_bi += bi; s_bd1 += bd1; s_bd2 += bd2
    print(f"{r['city']:<15} {ai:8.1f} {ad1:8.1f} {ad2:8.1f}  | {bi:8.1f} {bd1:8.1f} {bd2:8.1f}")

print("-" * 90)
print(f"{'MEDIA':15} {s_ai/n:8.1f} {s_ad1/n:8.1f} {s_ad2/n:8.1f}  | {s_bi/n:8.1f} {s_bd1/n:8.1f} {s_bd2/n:8.1f}")

print()
print("=" * 105)
print("ANALISI DISCREPANZE: casi dove INT e DEC2 danno verdetti diversi")
print("=" * 105)
print()

for city_name in CITIES:
    if city_name not in xl_st.sheet_names:
        continue
    cache_file = os.path.join(CACHE_DIR, f"{city_name}.pkl")
    if not os.path.exists(cache_file):
        continue
    raw_df = pd.read_pickle(cache_file)
    raw_df["time"] = pd.to_datetime(raw_df["time"])
    daily = compute_daily_max(raw_df)
    df_st = pd.read_excel(xl_st, sheet_name=city_name)
    df_st["Data"] = pd.to_datetime(df_st["Data"]).dt.date
    merged = pd.merge(daily, df_st[["Data", "Max_Temperatura_F"]], on="Data", how="inner")

    col = "1GG_best_match"
    if col not in merged.columns:
        continue

    actual_f = merged["Max_Temperatura_F"]
    forecast_f = merged[col].round(0)

    actual_c = (actual_f - 32) * 5 / 9
    forecast_c = (forecast_f - 32) * 5 / 9

    delta_int = forecast_c.round(0) - actual_c.round(0)
    delta_dec = forecast_c - actual_c

    valid = delta_int.notna() & delta_dec.notna()

    # INT verde ma DEC rosso (INT "regala" un verde che in realta non c'e?)
    igdr = ((delta_int.abs() <= 1) & (delta_dec.abs() > 1) & valid).sum()
    # INT rosso ma DEC verde (INT "toglie" un verde che in realta c'e)
    irdg = ((delta_int.abs() > 1) & (delta_dec.abs() <= 1) & valid).sum()
    n_tot = valid.sum()

    print(f"  {city_name:<15} INT verde/DEC rosso: {igdr:3d} ({igdr/n_tot*100:4.1f}%)   "
          f"INT rosso/DEC verde: {irdg:3d} ({irdg/n_tot*100:4.1f}%)   "
          f"Netto favore INT: {igdr - irdg:+d}")

print()
print("=" * 105)
print("IL VERO TEST: quale approccio predice meglio il BUCKET Polymarket?")
print("=" * 105)
print()
print("Polymarket: bucket = grado intero C. Scommetti su 2 bucket adiacenti.")
print("Vinci se actual_bucket in {forecast_bucket, forecast_bucket +/- 1}")
print("Questo e ESATTAMENTE il delta intero |round(fc) - round(ac)| <= 1")
print()
print("Con DEC2: |fc_preciso - ac_preciso| <= 1.0 NON misura i bucket.")
print("Esempio: actual=20.4C forecast=21.6C")
print("  DEC2: delta=1.2 -> ROSSO")
print("  INT:  round(20.4)=20 round(21.6)=22 delta=2 -> ROSSO")
print("  Bucket: actual=20, forecast=22, 2 bucket lontano -> ROSSO")
print("  -> Tutti concordano")
print()
print("Esempio: actual=20.6C forecast=21.4C")
print("  DEC2: delta=0.8 -> VERDE")
print("  INT:  round(20.6)=21 round(21.4)=21 delta=0 -> VERDE")
print("  Bucket: actual=21, forecast=21, stesso bucket -> VERDE")
print("  -> Tutti concordano")
print()
print("Esempio CRITICO: actual=19.6C forecast=21.4C")
print("  DEC2: delta=1.8 -> ROSSO")
print("  INT:  round(19.6)=20 round(21.4)=21 delta=1 -> VERDE")
print("  Bucket: actual=20, forecast=21, 1 bucket lontano")
print("  Con strategia 2-bucket (21 e 20): VINCI -> INT ha ragione!")
print()
print("Esempio CRITICO 2: actual=20.4C forecast=21.5C")
print("  DEC2: delta=1.1 -> ROSSO")
print("  INT:  round(20.4)=20 round(21.5)=22 delta=2 -> ROSSO")
print("  Bucket: actual=20, forecast=22, 2 bucket lontano")
print("  Con strategia 2-bucket (22 e 21): actual=20, PERDI -> entrambi giusti")
print()
print("=" * 105)
print("VERDETTO FINALE")
print("=" * 105)
print()
print("INT (intero) e il metodo CORRETTO per Polymarket perche:")
print("  1. Polymarket usa bucket interi in Celsius")
print("  2. Il delta intero misura esattamente 'quanti bucket di distanza'")
print("  3. |delta_int| <= 1 significa 'il forecast e nello stesso bucket")
print("     o in quello adiacente' -> con 2 scommesse copri il range")
print("  4. DEC sovrastima gli errori (delta=1.2C puo essere 1 solo bucket)")
print("     e sottostima altri (delta=0.8C puo essere 0 bucket o 1)")
print()
print("NOTA: i valori DECIMALI possono essere utili per un uso diverso:")
print("  - Calcolare probabilita per bucket (non solo verde/rosso)")
print("  - Stimare la confidenza di un forecast")
print("  - Ma per il verde/rosso di trading: INTERO e migliore")
