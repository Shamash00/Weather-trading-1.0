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

# Famiglie seamless: seamless -> [componenti]
FAMILIES = {
    "icon_seamless":         ["icon_d2", "icon_eu", "icon_global"],
    "gfs_seamless":          ["gfs_hrrr", "gfs_global"],
    "gem_seamless":          ["gem_hrdps_continental", "gem_hrdps_west", "gem_regional", "gem_global"],
    "jma_seamless":          ["jma_msm", "jma_gsm"],
    "kma_seamless":          ["kma_ldps", "kma_gdps"],
    "meteoswiss_icon_seamless": ["meteoswiss_icon_ch1", "meteoswiss_icon_ch2"],
    "meteofrance_seamless":  ["meteofrance_arome_france_hd", "meteofrance_arome_france",
                              "meteofrance_arpege_europe", "meteofrance_arpege_world"],
    "ukmo_seamless":         ["ukmo_global_deterministic_10km", "ukmo_uk_deterministic_2km"],
    "knmi_seamless":         ["knmi_harmonie_arome_europe", "knmi_harmonie_arome_netherlands"],
    "dmi_seamless":          ["dmi_harmonie_arome_europe"],
    "metno_seamless":        ["metno_nordic"],
}

# Copertura alta risoluzione (approssimativa)
HIRES_COVERAGE = {
    "icon_d2": "Germania e limitrofi",
    "icon_eu": "Europa",
    "gfs_hrrr": "USA continentali",
    "gem_hrdps_continental": "Canada/Nord USA",
    "gem_hrdps_west": "Canada occidentale",
    "gem_regional": "Nord America",
    "jma_msm": "Giappone",
    "kma_ldps": "Corea del Sud",
    "meteoswiss_icon_ch1": "Svizzera (1km)",
    "meteoswiss_icon_ch2": "Svizzera (2km)",
    "meteofrance_arome_france_hd": "Francia (1.3km)",
    "meteofrance_arome_france": "Francia (2.5km)",
    "meteofrance_arpege_europe": "Europa",
    "ukmo_uk_deterministic_2km": "UK (2km)",
    "ukmo_global_deterministic_10km": "Globale (10km)",
    "knmi_harmonie_arome_europe": "Europa",
    "knmi_harmonie_arome_netherlands": "Paesi Bassi",
    "dmi_harmonie_arome_europe": "Europa",
    "metno_nordic": "Scandinavia",
}


def compute_daily_max(hourly_df):
    hourly_df["Data"] = hourly_df["time"].dt.date
    prev1 = [c for c in hourly_df.columns if c.startswith("temperature_2m_previous_day1_")]
    agg = {c: "max" for c in prev1}
    daily = hourly_df.groupby("Data").agg(agg).reset_index()
    rename = {c: f"1GG_{c.replace('temperature_2m_previous_day1_', '')}" for c in prev1}
    daily.rename(columns=rename, inplace=True)
    return daily


xl_st = pd.ExcelFile(storico_path)

print("=" * 120)
print("CONFRONTO SEAMLESS vs COMPONENTI INDIVIDUALI")
print("=" * 120)
print()
print("Per ogni famiglia, confrontiamo il modello seamless con i suoi componenti.")
print("Verde% = |delta_C_intero| <= 1")
print()

all_rows = []

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

    reg_c = ((merged["Max_Temperatura_F"] - 32) * 5 / 9).round(0)

    # Calcola verde% per ogni modello disponibile
    model_verde = {}
    model_n = {}
    model_bias = {}
    for c in merged.columns:
        if not c.startswith("1GG_"):
            continue
        name = c.replace("1GG_", "")
        stima = ((merged[c].round(0) - 32) * 5 / 9).round(0)
        delta = stima - reg_c
        valid = delta.dropna()
        if len(valid) < 10:
            continue
        model_verde[name] = round((valid.abs() <= 1).sum() / len(valid) * 100, 1)
        model_n[name] = len(valid)
        model_bias[name] = round(float(valid.mean()), 2)

    print(f"\n{'-' * 120}")
    print(f"  {city_name}")
    print(f"{'-' * 120}")

    for seamless, components in FAMILIES.items():
        if seamless not in model_verde:
            continue

        v_seamless = model_verde[seamless]
        n_seamless = model_n[seamless]
        b_seamless = model_bias[seamless]

        available_components = [c for c in components if c in model_verde]

        if not available_components:
            print(f"  {seamless:<35} V%={v_seamless:5.1f}%  N={n_seamless:4d}  Bias={b_seamless:+.2f}  "
                  f"(nessun componente disponibile)")
            all_rows.append({
                "Citta": city_name, "Famiglia": seamless,
                "Modello": seamless, "Tipo": "seamless",
                "Verde%": v_seamless, "N": n_seamless, "Bias": b_seamless,
            })
            continue

        # Trova il miglior componente
        best_comp = max(available_components, key=lambda c: model_verde[c])
        v_best = model_verde[best_comp]

        diff = v_seamless - v_best
        winner = "SEAMLESS" if diff > 0 else ("PARI" if diff == 0 else best_comp.upper())

        print(f"  {seamless:<35} V%={v_seamless:5.1f}%  N={n_seamless:4d}  Bias={b_seamless:+.2f}")
        all_rows.append({
            "Citta": city_name, "Famiglia": seamless,
            "Modello": seamless, "Tipo": "seamless",
            "Verde%": v_seamless, "N": n_seamless, "Bias": b_seamless,
        })

        for comp in components:
            if comp not in model_verde:
                cov = HIRES_COVERAGE.get(comp, "")
                print(f"    {comp:<33} N/A  (copertura: {cov} - fuori range per {city_name})")
                continue
            v_c = model_verde[comp]
            n_c = model_n[comp]
            b_c = model_bias[comp]
            d = v_c - v_seamless
            marker = " <<<" if comp == best_comp and d != 0 else ""
            cov = HIRES_COVERAGE.get(comp, "globale")
            print(f"    {comp:<33} V%={v_c:5.1f}%  N={n_c:4d}  Bias={b_c:+.2f}  "
                  f"(diff vs seamless: {d:+.1f}%)  [{cov}]{marker}")
            all_rows.append({
                "Citta": city_name, "Famiglia": seamless,
                "Modello": comp, "Tipo": "componente",
                "Verde%": v_c, "N": n_c, "Bias": b_c,
                "Copertura": cov,
                "Diff vs Seamless": d,
            })

        print(f"    >>> Vincitore: {winner} (diff: {diff:+.1f}%)")
        print()

# Riepilogo globale
print()
print("=" * 120)
print("RIEPILOGO: QUANDO CONVIENE SEAMLESS vs COMPONENTE?")
print("=" * 120)
print()

df = pd.DataFrame(all_rows)
for seamless_name in FAMILIES.keys():
    family_data = df[df["Famiglia"] == seamless_name]
    if family_data.empty:
        continue

    seamless_rows = family_data[family_data["Tipo"] == "seamless"]
    comp_rows = family_data[family_data["Tipo"] == "componente"]

    if seamless_rows.empty:
        continue

    print(f"  {seamless_name}")
    wins_seamless = 0
    wins_comp = 0
    ties = 0

    for city in seamless_rows["Citta"].unique():
        v_s = seamless_rows[seamless_rows["Citta"] == city]["Verde%"].values[0]
        city_comps = comp_rows[comp_rows["Citta"] == city]
        if city_comps.empty:
            continue
        v_best_comp = city_comps["Verde%"].max()
        best_comp_name = city_comps.loc[city_comps["Verde%"].idxmax(), "Modello"]

        if v_s > v_best_comp:
            wins_seamless += 1
        elif v_s < v_best_comp:
            wins_comp += 1
        else:
            ties += 1

    total = wins_seamless + wins_comp + ties
    if total > 0:
        print(f"    Seamless vince: {wins_seamless}/{total} citta ({wins_seamless/total*100:.0f}%)")
        print(f"    Componente vince: {wins_comp}/{total} citta ({wins_comp/total*100:.0f}%)")
        if ties:
            print(f"    Pari: {ties}/{total}")
    print()
