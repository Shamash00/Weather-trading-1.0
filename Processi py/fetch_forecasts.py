import pandas as pd
import urllib.request
import json
import time
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
HISTORICAL_FILE = os.path.join(BASE_DIR, "Temperature Storiche", "0 Temperature FM-15 Tutte le Citta 2021-2025.xlsx")

# Airport station coordinates for each city
CITIES = {
    "Ankara":      {"lat": 40.128082, "lon": 32.995083},
    "Atlanta":     {"lat": 33.62972, "lon": -84.44224},
    "BuenosAires": {"lat": -34.822222, "lon": -58.535833},
    "Chicago":     {"lat": 41.96019, "lon": -87.93162},
    "Dallas":      {"lat": 32.8519, "lon": -96.8555},
    "Londra":      {"lat": 51.505278, "lon": 0.055278},
    "Lucknow":     {"lat": 26.7606, "lon": 80.8893},
    "Miami":       {"lat": 25.78805, "lon": -80.31694},
    "Monaco":      {"lat": 48.353783, "lon": 11.786086},
    "New York":    {"lat": 40.77945, "lon": -73.88027},
    "Parigi":      {"lat": 49.012779, "lon": 2.55},
    "SaoPaulo":    {"lat": -23.432075, "lon": -46.469511},
    "Seattle":     {"lat": 47.4444, "lon": -122.3138},
    "Seoul":       {"lat": 37.469075, "lon": 126.450517},
    "Shanghai":    {"lat": 31.143378, "lon": 121.805214},
    "Singapore":   {"lat": 1.350189, "lon": 103.994433},
    "TelAviv":     {"lat": 32.011389, "lon": 34.886667},
    "Tokyo":       {"lat": 35.552258, "lon": 139.779694},
    "Toronto":     {"lat": 43.677223, "lon": -79.630556},
    "Wellington":  {"lat": -41.3333333, "lon": 174.8},
}

API_BASE = "https://previous-runs-api.open-meteo.com/v1/forecast"
PARAMS = "hourly=temperature_2m,temperature_2m_previous_day1,temperature_2m_previous_day2&models=best_match&timezone=auto&temperature_unit=fahrenheit"

# Read date ranges from historical file
print("Lettura date storiche...")
xl = pd.ExcelFile(HISTORICAL_FILE)
date_ranges = {}
for sheet in xl.sheet_names:
    df = pd.read_excel(xl, sheet_name=sheet, usecols=[0])
    col = df.columns[0]
    start = str(df[col].min())[:10]
    end = str(df[col].max())[:10]
    date_ranges[sheet] = (start, end)
    print(f"  {sheet}: {start} -> {end}")

# Fetch forecast data for each city
out_path = os.path.join(BASE_DIR, "Previsioni FM-15 Tutte le Citta 2021-2025 v3.xlsx")

with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
    for city_name in xl.sheet_names:
        if city_name not in CITIES:
            print(f"  SKIP {city_name}: coordinate non definite")
            continue

        coords = CITIES[city_name]
        start_date, end_date = date_ranges[city_name]

        url = f"{API_BASE}?latitude={coords['lat']}&longitude={coords['lon']}&{PARAMS}&start_date={start_date}&end_date={end_date}"

        print(f"\nScaricamento {city_name} ({coords['lat']}, {coords['lon']}) {start_date} -> {end_date}...")

        try:
            resp = urllib.request.urlopen(url, timeout=120)
            data = json.loads(resp.read())

            hourly = data["hourly"]
            df_out = pd.DataFrame({
                "Data_Ora_Locale": pd.to_datetime(hourly["time"]),
                "Forecast_F": hourly["temperature_2m"],
                "Forecast_PrevDay1_F": hourly["temperature_2m_previous_day1"],
                "Forecast_PrevDay2_F": hourly["temperature_2m_previous_day2"],
            })

            # Add Celsius columns
            df_out["Forecast_C"] = ((df_out["Forecast_F"] - 32) * 5 / 9).round(1)
            df_out["Forecast_PrevDay1_C"] = ((df_out["Forecast_PrevDay1_F"] - 32) * 5 / 9).round(1)
            df_out["Forecast_PrevDay2_C"] = ((df_out["Forecast_PrevDay2_F"] - 32) * 5 / 9).round(1)

            # Reorder columns
            df_out = df_out[["Data_Ora_Locale", "Forecast_C", "Forecast_F",
                             "Forecast_PrevDay1_C", "Forecast_PrevDay1_F",
                             "Forecast_PrevDay2_C", "Forecast_PrevDay2_F"]]

            df_out.to_excel(writer, sheet_name=city_name, index=False)
            print(f"  OK: {len(df_out)} righe")

        except Exception as e:
            print(f"  ERRORE {city_name}: {e}")

        # Small delay to be respectful to the API
        time.sleep(1)

print(f"\nFile salvato: {out_path}")
