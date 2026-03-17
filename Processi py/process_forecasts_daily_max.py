import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
in_path = os.path.join(BASE_DIR, "Previsioni FM-15 Tutte le Citta 2021-2025.xlsx")
out_path = os.path.join(BASE_DIR, "Previsioni FM-15 Tutte le Citta 2021-2025 Daily Max.xlsx")

print("Caricamento file...")
xl = pd.ExcelFile(in_path)

with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
    for sheet in xl.sheet_names:
        print(f"\n{sheet}...")
        df = pd.read_excel(xl, sheet_name=sheet)
        df["Data_Ora_Locale"] = pd.to_datetime(df["Data_Ora_Locale"])

        # Filter: from April 6, 2021 onwards
        df = df[df["Data_Ora_Locale"] >= "2021-04-06"].copy()

        # Extract date column for grouping
        df["Data"] = df["Data_Ora_Locale"].dt.date

        # Daily max for each forecast type
        daily = df.groupby("Data").agg(
            Max_Forecast_C=("Forecast_C", "max"),
            Max_Forecast_F=("Forecast_F", "max"),
            Max_PrevDay1_C=("Forecast_PrevDay1_C", "max"),
            Max_PrevDay1_F=("Forecast_PrevDay1_F", "max"),
            Max_PrevDay2_C=("Forecast_PrevDay2_C", "max"),
            Max_PrevDay2_F=("Forecast_PrevDay2_F", "max"),
        ).reset_index()

        # Round Celsius to 1 decimal, Fahrenheit to integer (standard rounding)
        daily["Max_Forecast_C"] = daily["Max_Forecast_C"].round(1)
        daily["Max_PrevDay1_C"] = daily["Max_PrevDay1_C"].round(1)
        daily["Max_PrevDay2_C"] = daily["Max_PrevDay2_C"].round(1)

        daily["Max_Forecast_F"] = daily["Max_Forecast_F"].round(0).astype("Int64")
        daily["Max_PrevDay1_F"] = daily["Max_PrevDay1_F"].round(0).astype("Int64")
        daily["Max_PrevDay2_F"] = daily["Max_PrevDay2_F"].round(0).astype("Int64")

        daily.to_excel(writer, sheet_name=sheet, index=False)
        print(f"  {len(daily)} giorni (da {daily['Data'].iloc[0]} a {daily['Data'].iloc[-1]})")

print(f"\nFile salvato: {out_path}")
