import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
in_path = os.path.join(BASE_DIR, "Temperature Storiche", "0 Temperature FM-15 Tutte le Citta 2021-2025.xlsx")
out_path = os.path.join(BASE_DIR, "Temperature Storiche FM-15 Tutte le Citta 2021-2025 Daily Max.xlsx")

print("Caricamento file...")
xl = pd.ExcelFile(in_path)

with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
    for sheet in xl.sheet_names:
        print(f"\n{sheet}...")
        df = pd.read_excel(xl, sheet_name=sheet)

        # First column is always the datetime (name varies per city)
        date_col = df.columns[0]
        df[date_col] = pd.to_datetime(df[date_col])

        # Filter: from April 6, 2021 onwards
        df = df[df[date_col] >= "2021-04-06"].copy()

        # Extract date for grouping
        df["Data"] = df[date_col].dt.date

        # Daily max
        daily = df.groupby("Data").agg(
            Max_Temperatura_C=("Temperatura_C", "max"),
            Max_Temperatura_F=("Temperatura_F", "max"),
        ).reset_index()

        # Round Celsius to 1 decimal, Fahrenheit to integer
        daily["Max_Temperatura_C"] = daily["Max_Temperatura_C"].round(1)
        daily["Max_Temperatura_F"] = daily["Max_Temperatura_F"].round(0).astype("Int64")

        daily.to_excel(writer, sheet_name=sheet, index=False)
        print(f"  {len(daily)} giorni (da {daily['Data'].iloc[0]} a {daily['Data'].iloc[-1]})")

print(f"\nFile salvato: {out_path}")
