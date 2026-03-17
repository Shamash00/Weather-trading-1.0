import pandas as pd
import glob
import os
from zoneinfo import ZoneInfo

# Load all CSV files starting with 87576099999
base_dir = os.path.dirname(os.path.abspath(__file__))
files = sorted(glob.glob(os.path.join(base_dir, "87576099999*.csv")))
print(f"File trovati: {len(files)}")

frames = []
for f in files:
    sheet_df = pd.read_csv(f, dtype={"TMP": str}, low_memory=False)
    print(f"  {os.path.basename(f)}: {len(sheet_df)} righe")
    frames.append(sheet_df)

df = pd.concat(frames, ignore_index=True)
print(f"Righe totali: {len(df)}")

# Filter FM-15 only
df_fm15 = df[df["REPORT_TYPE"].str.strip() == "FM-15"].copy()
print(f"Righe FM-15: {len(df_fm15)}")

# Parse TMP: format is "+0280,1" where value before comma = temp*10, after comma = quality flag
INVALID_INT = {9999, -9999, 999, -999}

def parse_tmp(val):
    if pd.isna(val):
        return None
    try:
        s = str(val).strip()
        raw = s.split(",")[0]
        int_part = int(raw)
        if int_part in INVALID_INT:
            return None
        celsius = int_part / 10.0
        if celsius < -80 or celsius > 60:
            return None
        return celsius
    except Exception:
        return None

df_fm15["TMP_C"] = df_fm15["TMP"].apply(parse_tmp)

# Convert DATE from UTC to Buenos Aires local time (ART = UTC-3, no DST)
ba_tz = ZoneInfo("America/Argentina/Buenos_Aires")

df_fm15["DATE_UTC"] = pd.to_datetime(df_fm15["DATE"], utc=True)
df_fm15["DATE_LOCAL"] = df_fm15["DATE_UTC"].dt.tz_convert(ba_tz)

# Build clean output
output = df_fm15[["DATE_LOCAL", "TMP_C"]].copy()
output.columns = ["Data_Ora_Locale_BuenosAires", "Temperatura_C"]

# Add Fahrenheit column
output["Temperatura_F"] = (output["Temperatura_C"] * 9 / 5 + 32).round(1)

# Remove rows with missing temperature
output = output.dropna(subset=["Temperatura_C"])

# Sort by date
output = output.sort_values("Data_Ora_Locale_BuenosAires").reset_index(drop=True)

# Remove timezone info for cleaner Excel output
output["Data_Ora_Locale_BuenosAires"] = output["Data_Ora_Locale_BuenosAires"].dt.tz_localize(None)

print(f"Righe in output: {len(output)}")
print("\nPrime 10 righe:")
print(output.head(10).to_string())

# Save to Excel
out_path = os.path.join(base_dir, "Temperature FM-15 BuenosAires Orario Locale 2021-2025.xlsx")
output.to_excel(out_path, index=False)
print(f"\nFile salvato: {out_path}")
