import pandas as pd
import glob
import os
from zoneinfo import ZoneInfo

# Load all CSV files starting with 07157099999
base_dir = os.path.dirname(os.path.abspath(__file__))
files = sorted(glob.glob(os.path.join(base_dir, "07157099999*.csv")))
print(f"File trovati: {len(files)}")

frames = []
for f in files:
    sheet_df = pd.read_csv(f, dtype={"TMP": str})
    print(f"  {os.path.basename(f)}: {len(sheet_df)} righe")
    frames.append(sheet_df)

df = pd.concat(frames, ignore_index=True)
print(f"Righe totali: {len(df)}")

# Filter FM-15 only
df_fm15 = df[df["REPORT_TYPE"].str.strip() == "FM-15"].copy()
print(f"Righe FM-15: {len(df_fm15)}")

# Parse TMP: format is "+0110,1" where value before comma = temp*10, after comma = quality flag
# Also handles invalid values like "+9999,9"
INVALID_INT = {9999, -9999, 999, -999}

def parse_tmp(val):
    if pd.isna(val):
        return None
    try:
        s = str(val).strip()
        # Split on comma, take the part before it (e.g. "+0110")
        raw = s.split(",")[0]
        int_part = int(raw)
        if int_part in INVALID_INT:
            return None
        celsius = int_part / 10.0
        # Sanity check: realistic temperature range -80°C to +60°C
        if celsius < -80 or celsius > 60:
            return None
        return celsius
    except Exception:
        return None

df_fm15["TMP_C"] = df_fm15["TMP"].apply(parse_tmp)

# Convert DATE from UTC to Paris local time (handles CET/CEST automatically)
paris_tz = ZoneInfo("Europe/Paris")

df_fm15["DATE_UTC"] = pd.to_datetime(df_fm15["DATE"], utc=True)
df_fm15["DATE_LOCAL"] = df_fm15["DATE_UTC"].dt.tz_convert(paris_tz)

# Build clean output
output = df_fm15[["DATE_LOCAL", "TMP_C"]].copy()
output.columns = ["Data_Ora_Locale_Parigi", "Temperatura_C"]

# Add Fahrenheit column
output["Temperatura_F"] = (output["Temperatura_C"] * 9 / 5 + 32).round(1)

# Remove rows with missing temperature
output = output.dropna(subset=["Temperatura_C"])

# Sort by date
output = output.sort_values("Data_Ora_Locale_Parigi").reset_index(drop=True)

# Remove timezone info for cleaner Excel output
output["Data_Ora_Locale_Parigi"] = output["Data_Ora_Locale_Parigi"].dt.tz_localize(None)

print(f"Righe in output: {len(output)}")
print("\nPrime 10 righe:")
print(output.head(10).to_string())

# Save to Excel
out_path = os.path.join(base_dir, "Temperature FM-15 Parigi Orario Locale 2021-2025.xlsx")
output.to_excel(out_path, index=False)
print(f"\nFile salvato: {out_path}")
