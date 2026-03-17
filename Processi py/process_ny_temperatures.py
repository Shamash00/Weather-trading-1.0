import pandas as pd
from zoneinfo import ZoneInfo

# Load all sheets and concatenate
print("Caricamento file...")
xl = pd.ExcelFile("Temperature Registrate Aereoporto New York 2021-2025.xlsx")
print(f"Fogli trovati: {xl.sheet_names}")

frames = []
for sheet in xl.sheet_names:
    sheet_df = pd.read_excel(xl, sheet_name=sheet, dtype={"TMP": str})
    print(f"  Foglio {sheet}: {len(sheet_df)} righe")
    frames.append(sheet_df)

df = pd.concat(frames, ignore_index=True)
print(f"Righe totali: {len(df)}")

# Filter FM-15 only
df_fm15 = df[df["REPORT_TYPE"].str.strip() == "FM-15"].copy()
print(f"Righe FM-15: {len(df_fm15)}")

# Parse TMP: format is e.g. "50.5" where integer part = temp*10, decimal = quality flag
# Take integer part (split on '.'), divide by 10 to get Celsius
INVALID_TMP = {"9999.9", "9999", "999.9", "999", ""}

def parse_tmp(val):
    if pd.isna(val) or str(val).strip() in INVALID_TMP:
        return None
    try:
        s = str(val).strip()
        int_part = int(s.split(".")[0])
        celsius = int_part / 10.0
        # Sanity check: realistic temperature range -80°C to +60°C
        if celsius < -80 or celsius > 60:
            return None
        return celsius
    except Exception:
        return None

df_fm15["TMP_C"] = df_fm15["TMP"].apply(parse_tmp)

# Convert DATE from UTC to New York local time (America/New_York handles EST/EDT automatically)
utc = ZoneInfo("UTC")
ny_tz = ZoneInfo("America/New_York")

df_fm15["DATE_UTC"] = pd.to_datetime(df_fm15["DATE"], utc=True)
df_fm15["DATE_LOCAL"] = df_fm15["DATE_UTC"].dt.tz_convert(ny_tz)

# Build clean output
output = df_fm15[["DATE_LOCAL", "TMP_C"]].copy()
output.columns = ["Data_Ora_Locale_NY", "Temperatura_C"]

# Add Fahrenheit column
output["Temperatura_F"] = (output["Temperatura_C"] * 9 / 5 + 32).round(1)

# Remove rows with missing temperature
output = output.dropna(subset=["Temperatura_C"])

# Sort by date
output = output.sort_values("Data_Ora_Locale_NY").reset_index(drop=True)

# Remove timezone info for cleaner Excel output (keep the local time value)
output["Data_Ora_Locale_NY"] = output["Data_Ora_Locale_NY"].dt.tz_localize(None)

print(f"Righe in output: {len(output)}")
print("\nPrime 10 righe:")
print(output.head(10).to_string())

# Save to Excel
out_path = "Temperature FM-15 New York Orario Locale 2021-2025_v2.xlsx"
output.to_excel(out_path, index=False)
print(f"\nFile salvato: {out_path}")
