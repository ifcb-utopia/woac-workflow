import pandas as pd
from pathlib import Path


def dmm_to_dd(dmm, hemisphere=None):
    if pd.isna(dmm):
        return pd.NA

    value = float(dmm)
    if abs(value) >= 100:
        degrees = int(abs(value) // 100)
        minutes = abs(value) - (degrees * 100)
        dd = degrees + (minutes / 60)
    else:
        dd = value

    if hemisphere in ['S', 's', 'W', 'w']:
        dd = -dd

    return dd


# Folder containing your GPS files
gps_folder = Path('/Users/alisonchase/Library/CloudStorage/OneDrive-UW/SalishSea_WOAC/IFCB/April2026/RC0148scs/NAV_GP33')
# Output file name
output_file = '/Users/alisonchase/Library/CloudStorage/OneDrive-UW/SalishSea_WOAC/IFCB/April2026/combined_gps_data.xlsx'

# Look for GPS GGA files with 'GP33-GGA' in filename
gps_files = sorted([f for f in gps_folder.glob('*.Raw') if 'GP33-GGA' in f.name])

all_rows = []

for file in gps_files:
    with open(file, 'r') as f:
        for line in f:
            parts = line.strip().split(',')
            if len(parts) >= 8 and parts[2] == '$GPGGA':
                date = parts[0]
                time = parts[1]
                lat = parts[4]
                lat_hem = parts[5]
                lon = parts[6]
                lon_hem = parts[7]
                
                # Skip lines with empty lat/lon
                if lat and lon:
                    datetime = pd.to_datetime(f'{date} {time}', format='%m/%d/%Y %H:%M:%S.%f', errors='coerce')
                    lat_dd = dmm_to_dd(lat, lat_hem)
                    lon_dd = dmm_to_dd(lon, lon_hem)
                    all_rows.append([datetime, date, time, lat_dd, lat_hem, lon_dd, lon_hem])

# Create DataFrame
combined_df = pd.DataFrame(all_rows, columns=['Datetime', 'date', 'time', 'lat', 'lat_hem', 'lon', 'lon_hem'])

# Save to Excel
combined_df.to_excel(output_file, index=False)

print(f"✅ Combined GPS data saved to '{output_file}'.")
print(f"Processed {len(gps_files)} GPS files")
print(f"Total GPS records: {len(all_rows)}")
