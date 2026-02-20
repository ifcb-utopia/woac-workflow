import pandas as pd
from datetime import datetime
from dateutil import parser
import numpy as np
import math
import os

# This script matches IFCB sample timestamps with GPS data to add latitude and longitude to the sample metadata.
# When there is information in the CollectionTime column of the metadata file, it will be used to match with the GPS data.

# === User Inputs ===
gps_file = 'Sept2025/combined_gps_data.xlsx'             # text file with date, time, lat, lon
tsg_file = 'Sept2025/combined_tsg_data.xlsx'             # text file with TSG data
sample_file = 'Sept2025/WOAC-Sept2025-metadata_edit.xlsx'     # text file with sample timestamps
sample_time_col = 'DateTime'        # name of the timestamp column in Excel
output_file = 'Sept2025/WOAC-Sept2025-metadata_edit_GPS_TSG.xlsx'  # output file name

# === Load GPS data ===
# Adjust delimiter if needed (e.g., ',' or '\t')
gps_df = pd.read_excel(gps_file)

# === Load TSG data ===
tsg_df = pd.read_excel(tsg_file)
tsg_df['datetime'] = pd.to_datetime(tsg_df['Datetime'], errors='coerce')

def dmm_to_dd(dmm, hemisphere=None):
    deg = int(dmm) // 100
    minutes = dmm - (deg * 100)
    dd = deg + (minutes / 60)
    if hemisphere in ['S', 'W']:
        dd = -dd
    return dd

gps_df['lat_dd'] = gps_df.apply(lambda row: dmm_to_dd(row['lat'], row.get('lat_hem')), axis=1)
gps_df['lon_dd'] = gps_df.apply(lambda row: dmm_to_dd(row['lon'], row.get('lon_hem')), axis=1)

# === Combine date and time into a single datetime ===
# GPS data already has properly formatted date and time
gps_df['datetime'] = pd.to_datetime(gps_df['date'] + ' ' + gps_df['time'], format='%m/%d/%Y %H:%M:%S.%f', errors='coerce')

# === Load IFCB sample timestamps and hdr filenames ===
samples_df = pd.read_excel(sample_file)
samples_df['datetime'] = pd.to_datetime(
    samples_df[sample_time_col],
    format="%m/%d/%y %H:%M",
    errors='coerce'
)

# Process CollectionTime column if it exists
if 'CollectionTime' in samples_df.columns:
    samples_df['collection_datetime'] = pd.to_datetime(
        samples_df['CollectionTime'],
        format="%m/%d/%y %H:%M",
        errors='coerce'
    )
else:
    samples_df['collection_datetime'] = pd.NaT

bad = samples_df[samples_df['datetime'].isna()]

samples_df = samples_df.dropna(subset=['datetime']).reset_index(drop=True)

# === Match each sample to closest GPS time ===
matched_lat = []
matched_lon = []

gps_times = gps_df['datetime'].values

for index, row in samples_df.iterrows():
    # Skip GPS matching for niskin samples
    if 'Type' in samples_df.columns and str(row['Type']).lower() == 'niskin':
        matched_lat.append(np.nan)
        matched_lon.append(np.nan)
        continue
    
    # Use CollectionTime if available and not null, otherwise use DateTime
    if pd.notna(row['collection_datetime']):
        sample_time = row['collection_datetime']
    else:
        sample_time = row['datetime']
    
    deltas = np.abs(gps_times - np.datetime64(sample_time))
    idx = deltas.argmin()
    matched_lat.append(gps_df.iloc[idx]['lat_dd'])
    matched_lon.append(gps_df.iloc[idx]['lon_dd'])

# Add to dataframe
samples_df['Latitude'] = matched_lat
samples_df['Longitude'] = matched_lon

# === Match each sample to closest TSG time ===
matched_temperature = []
matched_salinity = []

tsg_times = tsg_df['datetime'].values

for index, row in samples_df.iterrows():
    # Skip TSG matching for niskin samples
    if 'Type' in samples_df.columns and str(row['Type']).lower() == 'niskin':
        matched_temperature.append(np.nan)
        matched_salinity.append(np.nan)
        continue
    
    # Use CollectionTime if available and not null, otherwise use DateTime
    if pd.notna(row['collection_datetime']):
        sample_time = row['collection_datetime']
    else:
        sample_time = row['datetime']
    
    deltas = np.abs(tsg_times - np.datetime64(sample_time))
    idx = deltas.argmin()
    matched_temperature.append(tsg_df.iloc[idx]['Temperature'])
    matched_salinity.append(tsg_df.iloc[idx]['Salinity'])

# Add TSG data to dataframe
samples_df['Temperature'] = matched_temperature
samples_df['Salinity'] = matched_salinity

# drop the collection_datetime column
samples_df = samples_df.drop(columns=['collection_datetime'], errors='ignore')

# === Output result ===
samples_df.to_excel(output_file, index=False)

print(f"✅ Matching complete. Output saved to '{output_file}'.")
