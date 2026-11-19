import pandas as pd
import sys
import os
from datetime import datetime
from dateutil import parser
import numpy as np
import math

# This script matches IFCB sample timestamps with GPS data to add latitude and longitude to the sample metadata.
# When there is information in the CollectionTime column of the metadata file, it will be used to match with the GPS data.
# SampleTime = Name of the timestamp column in the sample metadata file (e.g., "DateTime")
# Usage:
# python mergeMetadata_GPS_TSG.py /path/to/gps.xlsx /path/to/tsg.xlsx /path/to/sample.xlsx SampleTime /path/to/output_basename
# This will create /path/to/output_basename.xlsx


def dmm_to_dd(dmm, hemisphere=None):
    if pd.isna(dmm):
        return np.nan

    value = float(dmm)
    abs_value = abs(value)
    
    # Detect if already in decimal degrees format
    # DD format: typically -180 to 180, may have many decimal places (e.g., -122.395588)
    # DMM format: typically 0-18000 for lon, 0-9000 for lat, with format like DDDMM.MMMM
    
    # If the value is > 180, it must be in DMM format (can't be DD)
    if abs_value > 180:
        # Definitely in DMM format - convert to DD
        degrees = int(abs_value // 100)
        minutes = abs_value - (degrees * 100)
        dd = degrees + (minutes / 60)
    else:
        # Already in decimal degrees format
        dd = abs_value

    if hemisphere in ['S', 's', 'W', 'w']:
        dd = -dd
    elif value < 0:
        dd = -dd

    return dd


def merge_gps_tsg_metadata(gps_file, tsg_file, sample_file, sample_time_col, output_file):
    """
    Merge GPS and TSG data with IFCB sample metadata based on timestamp matching.
    
    Parameters:
    -----------
    gps_file : str
        Path to Excel file with combined GPS data
    tsg_file : str
        Path to Excel file with combined TSG data
    sample_file : str
        Path to Excel file with IFCB sample metadata
    sample_time_col : str
        Name of the timestamp column in the sample metadata file
    output_file : str
        Path where the merged output Excel file will be saved
    """
    
    # === Load GPS data ===
    gps_df = pd.read_excel(gps_file)

    # === Load TSG data ===
    tsg_df = pd.read_excel(tsg_file)
    tsg_df['datetime'] = pd.to_datetime(tsg_df['Datetime'], errors='coerce')

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

    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.isdir(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    # Remove any extension from output_file if present and add .xlsx
    output_base = os.path.splitext(output_file)[0]
    excel_output = f"{output_base}.xlsx"
    
    # === Output result ===
    samples_df.to_excel(excel_output, index=False)

    print(f"✅ Matching complete. Output saved to '{excel_output}'.")


if __name__ == "__main__":
    # Check if paths are provided as command-line arguments
    if len(sys.argv) == 6:
        gps_file_path = sys.argv[1]
        tsg_file_path = sys.argv[2]
        sample_file_path = sys.argv[3]
        sample_time_column = sys.argv[4]
        output_file_path = sys.argv[5]
    elif len(sys.argv) == 5:
        # If only 5 arguments provided, use 'DateTime' as default for sample_time_col
        gps_file_path = sys.argv[1]
        tsg_file_path = sys.argv[2]
        sample_file_path = sys.argv[3]
        output_file_path = sys.argv[4]
        sample_time_column = 'DateTime'
    else:
        # Default paths
        gps_file_path = ''
        tsg_file_path = ''
        sample_file_path = ''
        sample_time_column = 'DateTime'
        output_file_path = ''
    
    # Validate input files exist
    if not os.path.isfile(gps_file_path):
        print(f"Error: GPS file does not exist: {gps_file_path}")
        sys.exit(1)
    if not os.path.isfile(tsg_file_path):
        print(f"Error: TSG file does not exist: {tsg_file_path}")
        sys.exit(1)
    if not os.path.isfile(sample_file_path):
        print(f"Error: Sample file does not exist: {sample_file_path}")
        sys.exit(1)
    
    merge_gps_tsg_metadata(gps_file_path, tsg_file_path, sample_file_path, sample_time_column, output_file_path)
