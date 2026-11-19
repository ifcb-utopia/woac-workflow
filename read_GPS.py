import pandas as pd
import sys
import os
from pathlib import Path

# This script processes GPS GGA files from a specified folder and combines the data into an Excel file.
# Usage:
# python read_GPS.py /path/to/gps/folder /path/to/output.xlsx


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


def process_gps_data(gps_folder, output_file):
    """
    Process GPS GGA files and save combined data to Excel.
    
    Parameters:
    -----------
    gps_folder : str
        Path to the folder containing GPS .Raw files
    output_file : str
        Path where the output Excel file will be saved
    """
    
    gps_folder = Path(gps_folder)
    
    # Look for GPS GGA files with 'GP33-GGA' in filename
    gps_files = sorted([f for f in gps_folder.glob('*.Raw') if 'GP33-GGA' in f.name])

    if not gps_files:
        print(f"⚠️  No GPS files found in {gps_folder}")
        return

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

    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.isdir(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    # Save to Excel
    combined_df.to_excel(output_file, index=False)

    print(f"✅ Combined GPS data saved to '{output_file}'.")
    print(f"Processed {len(gps_files)} GPS files")
    print(f"Total GPS records: {len(all_rows)}")


if __name__ == "__main__":
    # Check if paths are provided as command-line arguments
    if len(sys.argv) == 3:
        gps_folder_path = sys.argv[1]
        output_file_path = sys.argv[2]
    else:
        # Default paths
        gps_folder_path = ''
        output_file_path = ''
    
    # Validate input folder exists
    if not os.path.isdir(gps_folder_path):
        print(f"Error: GPS folder does not exist: {gps_folder_path}")
        sys.exit(1)
    
    process_gps_data(gps_folder_path, output_file_path)
