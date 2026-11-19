import re
import pandas as pd
import sys
import os

# This script merges IFCB metadata with bottle data from CTD casts.
# It handles GPS coordinates, timestamps, and ensures proper formatting for merging.
# Usage:
# python mergeMetadata_bottle.py metadata.xlsx bottle.csv output.xlsx [ecotaxa_output.csv]

def dmm_to_dd(value):
    if pd.isna(value):
        return float('nan')

    text = str(value).strip()
    if not text:
        return float('nan')

    hemisphere = None
    if text[-1].upper() in ['N', 'S', 'E', 'W']:
        hemisphere = text[-1].upper()
        text = text[:-1].strip()

    if re.match(r'^[-+]?\d+(?:\.\d+)?$', text):
        numeric = float(text)
    else:
        parts = text.replace(',', ' ').split()
        if len(parts) >= 2:
            try:
                degrees = int(float(parts[0]))
                minutes = float(parts[1])
            except (ValueError, TypeError):
                return float('nan')

            numeric = degrees + (minutes / 60)
        else:
            try:
                numeric = float(text)
            except (ValueError, TypeError):
                return float('nan')

    if hemisphere in ['S', 'W']:
        numeric = -numeric

    return numeric


def merge_metadata_with_bottle(metadata_file, bottle_file, output_file, ecotaxa_file=None):
    """
    Merge IFCB metadata with bottle data from CTD casts.
    
    Parameters:
    -----------
    metadata_file : str
        Path to Excel file with IFCB sample timestamps, GPS, and TSG data
    bottle_file : str
        Path to CSV file with bottle times, locations, and water condition data
    output_file : str
        Path where the merged output Excel file will be saved
    ecotaxa_file : str, optional
        Path where the Ecotaxa-formatted output file will be saved
        If None, defaults to output_file with '-toEcotaxa' suffix
    """
    
    # Load IFCB metadata file
    metadata_df = pd.read_excel(metadata_file)
    # Check the actual column names and parse them safely
    # Parse datetime from metadata_df
    metadata_df['datetime'] = pd.to_datetime(metadata_df['DateTime'], errors='coerce', utc=True)  # coerce bad values to NaT
    metadata_df['datetime_local'] = metadata_df['datetime'].dt.tz_convert('America/Los_Angeles')
    metadata_df['merge_date'] = metadata_df['datetime_local'].dt.strftime('%m/%d/%y')

    # If Latitude/Longitude columns are missing, build them from NMEA degree-decimal-minute values.
    if 'Latitude' not in metadata_df.columns or 'Longitude' not in metadata_df.columns:
        if 'NMEAlat' in metadata_df.columns:
            metadata_df['Latitude'] = metadata_df['NMEAlat'].apply(dmm_to_dd)
        else:
            metadata_df['Latitude'] = float('nan')

        if 'NMEANlon' in metadata_df.columns:
            metadata_df['Longitude'] = metadata_df['NMEANlon'].apply(dmm_to_dd)
        else:
            metadata_df['Longitude'] = float('nan')

    # Remove decimal part *only if* the value is a whole number and not NaN
    metadata_df['Bottle'] = (
        metadata_df['Bottle']
        .apply(lambda x: str(int(x)) if pd.notnull(x) and x == int(x) else str(x))
    )
    metadata_df['Station'] = (
        metadata_df['Station']
        .apply(lambda x: str(int(x)) if pd.notnull(x) and x == int(x) else str(x))
    )
    metadata_df['Station'] = metadata_df['Station'].astype(str)

    # Copy DateTime values into CollectionTime column where CollectionTime is empty/null
    # This preserves existing CollectionTime values while filling in missing ones
    if 'CollectionTime' in metadata_df.columns:
        # Fill CollectionTime with DateTime values only where CollectionTime is null or empty
        metadata_df['CollectionTime'] = metadata_df['CollectionTime'].fillna(metadata_df['DateTime'])
        # Also handle empty strings
        mask = metadata_df['CollectionTime'].astype(str).str.strip() == ''
        metadata_df.loc[mask, 'CollectionTime'] = metadata_df.loc[mask, 'DateTime']
    else:
        # If CollectionTime column doesn't exist, create it from DateTime
        metadata_df['CollectionTime'] = metadata_df['DateTime']

    # Load bottle file
    bottle_df = pd.read_csv(bottle_file)
    bottle_df.columns = bottle_df.columns.str.strip()  # remove leading/trailing whitespace

    # Parse the 'date' column which looks like 'Apr 14 2025'
    bottle_df['date'] = pd.to_datetime(bottle_df['Date'].astype(str).str.strip(), format='%b %d %Y', errors='coerce')
    bottle_df['merge_date'] = bottle_df['date'].dt.strftime('%m/%d/%y')
    # Remove leading 'p' or 'P' and cast to string
    bottle_df['Station'] = bottle_df['Station'].astype(str).str.lower().str.replace('p', '', regex=False)

    # Ensure bottle numbers are same type
    metadata_df['Bottle'] = metadata_df['Bottle'].astype(str)
    bottle_df['Bottle'] = bottle_df['Bottle'].astype(str)

    # Strip spaces from Station
    metadata_df['Station'] = metadata_df['Station'].astype(str).str.strip()
    bottle_df['Station'] = bottle_df['Station'].astype(str).str.lower().str.replace('p', '', regex=False).str.strip()

    # Also strip Bottle and merge_date just to be safe
    metadata_df['Bottle'] = metadata_df['Bottle'].astype(str).str.strip()
    bottle_df['Bottle'] = bottle_df['Bottle'].astype(str).str.strip()

    metadata_df['merge_date'] = metadata_df['merge_date'].astype(str).str.strip()
    bottle_df['merge_date'] = bottle_df['merge_date'].astype(str).str.strip()

    # If bottle coordinates are missing, build them from NMEA degree-decimal-minute values.
    if 'Latitude' not in bottle_df.columns or 'Longitude' not in bottle_df.columns:
        if 'NMEAlat' in bottle_df.columns:
            bottle_df['Latitude'] = bottle_df['NMEAlat'].apply(dmm_to_dd)
        else:
            bottle_df['Latitude'] = float('nan')

        if 'NMEANlon' in bottle_df.columns:
            bottle_df['Longitude'] = bottle_df['NMEANlon'].apply(dmm_to_dd)
        else:
            bottle_df['Longitude'] = float('nan')

    # Extract latitude and longitude from bottle_df for later use
    bottle_coords = bottle_df[['merge_date', 'Bottle', 'Station', 'Latitude', 'Longitude']].copy()
    bottle_coords.columns = bottle_coords.columns.str.strip()  # ensure no spaces

    # Remove latitude and longitude from bottle_df to avoid conflicts during merge
    bottle_df = bottle_df.drop(columns=['Latitude', 'Longitude'])

    # Merge on simplified date and bottle number
    merged_df = pd.merge( 
        metadata_df,
        bottle_df, 
        on=['merge_date', 'Bottle', 'Station'],
        how='left'
    )

    # Update existing Latitude and Longitude columns with bottle data where available
    # First, merge with bottle coordinates
    coords_merged = pd.merge(
        merged_df,
        bottle_coords,
        on=['merge_date', 'Bottle', 'Station'],
        how='left',
        suffixes=('', '_bottle')
    )

    # Prefer bottle-derived Latitude and Longitude values when they are available.
    merged_df['Latitude'] = coords_merged['Latitude_bottle'].combine_first(merged_df['Latitude'])
    merged_df['Longitude'] = coords_merged['Longitude_bottle'].combine_first(merged_df['Longitude'])

    # Ensure Temperature and Salinity from metadata are preserved in the final output
    # These columns should already be present from the metadata_df (GPS_TSG file)
    if 'Temperature' in metadata_df.columns and 'Temperature' not in merged_df.columns:
        merged_df['Temperature'] = metadata_df['Temperature']
    if 'Salinity' in metadata_df.columns and 'Salinity' not in merged_df.columns:
        merged_df['Salinity'] = metadata_df['Salinity']

    # Fill CollectionTime with NMEAtimeUTC values for all niskin samples
    if 'Type' in merged_df.columns and 'NMEAtimeUTC' in merged_df.columns:
        niskin_mask = merged_df['Type'].str.lower() == 'niskin'
        merged_df.loc[niskin_mask, 'CollectionTime'] = merged_df.loc[niskin_mask, 'NMEAtimeUTC']

    # Copy DepSM values into Depth column when values exist, and round to nearest integer.
    # Also default inline-type rows to a depth of 2.
    if 'Depth' in merged_df.columns:
        if 'DepSM' in merged_df.columns:
            depth_mask = merged_df['DepSM'].notna()
            merged_df.loc[depth_mask, 'Depth'] = merged_df.loc[depth_mask, 'DepSM'].round().astype(int)

        inline_mask = merged_df['Type'].astype(str).str.lower() == 'inline' if 'Type' in merged_df.columns else pd.Series(False, index=merged_df.index)
        merged_df.loc[inline_mask, 'Depth'] = 2

    # Remove timezone information from datetime columns before saving to Excel
    for col in merged_df.columns:
        if merged_df[col].dtype == 'datetime64[ns, UTC]' or merged_df[col].dtype == 'datetime64[ns, America/Los_Angeles]':
            merged_df[col] = merged_df[col].dt.tz_localize(None)
        elif hasattr(merged_df[col], 'dt') and hasattr(merged_df[col].dt, 'tz') and merged_df[col].dt.tz is not None:
            merged_df[col] = merged_df[col].dt.tz_localize(None)

    # Drop columns that are not needed in the final output
    columns_to_drop = ['datetime', 'datetime_local', 'merge_date', 'Uploadtime', 'NMEAlat','NMEANlon', 'Date','Time', 'Sigma-é00', 'Sigma-t00', 'Scan', 'date']
    merged_df = merged_df.drop(columns=columns_to_drop, errors='ignore')

    # Rename columns
    if 'BinId' in merged_df.columns:
        merged_df = merged_df.rename(columns={'BinId': 'bin'})
    if 'DateTime' in merged_df.columns:
        merged_df = merged_df.rename(columns={'DateTime': 'RunTime'})
    if 'CollectionTime' in merged_df.columns:
        merged_df = merged_df.rename(columns={'CollectionTime': 'DateTime'})

    # Filter out FSW samples from all outputs
    if 'Type' in merged_df.columns:
        initial_count = len(merged_df)
        merged_df = merged_df[merged_df['Type'].str.lower() != 'fsw']
        print(f"Filtered out {initial_count - len(merged_df)} FSW samples from all outputs")

    # Rename BinId column to bin
    if 'BinId' in merged_df.columns:
        merged_df = merged_df.rename(columns={'BinId': 'bin'})

    # Copy Potemp090C and Sal00 values into Temperature and Salinity columns where they exist
    if 'Potemp090C' in merged_df.columns:
        bottle_mask = merged_df['Potemp090C'].notna()
        merged_df.loc[bottle_mask, 'Temperature'] = merged_df.loc[bottle_mask, 'Potemp090C']

    if 'Sal00' in merged_df.columns:
        bottle_mask = merged_df['Sal00'].notna()
        merged_df.loc[bottle_mask, 'Salinity'] = merged_df.loc[bottle_mask, 'Sal00']

    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.isdir(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    # Save result
    merged_df.to_excel(output_file, index=False)
    print(f"✅ Merged metadata saved to '{output_file}'.")

    # also save a CSV version
    csv_output_file = output_file.replace('.xlsx', '.csv')
    merged_df.to_csv(csv_output_file, index=False)

    # === Create additional output file with CTD temp and sal merged in for Ecotaxa upload ===
    if ecotaxa_file is None:
        ecotaxa_file = output_file.replace('.xlsx', '-toEcotaxa.xlsx')
    
    # Create EcoTaxa directory if it doesn't exist
    ecotaxa_dir = os.path.dirname(ecotaxa_file)
    if ecotaxa_dir and not os.path.isdir(ecotaxa_dir):
        os.makedirs(ecotaxa_dir, exist_ok=True)
    
    # Copy merged_df for the bottle data version (FSW rows already filtered out above)
    bottle_data_df = merged_df.copy()

    # Find the index of NMEAtimeUTC column to remove it and all columns to the right
    if 'NMEAtimeUTC' in bottle_data_df.columns:
        nmea_index = bottle_data_df.columns.get_loc('NMEAtimeUTC')
        # Keep only columns before NMEAtimeUTC
        bottle_data_df = bottle_data_df.iloc[:, :nmea_index]

    # Save the bottle data version
    bottle_data_df.to_excel(ecotaxa_file, index=False)
    print(f"✅ Merged metadata with bottle data saved to '{ecotaxa_file}'.")

    # also save a CSV version of bottle data
    bottle_csv_output_file = ecotaxa_file.replace('.xlsx', '.csv')
    bottle_data_df.to_csv(bottle_csv_output_file, index=False)


if __name__ == "__main__":
    # Check if paths are provided as command-line arguments
    if len(sys.argv) >= 4:
        metadata_file_path = sys.argv[1]
        bottle_file_path = sys.argv[2]
        output_file_path = sys.argv[3]
        ecotaxa_file_path = sys.argv[4] if len(sys.argv) > 4 else None
    else:
        # Default paths
        metadata_file_path = ''
        bottle_file_path = ''
        output_file_path = ''
        ecotaxa_file_path = ''
    
    # Validate input files exist
    if not os.path.isfile(metadata_file_path):
        print(f"Error: Metadata file does not exist: {metadata_file_path}")
        sys.exit(1)
    if not os.path.isfile(bottle_file_path):
        print(f"Error: Bottle file does not exist: {bottle_file_path}")
        sys.exit(1)
    
    merge_metadata_with_bottle(metadata_file_path, bottle_file_path, output_file_path, ecotaxa_file_path)