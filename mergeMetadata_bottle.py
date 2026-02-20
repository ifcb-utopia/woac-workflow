import pandas as pd

# === User Inputs ===
metadata_file = 'Sept2025/WOAC-Sept2025-metadata_edit_GPS_TSG.xlsx'     # text file with IFCB sample timestamps and bottle info
bottle_file = 'Sept2025/September_2025_btl.csv'             # text file with bottle times, locations, water condition
output_file = 'Sept2025/WOAC-Sept2025-metadata_merged.xlsx'  # output file name
ecotaxa_file = 'Sept2025/EcoTaxa/WOAC-Sept2025-metadata_merged-toEcotaxa.xlsx'  # text file with Ecotaxa data (if needed)

# Load IFCB metadata file
metadata_df = pd.read_excel(metadata_file)
# Check the actual column names and parse them safely
# Parse datetime from metadata_df
metadata_df['datetime'] = pd.to_datetime(metadata_df['DateTime'], errors='coerce', utc=True)  # coerce bad values to NaT
metadata_df['datetime_local'] = metadata_df['datetime'].dt.tz_convert('America/Los_Angeles')
metadata_df['merge_date'] = metadata_df['datetime_local'].dt.strftime('%m/%d/%y')
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

# Update the existing Latitude and Longitude columns with bottle values where available
merged_df['Latitude'] = coords_merged['Latitude_bottle'].fillna(merged_df['Latitude'])
merged_df['Longitude'] = coords_merged['Longitude_bottle'].fillna(merged_df['Longitude'])

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

# Copy DepSM values into Depth column when values exist, and round to nearest integer
if 'DepSM' in merged_df.columns and 'Depth' in merged_df.columns:
    # Fill Depth with DepSM values where DepSM is not null
    depth_mask = merged_df['DepSM'].notna()
    merged_df.loc[depth_mask, 'Depth'] = merged_df.loc[depth_mask, 'DepSM'].round().astype(int)

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

# Save result
merged_df.to_excel(output_file, index=False)
print(f"✅ Merged metadata saved to '{output_file}'.")

# also save a CSV version
csv_output_file = output_file.replace('.xlsx', '.csv')
merged_df.to_csv(csv_output_file, index=False)

# === Create additional output file with CTD temp and sal merged in for Ecotaxa upload ===
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