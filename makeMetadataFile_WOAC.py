import os
import pandas as pd

# This script creates a metadata file for the WOAC IFCB data based solely on the .hdr files in the raw data directory.
# Subsequently, the file can be merged with ship GPS and bottle TSG data to create a comprehensive metadata file.

# Specify the directory path of the raw IFCB data
directory = '/Users/AIRS Shared Lab/OneDrive - UW/IFCB216/raw/TallShip2025/raw_db' #'C:/Users/AIRS Shared Lab/OneDrive - UW/IFCB216/raw/WOAC_sept25/raw_db'

# Get all file names in the directory
file_names = os.listdir(directory)

# Get all file names in the directory ending with .hdr
hdr_files = [file for file in file_names if file.endswith('.hdr')]

# Create a DataFrame with a column of file names without extension
df = pd.DataFrame({'BinId': [os.path.splitext(file)[0] for file in hdr_files]})

# Extract DateTime from file names and create a new column
df['date_str'] = df['BinId'].str[1:9]
df['time_str'] = df['BinId'].str[10:16]
df['date_time_str'] = df['date_str'] + df['time_str']

df['DateTime'] = pd.to_datetime(df['date_time_str'], format='%Y%m%d%H%M%S')

# Extract the value after "FileComment:" from each .hdr file and populate a new column
extracted_comments = []
for file in hdr_files:
    file_path = os.path.join(directory, file)
    with open(file_path, 'r') as f:
        lines = f.readlines()
        # Find the line containing "FileComment"
        file_comment_line = next((line.strip() for line in lines if "FileComment:" in line), None)
        # Extract the part after "FileComment:"
        if file_comment_line:
            comment = file_comment_line.split("FileComment:")[1].strip()
        else:
            comment = None
        extracted_comments.append(comment)

print(extracted_comments)

# Add a column for the collected time to be filled manually later when applicable
df['CollectionTime'] = None

# Add the extracted comments as a new column in the DataFrame
df['FileComment'] = extracted_comments

df = df.sort_values(by='DateTime')

df = df.drop_duplicates(subset='BinId', keep='first')

# Create a series of columns to be edited/populated later
df['Type'] = 1
df['Concentration'] = 1
df['Flag'] = None
df['Depth'] = None
df['Station'] = None
df['Bottle'] = None

# drop columns that are not needed
df = df.drop(columns=['date_str', 'time_str', 'date_time_str'])

print(df)

# Save the DataFrame to an excel file
df.to_excel('/Users/AIRS Shared Lab/OneDrive - UW/IFCB216/Tall Ship Cruise Oct 2025/SL2025-metadata.xlsx', index=False)