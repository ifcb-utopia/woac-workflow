import pandas as pd
import sys
import os
from pathlib import Path

# This script processes TSG (Temperature and Salinity) .Raw files from a specified folder and combines the data into an Excel file.
# Usage:
# python read_TSG.py /path/to/tsg/folder /path/to/output.xlsx   

def process_tsg_data(tsg_folder, output_file):
    """
    Process TSG (Temperature Salinity) .Raw files and save combined data to Excel.
    
    Parameters:
    -----------
    tsg_folder : str
        Path to the folder containing TSG .Raw files
    output_file : str
        Path where the output Excel file will be saved
    """
    
    tsg_folder = Path(tsg_folder)
    
    tsg_files = sorted([f for f in tsg_folder.glob('*.Raw') if 'SBEInterfaceBox' in f.name])

    if not tsg_files:
        print(f"⚠️  No TSG files found in {tsg_folder}")
        return

    all_rows = []

    for file in tsg_files:
        with open(file, 'r') as f:
            for line in f:
                parts = line.strip().split(',')
                if len(parts) >= 5:
                    try:
                        date = parts[0]
                        time = parts[1]
                        
                        # Extract temperature from t1= field
                        temperature = None
                        salinity = None
                        
                        for part in parts[2:]:
                            part = part.strip()
                            if part.startswith('t1='):
                                temperature = float(part.split('=')[1].strip())
                            elif part.startswith('s='):
                                salinity = float(part.split('=')[1].strip())
                        
                        if temperature is not None and salinity is not None:
                            datetime = pd.to_datetime(f'{date} {time}', format='%m/%d/%Y %H:%M:%S.%f', errors='coerce')
                            all_rows.append([datetime, temperature, salinity])
                    except (ValueError, IndexError):
                        # Skip lines that can't be parsed
                        continue

    # Create DataFrame
    combined_df = pd.DataFrame(all_rows, columns=['Datetime', 'Temperature', 'Salinity'])

    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.isdir(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    # Save to Excel
    combined_df.to_excel(output_file, index=False)
    
    print(f"✅ Combined TSG data saved to '{output_file}'.")
    print(f"Processed {len(tsg_files)} TSG files")
    print(f"Total TSG records: {len(all_rows)}")


if __name__ == "__main__":
    # Check if paths are provided as command-line arguments
    if len(sys.argv) == 3:
        tsg_folder_path = sys.argv[1]
        output_file_path = sys.argv[2]
    else:
        # Default paths
        tsg_folder_path = ''
        output_file_path = ''
    
    # Validate input folder exists
    if not os.path.isdir(tsg_folder_path):
        print(f"Error: TSG folder does not exist: {tsg_folder_path}")
        sys.exit(1)
    
    process_tsg_data(tsg_folder_path, output_file_path)
