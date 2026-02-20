import pandas as pd
from pathlib import Path


tsg_folder = Path('Sept2025/WOAC fall 2025 ship data/scs/SEAWATER')  # Folder containing your TSG files
output_file = 'Sept2025/combined_tsg_data.xlsx'  # Output file name

tsg_files = sorted([f for f in tsg_folder.glob('*.Raw') if 'SBEInterfaceBox' in f.name])  #SSW-TSG

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

# Save to Excel
combined_df.to_excel(output_file, index=False)
