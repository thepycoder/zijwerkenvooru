import pandas as pd
import os

file_path = 'web/src/data/subdocuments.parquet'

if not os.path.exists(file_path):
    print(f"File not found: {file_path}")
    exit(1)

try:
    df = pd.read_parquet(file_path)
    unknowns = df[df['type'] == 'Unknown']
    
    if unknowns.empty:
        print("No subdocuments with 'Unknown' type found.")
    else:
        print(f"Found {len(unknowns)} subdocuments with 'Unknown' type:")
        # Adjust display options to show all rows if reasonable, or a good chunk
        pd.set_option('display.max_rows', None) 
        print(unknowns[['dossier_id', 'id']])
        
except Exception as e:
    print(f"Error reading parquet file: {e}")

