import pandas as pd
import os
import requests
import time

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, '../web/src/data')
OUTPUT_DIR = os.path.join(BASE_DIR, 'downloads')

def get_padded_id(id_val, length=4):
    return str(id_val).zfill(length)

def is_valid_pdf(filepath):
    """
    Check if a file is a valid PDF (not empty and not an HTML error page).
    Returns True if valid, False otherwise.
    """
    try:
        if not os.path.exists(filepath):
            return False
        
        # Check file size
        file_size = os.path.getsize(filepath)
        if file_size < 1000:  # Very small files are likely errors
            return False
        
        # Read first few bytes to check PDF magic number
        with open(filepath, 'rb') as f:
            first_bytes = f.read(1024)
            
        # Check for PDF magic number
        if not first_bytes.startswith(b'%PDF'):
            # Might be HTML error page
            try:
                text = first_bytes.decode('utf-8', errors='ignore')
                if 'Request Rejected' in text or 'not found' in text.lower() or '<html>' in text.lower():
                    return False
            except (UnicodeDecodeError, ValueError):
                pass
            # If it doesn't start with %PDF and isn't obviously HTML, still might be invalid
            return False
        
        return True
    except Exception as e:
        print(f"    Warning: Error checking PDF validity: {e}")
        return False

def construct_url(session_id, dossier_id, subdoc_id):
    # Pattern: https://www.dekamer.be/FLWB/PDF/{session_id}/{dossier_id_padded_4}/{session_id}K{dossier_id_padded_4}{subdoc_id}.pdf
    
    dossier_id_padded = get_padded_id(dossier_id, 4)
    # subdoc_id is usually something like "001" or "002" or "001-1"
    # The URL pattern usually appends it directly.
    # Note: If subdoc_id contains spaces or special chars, it might need handling, but based on inspection it looked clean.
    
    filename = f"{session_id}K{dossier_id_padded}{subdoc_id}.pdf"
    url = f"https://www.dekamer.be/FLWB/PDF/{session_id}/{dossier_id_padded}/{filename}"
    return url

def main():
    # Read parquet files
    print("Reading parquet files...")
    dossiers_path = os.path.join(DATA_DIR, 'dossiers.parquet')
    subdocs_path = os.path.join(DATA_DIR, 'subdocuments.parquet')
    
    if not os.path.exists(dossiers_path) or not os.path.exists(subdocs_path):
        print(f"Parquet files not found in {DATA_DIR}")
        return

    try:
        df_dossiers = pd.read_parquet(dossiers_path)
        df_subdocs = pd.read_parquet(subdocs_path)
    except Exception as e:
        print(f"Error reading parquet files: {e}")
        return

    # Join
    # dossier_id in dossiers is 'id', in subdocs is 'dossier_id'
    print("Merging data...")
    merged = df_subdocs.merge(df_dossiers[['id', 'session_id']], left_on='dossier_id', right_on='id', how='left')
    
    # Check if merge was successful (session_id should not be null)
    if merged['session_id'].isnull().any():
        print("Warning: Some subdocuments could not be linked to a session ID.")
        merged = merged.dropna(subset=['session_id'])
    
    # Create output directory
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        
    # Download loop
    # Limit to a few dossiers for testing (e.g. 5 random dossiers that have subdocuments)
    unique_dossiers = merged['dossier_id'].unique()
    target_dossiers = unique_dossiers
    
    print(f"Found {len(unique_dossiers)} dossiers with documents.")
    print(f"Downloading PDFs for the first {len(target_dossiers)} dossiers...")
    
    for dossier_id in target_dossiers:
        dossier_docs = merged[merged['dossier_id'] == dossier_id]
        
        # Create dossier folder
        dossier_dir = os.path.join(OUTPUT_DIR, str(dossier_id))
        if not os.path.exists(dossier_dir):
            os.makedirs(dossier_dir)
            
        session_id = dossier_docs.iloc[0]['session_id']
        
        print(f"\nProcessing Dossier {dossier_id} (Session {session_id})")
        
        for _, row in dossier_docs.iterrows():
            subdoc_id = row['id_x'] # id from subdocuments
            # Clean subdoc_id if necessary? 
            # In some cases id might be "001"
            
            url = construct_url(session_id, dossier_id, subdoc_id)
            filename = f"{session_id}K{get_padded_id(dossier_id, 4)}{subdoc_id}.pdf"
            filepath = os.path.join(dossier_dir, filename)
            
            if os.path.exists(filepath):
                print(f"  [SKIP] {filename} (exists)")
                continue
                
            print(f"  [DOWN] {url}")
            try:
                response = requests.get(url, timeout=10)
                if response.status_code == 200:
                    with open(filepath, 'wb') as f:
                        f.write(response.content)
                    
                    # Check if the downloaded file is valid
                    if not is_valid_pdf(filepath):
                        # Delete invalid file
                        os.remove(filepath)
                        print("    ERROR: Downloaded file is empty or contains error message")
                        print("    Waiting 10 seconds before continuing...")
                        time.sleep(10)  # Wait longer when error detected
                    else:
                        # Valid PDF downloaded
                        pass
                else:
                    print(f"    FAILED: Status {response.status_code}")
                    # Wait a bit longer on HTTP errors
                    time.sleep(5)
            except Exception as e:
                print(f"    ERROR: {e}")
                # Wait a bit longer on exceptions
                time.sleep(5)
            
            time.sleep(1) # Rate limit

if __name__ == "__main__":
    main()

