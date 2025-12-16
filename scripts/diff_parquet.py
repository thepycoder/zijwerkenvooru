import os
import sys
import subprocess
import pandas as pd
import json
import tempfile
import shutil

def get_git_root():
    return subprocess.check_output(['git', 'rev-parse', '--show-toplevel']).decode('utf-8').strip()

def get_changed_parquet_files():
    # Get staged and unstaged changes
    cmd = ['git', 'diff', '--name-only', 'HEAD']
    try:
        output = subprocess.check_output(cmd).decode('utf-8')
    except subprocess.CalledProcessError:
        return []
    files = [f.strip() for f in output.split('\n') if f.strip().endswith('.parquet')]
    return files

def file_in_head(filepath):
    try:
        subprocess.check_call(['git', 'cat-file', '-e', f'HEAD:{filepath}'], stderr=subprocess.DEVNULL)
        return True
    except subprocess.CalledProcessError:
        return False

def main():
    try:
        root = get_git_root()
    except:
        print("Not a git repository.")
        return

    files = get_changed_parquet_files()
    
    if not files:
        print("No changed parquet files found.")
        return

    temp_dir = tempfile.mkdtemp(prefix="parquet_diff_")
    print(f"Using temp dir: {temp_dir}")

    try:
        for relative_path in files:
            full_path = os.path.join(root, relative_path)
            print(f"Processing {relative_path}...")
            
            filename = os.path.basename(relative_path)
            current_json_path = os.path.join(temp_dir, f"{filename}.current.json")
            old_json_path = os.path.join(temp_dir, f"{filename}.old.json")

            # 1. Handle Old Version (HEAD)
            if file_in_head(relative_path):
                old_parquet_path = os.path.join(temp_dir, f"{filename}.old.parquet")
                with open(old_parquet_path, 'wb') as f:
                    subprocess.check_call(['git', 'show', f'HEAD:{relative_path}'], stdout=f)
                
                try:
                    df_old = pd.read_parquet(old_parquet_path)
                    with open(old_json_path, 'w') as f:
                        json.dump(df_old.to_dict(orient='records'), f, indent=2, default=str)
                except Exception as e:
                    print(f"Failed to read old parquet: {e}")
                    with open(old_json_path, 'w') as f:
                        f.write("[]")
            else:
                # New file
                with open(old_json_path, 'w') as f:
                    f.write("[]")

            # 2. Handle Current Version
            if os.path.exists(full_path):
                try:
                    df_current = pd.read_parquet(full_path)
                    with open(current_json_path, 'w') as f:
                        json.dump(df_current.to_dict(orient='records'), f, indent=2, default=str)
                except Exception as e:
                    print(f"Failed to read current parquet: {e}")
                    with open(current_json_path, 'w') as f:
                        f.write("[]")
            else:
                # Deleted file
                with open(current_json_path, 'w') as f:
                    f.write("[]")

            # 3. Diff
            print(f"Opening diff for {relative_path}...")
            # We use call so it opens and we move to next, or check_call to block?
            # VS Code usually returns immediately unless -w is passed.
            # We probably want to fire them all.
            subprocess.call(['code', '--diff', old_json_path, current_json_path])

    finally:
        # We don't delete temp dir immediately so the user can see the diffs in VS Code
        # VS Code needs the files to exist to diff them.
        print(f"Temp files left in {temp_dir}")

if __name__ == "__main__":
    main()

