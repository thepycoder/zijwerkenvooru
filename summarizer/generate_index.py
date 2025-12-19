#!/usr/bin/env python3
"""Generate an index file listing all available results JSON files."""
import json
from pathlib import Path

def main():
    """Generate results_index.json listing all available result files."""
    base_dir = Path(__file__).parent
    result_files = []
    
    # Find all results_*.json files
    for file in base_dir.glob("results_*.json"):
        result_files.append(file.name)
    
    # Also check for results.json
    if (base_dir / "results.json").exists():
        result_files.append("results.json")
    
    # Create index
    index = {
        "files": sorted(result_files),
        "count": len(result_files)
    }
    
    # Write index file
    index_path = base_dir / "results_index.json"
    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(index, f, indent=2)
    
    print(f"Generated index with {len(result_files)} files:")
    for file in sorted(result_files):
        print(f"  - {file}")
    print(f"\nIndex saved to: {index_path}")

if __name__ == "__main__":
    main()

