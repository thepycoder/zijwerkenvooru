#!/usr/bin/env python3
"""Generate an index file listing all available runs."""
import json
from pathlib import Path


def main():
    """Generate runs_index.json listing all available run directories."""
    base_dir = Path(__file__).parent
    runs_dir = base_dir / "runs"
    
    if not runs_dir.exists():
        print(f"Runs directory does not exist: {runs_dir}")
        return
    
    runs = []
    
    # Find all run_* directories
    for run_dir in sorted(runs_dir.glob("run_*"), reverse=True):
        if run_dir.is_dir():
            # Check if it has any results files
            result_files = list(run_dir.glob("results_*.json"))
            if result_files:
                runs.append({
                    "run_id": run_dir.name,
                    "path": str(run_dir.relative_to(base_dir)),
                    "result_count": len(result_files),
                })
    
    # Create index
    index = {
        "runs": runs,
        "count": len(runs)
    }
    
    # Write index file
    index_path = runs_dir / "runs_index.json"
    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(index, f, indent=2)
    
    print(f"Generated runs index with {len(runs)} runs:")
    for run in runs:
        print(f"  - {run['run_id']} ({run['result_count']} results)")
    print(f"\nIndex saved to: {index_path}")


if __name__ == "__main__":
    main()

