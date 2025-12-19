#!/usr/bin/env python3
"""Script to recalculate and update costs in existing JSON result files."""
import json
import sys
from pathlib import Path
from typing import Dict

# Import cost estimator to get MODEL_PRICING
from .cost_estimator import MODEL_PRICING


def update_costs_in_file(file_path: Path) -> Dict:
    """
    Update costs in a single JSON result file.
    
    Returns:
        Dictionary with update statistics
    """
    print(f"Processing {file_path.name}...")
    
    # Read the file
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    updated_count = 0
    total_cost = 0.0
    
    # Process each dossier entry
    for dossier_id, entry in data.items():
        if not isinstance(entry, dict):
            continue
            
        model = entry.get("model")
        cost_info = entry.get("cost_info", {})
        
        if not model or not cost_info:
            continue
        
        # Get token counts
        input_tokens = cost_info.get("input_tokens", 0)
        output_tokens = cost_info.get("output_tokens", 0)
        
        # Check if model has pricing configured
        if model not in MODEL_PRICING:
            print(f"  Warning: No pricing found for model '{model}'")
            continue
        
        # Calculate costs
        pricing = MODEL_PRICING[model]
        input_cost = pricing.cost(input_tokens, 0)
        output_cost = pricing.cost(0, output_tokens)
        total_entry_cost = pricing.cost(input_tokens, output_tokens)
        
        # Update cost_info
        entry["cost_info"]["input_cost"] = input_cost
        entry["cost_info"]["output_cost"] = output_cost
        entry["cost_info"]["total_cost"] = total_entry_cost
        
        updated_count += 1
        total_cost += total_entry_cost
    
    # Write updated data back
    if updated_count > 0:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"  Updated {updated_count} entries, total cost: ${total_cost:.4f}")
    else:
        print(f"  No entries updated")
    
    return {
        "file": file_path.name,
        "updated_count": updated_count,
        "total_cost": total_cost,
    }


def main():
    """Main function to update costs in all JSON files in a directory."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Recalculate and update costs in JSON result files"
    )
    parser.add_argument(
        "directory",
        type=str,
        help="Directory containing JSON result files to update",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be updated without making changes",
    )
    args = parser.parse_args()
    
    directory = Path(args.directory)
    if not directory.exists():
        print(f"Error: Directory {directory} does not exist")
        sys.exit(1)
    
    # Find all JSON result files
    json_files = list(directory.glob("results_*.json"))
    
    if not json_files:
        print(f"No result JSON files found in {directory}")
        sys.exit(1)
    
    print(f"Found {len(json_files)} JSON files to process\n")
    
    if args.dry_run:
        print("DRY RUN MODE - No files will be modified\n")
    
    results = []
    for json_file in sorted(json_files):
        if args.dry_run:
            # Just show what would be updated
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            for dossier_id, entry in data.items():
                if not isinstance(entry, dict):
                    continue
                    
                model = entry.get("model")
                cost_info = entry.get("cost_info", {})
                
                if not model or not cost_info:
                    continue
                
                input_tokens = cost_info.get("input_tokens", 0)
                output_tokens = cost_info.get("output_tokens", 0)
                current_cost = cost_info.get("total_cost", 0.0)
                
                if model in MODEL_PRICING:
                    pricing = MODEL_PRICING[model]
                    new_cost = pricing.cost(input_tokens, output_tokens)
                    if abs(new_cost - current_cost) > 0.0001:
                        print(f"{json_file.name} - Dossier {dossier_id}: "
                              f"${current_cost:.4f} -> ${new_cost:.4f}")
        else:
            result = update_costs_in_file(json_file)
            results.append(result)
    
    if not args.dry_run:
        print(f"\n{'='*80}")
        print("Summary:")
        print(f"{'='*80}")
        total_updated = sum(r["updated_count"] for r in results)
        grand_total_cost = sum(r["total_cost"] for r in results)
        print(f"Total entries updated: {total_updated}")
        print(f"Grand total cost: ${grand_total_cost:.4f}")
        print(f"\nFiles processed: {len(results)}")


if __name__ == "__main__":
    main()

