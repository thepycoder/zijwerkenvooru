#!/usr/bin/env python3
"""Plot cost and output token counts comparing models per dossier for the last run."""
import json
from pathlib import Path
from collections import defaultdict
import matplotlib.pyplot as plt
import numpy as np


def extract_model_name(filename):
    """Extract model name from results filename."""
    model = filename.stem.replace('results_', '')
    return model


def collect_dossier_model_data():
    """Collect cost and token data per dossier per model from the last run."""
    base_dir = Path(__file__).parent
    runs_dir = base_dir / "runs"
    index_path = runs_dir / "runs_index.json"
    
    if not index_path.exists():
        print(f"Runs index not found: {index_path}")
        return None
    
    with open(index_path, 'r', encoding='utf-8') as f:
        index = json.load(f)
    
    if not index['runs']:
        print("No runs found")
        return None
    
    # Get the last (most recent) run
    last_run = index['runs'][0]  # First entry is the most recent
    run_path = base_dir / last_run['path']
    
    print(f"Processing last run: {last_run['run_id']}")
    
    # Structure: dossier_data[dossier_id][model] = {cost, input_tokens, output_tokens}
    dossier_data = defaultdict(lambda: defaultdict(dict))
    all_models = set()
    
    # Process all result files in the last run
    for result_file in sorted(run_path.glob("results_*.json")):
        model = extract_model_name(result_file)
        all_models.add(model)
        
        try:
            with open(result_file, 'r', encoding='utf-8') as f:
                results = json.load(f)
            
            # Collect data for each dossier
            for dossier_id, dossier_info in results.items():
                if isinstance(dossier_info, dict) and 'cost_info' in dossier_info:
                    cost_info = dossier_info['cost_info']
                    if cost_info is not None:
                        dossier_data[dossier_id][model] = {
                            'cost': cost_info.get('total_cost', 0.0),
                            'input_tokens': cost_info.get('input_tokens', 0),
                            'output_tokens': cost_info.get('output_tokens', 0)
                        }
                
        except Exception as e:
            print(f"Error processing {result_file}: {e}")
            continue
    
    return dict(dossier_data), sorted(all_models), last_run['run_id']


def create_plots(dossier_data, models, run_id):
    """Create plots comparing models per dossier."""
    if not dossier_data:
        print("No data to plot")
        return
    
    # Sort dossiers by ID (convert to int for proper sorting)
    sorted_dossiers = sorted(dossier_data.items(), key=lambda x: int(x[0]) if x[0].isdigit() else 0)
    dossier_ids = [d[0] for d in sorted_dossiers]
    
    n_dossiers = len(dossier_ids)
    n_models = len(models)
    
    # Prepare data arrays: [dossier][model]
    costs = np.zeros((n_dossiers, n_models))
    output_tokens = np.zeros((n_dossiers, n_models))
    
    for i, dossier_id in enumerate(dossier_ids):
        for j, model in enumerate(models):
            if model in dossier_data[dossier_id]:
                costs[i, j] = dossier_data[dossier_id][model]['cost']
                output_tokens[i, j] = dossier_data[dossier_id][model]['output_tokens']
    
    # Create figure with subplots
    fig, axes = plt.subplots(2, 1, figsize=(max(16, n_dossiers * 1.5), 10))
    fig.suptitle(f'Model Comparison per Dossier - {run_id}', fontsize=16, fontweight='bold')
    
    # Set up colors for models
    colors = plt.cm.tab20(np.linspace(0, 1, n_models))
    
    # Calculate bar width
    width = 0.8 / n_models
    x = np.arange(n_dossiers)
    
    # Plot 1: Cost
    ax1 = axes[0]
    for j, model in enumerate(models):
        offset = (j - n_models/2 + 0.5) * width
        bars = ax1.bar(x + offset, costs[:, j], width, label=model, color=colors[j], alpha=0.7)
    
    ax1.set_xlabel('Dossier ID', fontsize=12)
    ax1.set_ylabel('Cost ($)', fontsize=12)
    ax1.set_title('Cost per Model per Dossier', fontsize=14, fontweight='bold')
    ax1.set_xticks(x)
    ax1.set_xticklabels(dossier_ids, rotation=45, ha='right')
    ax1.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
    ax1.grid(axis='y', alpha=0.3)
    
    # Plot 2: Output tokens
    ax2 = axes[1]
    for j, model in enumerate(models):
        offset = (j - n_models/2 + 0.5) * width
        bars = ax2.bar(x + offset, output_tokens[:, j], width, label=model, color=colors[j], alpha=0.7)
    
    ax2.set_xlabel('Dossier ID', fontsize=12)
    ax2.set_ylabel('Output Tokens', fontsize=12)
    ax2.set_title('Output Tokens per Model per Dossier', fontsize=14, fontweight='bold')
    ax2.set_xticks(x)
    ax2.set_xticklabels(dossier_ids, rotation=45, ha='right')
    ax2.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=8)
    ax2.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    
    # Save plot
    output_path = Path(__file__).parent / "runs" / f"model_comparison_per_dossier_{run_id}.png"
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    print(f"Plot saved to: {output_path}")
    
    # Print summary table
    print(f"\nSummary for {run_id} (Cost / Output Tokens):")
    print(f"{'Dossier':<12} " + " | ".join([f"{m:<20}" for m in models]))
    print("-" * (12 + (n_models * 24)))
    for dossier_id in dossier_ids:
        row = f"{dossier_id:<12} "
        for model in models:
            if model in dossier_data[dossier_id]:
                data = dossier_data[dossier_id][model]
                row += f"${data['cost']:.4f}/{data['output_tokens']:,} | "
            else:
                row += "N/A | "
        print(row.rstrip(" | "))
    
    plt.show()


def main():
    """Main function."""
    result = collect_dossier_model_data()
    if result:
        dossier_data, models, run_id = result
        create_plots(dossier_data, models, run_id)


if __name__ == "__main__":
    main()
