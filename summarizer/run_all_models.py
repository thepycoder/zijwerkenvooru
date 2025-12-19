#!/usr/bin/env python3
"""Run all configured models in parallel and organize results."""
import json
import os
import shutil
import subprocess
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from .data_loader import load_dossier_from_parquet
from .pipeline import summarize_dossier
from .model_config import MODELS_TO_TEST
from .cost_estimator import MODEL_PRICING


def sanitize_model_name(model_name: str) -> str:
    """Sanitize model name for use in filenames."""
    return model_name.replace("/", "_").replace(":", "_").replace(" ", "_")


def process_single_model(
    config_dict: Dict,
    dossiers_data: Dict,
    results_dir: str,
) -> Dict:
    """
    Process a single model configuration.
    This function runs in a separate process.
    """
    from .model_config import ModelConfig
    from .llm import (
        OpenAILLMClient,
        AnthropicLLMClient,
        MistralLLMClient,
        GeminiLLMClient,
        NebiusLLMClient,
    )
    
    # Map client class names to actual classes
    client_classes = {
        "OpenAILLMClient": OpenAILLMClient,
        "AnthropicLLMClient": AnthropicLLMClient,
        "MistralLLMClient": MistralLLMClient,
        "GeminiLLMClient": GeminiLLMClient,
        "NebiusLLMClient": NebiusLLMClient,
    }
    
    # Reconstruct config from dict (needed for multiprocessing)
    client_class = client_classes[config_dict["client_class"]]
    config = ModelConfig(
        name=config_dict["name"],
        provider=config_dict["provider"],
        model=config_dict["model"],
        client_class=client_class,
        api_key_env=config_dict.get("api_key_env"),
    )
    
    print(f"\n{'='*80}")
    print(f"Starting: {config.name} ({config.model})")
    print(f"{'='*80}\n")
    
    try:
        # Create LLM client
        llm_client = config.client_class(model=config.model)
        
        # Sanitize model name for filename
        safe_model_name = sanitize_model_name(config.model)
        results_dir_path = Path(results_dir)
        output_file = results_dir_path / f"results_{safe_model_name}.json"
        
        # Initialize results dictionary
        results = {}
        
        # Load existing results if file exists
        if output_file.exists():
            try:
                with open(output_file, "r", encoding="utf-8") as f:
                    results = json.load(f)
                print(f"Loaded existing results from {output_file}")
            except Exception as e:
                print(f"Warning: Could not load existing results: {e}")
                results = {}
        
        # Process each dossier
        for dossier_id, dossier_info in dossiers_data.items():
            print(f"\n[{config.name}] Processing dossier {dossier_id}...")
            
            try:
                # Load dossier
                dossier = load_dossier_from_parquet(
                    dossier_id=dossier_id,
                    subdocuments_path=dossier_info["subdocuments_path"],
                    dossiers_path=dossier_info["dossiers_path"],
                    markdown_base_path=dossier_info["markdown_base_path"],
                )
                
                if not dossier:
                    print(f"[{config.name}] Error: Dossier {dossier_id} not found")
                    continue
                
                # Run pipeline
                summary = summarize_dossier(dossier, llm_client)
                
                if summary:
                    # Extract retry and cost info
                    retry_info = getattr(summary, '_retry_info', None)
                    cost_info = getattr(summary, '_cost_info', None)
                    
                    # Calculate cost if we have pricing info and cost info
                    total_cost = 0.0
                    cost_breakdown = {}
                    if cost_info and config.model in MODEL_PRICING:
                        pricing = MODEL_PRICING[config.model]
                        total_cost = pricing.cost(cost_info.input_tokens, cost_info.output_tokens)
                        cost_breakdown = {
                            "input_tokens": cost_info.input_tokens,
                            "output_tokens": cost_info.output_tokens,
                            "input_cost": pricing.cost(cost_info.input_tokens, 0),
                            "output_cost": pricing.cost(0, cost_info.output_tokens),
                        }
                    
                    # Prepare retry info for JSON
                    retry_data = None
                    if retry_info:
                        retry_data = {
                            "json_parse_retries": retry_info.json_parse_retries,
                            "json_parse_failed": retry_info.json_parse_failed,
                            "json_parse_error": retry_info.json_parse_error,
                        }
                    
                    result_data = {
                        "dossier_id": summary.dossier_id,
                        "title": summary.title,
                        "generated_title": summary.generated_title,
                        "model": config.model,
                        "model_name": config.name,
                        "provider": config.provider,
                        "fact_summary": {
                            "summary": summary.fact_summary.summary,
                            "document_type": summary.fact_summary.document_type,
                            "selection_reason": summary.fact_summary.selection_reason,
                        },
                        "political_analysis": {
                            "arguments_for": summary.political_analysis.arguments_for,
                            "arguments_against": summary.political_analysis.arguments_against,
                            "neutral_technical": summary.political_analysis.neutral_technical,
                            "summary_debate": summary.political_analysis.summary_debate,
                            "notable_changes": summary.political_analysis.notable_changes,
                            "has_debate": summary.political_analysis.has_debate,
                        },
                        "retry_info": retry_data,
                        "cost_info": {
                            "total_cost": total_cost,
                            **cost_breakdown,
                        } if cost_info else None,
                    }
                    
                    # Update results and write immediately
                    results[dossier_id] = result_data
                    
                    # Write updated results to file immediately
                    with open(output_file, "w", encoding="utf-8") as f:
                        json.dump(results, f, indent=2, ensure_ascii=False)
                    
                    print(f"[{config.name}] ✓ Dossier {dossier_id} completed")
                else:
                    print(f"[{config.name}] ✗ Failed to generate summary for dossier {dossier_id}")
                    
            except Exception as e:
                print(f"[{config.name}] ✗ Error processing dossier {dossier_id}: {e}")
                import traceback
                traceback.print_exc()
        
        return {
            "config": config_dict,
            "success": True,
            "output_file": str(output_file),
            "results_count": len(results),
        }
        
    except Exception as e:
        print(f"\n[{config.name}] ✗ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        return {
            "config": config_dict,
            "success": False,
            "error": str(e),
        }


def create_run_metadata(results_dir: Path) -> Dict:
    """Create metadata file for this run."""
    metadata = {
        "timestamp": datetime.now().isoformat(),
        "run_id": datetime.now().strftime("%Y%m%d_%H%M%S"),
    }
    
    # Try to get git commit hash
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent,
        )
        if result.returncode == 0:
            metadata["git_commit"] = result.stdout.strip()
    except Exception:
        pass
    
    # Try to get git branch
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parent.parent,
        )
        if result.returncode == 0:
            metadata["git_branch"] = result.stdout.strip()
    except Exception:
        pass
    
    metadata_path = results_dir / "run_metadata.json"
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)
    
    return metadata


def copy_prompts_file(results_dir: Path):
    """Copy prompts.py to the results directory."""
    prompts_source = Path(__file__).parent / "prompts.py"
    prompts_dest = results_dir / "prompts.py"
    
    if prompts_source.exists():
        shutil.copy2(prompts_source, prompts_dest)
        print(f"Copied prompts.py to {prompts_dest}")
    else:
        print(f"Warning: prompts.py not found at {prompts_source}")


def create_run_summary(results_dir: Path, results: List[Dict]):
    """Create a summary file of the run."""
    summary = {
        "run_timestamp": datetime.now().isoformat(),
        "models_tested": len(results),
        "models_successful": sum(1 for r in results if r.get("success", False)),
        "models_failed": sum(1 for r in results if not r.get("success", False)),
        "model_results": [],
    }
    
    for result in results:
        model_info = {
            "name": result["config"]["name"],
            "provider": result["config"]["provider"],
            "model": result["config"]["model"],
            "success": result.get("success", False),
        }
        
        if result.get("success"):
            model_info["results_count"] = result.get("results_count", 0)
            model_info["output_file"] = result.get("output_file", "")
        else:
            model_info["error"] = result.get("error", "Unknown error")
        
        summary["model_results"].append(model_info)
    
    summary_path = results_dir / "run_summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    
    print(f"\n{'='*80}")
    print("Run Summary:")
    print(f"{'='*80}")
    print(f"Models tested: {summary['models_tested']}")
    print(f"Successful: {summary['models_successful']}")
    print(f"Failed: {summary['models_failed']}")
    print(f"\nSummary saved to: {summary_path}")


def main():
    """Run all configured models in parallel."""
    # Configuration
    base_dir = Path(__file__).parent.parent
    subdocuments_path = str(base_dir / "web" / "src" / "data" / "subdocuments.parquet")
    dossiers_path = str(base_dir / "web" / "src" / "data" / "dossiers.parquet")
    markdown_base_path = str(base_dir / "pdf-parser" / "processed_markdown")
    
    # Reference dossiers from the plan
    reference_dossiers = {
        "191": "Completed Law (Standard) - Has AangenomenTekst + Verslag",
        "135": "Debated Bill (Ongoing) - Has Verslag + Voorstel, no Law",
        "30": "Floating Amendment (Stalled) - Has Voorstel + Amendement, no Verslag",
        "449": "Special (Budget) - Begroting",
        "200": "Various document types",
        "1131": "ET Telescoop"
    }
    
    # Create results directory with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_dir = Path(__file__).parent / "runs" / f"run_{timestamp}"
    results_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"\n{'='*80}")
    print("Starting parallel model run")
    print(f"Results will be saved to: {results_dir}")
    print(f"Models to test: {len(MODELS_TO_TEST)}")
    print(f"{'='*80}\n")
    
    # Create metadata
    metadata = create_run_metadata(results_dir)
    print(f"Run ID: {metadata['run_id']}")
    
    # Copy prompts file
    copy_prompts_file(results_dir)
    
    # Prepare dossier data for worker processes
    dossiers_data = {
        dossier_id: {
            "subdocuments_path": subdocuments_path,
            "dossiers_path": dossiers_path,
            "markdown_base_path": markdown_base_path,
            "description": description,
        }
        for dossier_id, description in reference_dossiers.items()
    }
    
    # Convert configs to dicts for multiprocessing
    config_dicts = [
        {
            "name": config.name,
            "provider": config.provider,
            "model": config.model,
            "client_class": config.client_class.__name__,
            "api_key_env": config.api_key_env,
        }
        for config in MODELS_TO_TEST
    ]
    
    # Run models in parallel
    results = []
    max_workers = min(len(MODELS_TO_TEST), os.cpu_count() or 4)
    
    print(f"\nRunning {len(MODELS_TO_TEST)} models with {max_workers} workers...\n")
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        # Convert results_dir to string for multiprocessing
        results_dir_str = str(results_dir)
        future_to_config = {
            executor.submit(
                process_single_model,
                config_dict,
                dossiers_data,
                results_dir_str,
            ): config_dict
            for config_dict in config_dicts
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_config):
            config_dict = future_to_config[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                print(f"\n✗ Fatal error processing {config_dict['name']}: {e}")
                results.append({
                    "config": config_dict,
                    "success": False,
                    "error": str(e),
                })
    
    # Create run summary
    create_run_summary(results_dir, results)
    
    # Generate/update runs index
    try:
        import subprocess
        import sys
        generate_index_script = Path(__file__).parent / "generate_runs_index.py"
        subprocess.run([sys.executable, str(generate_index_script)], check=False)
    except Exception as e:
        print(f"Warning: Could not generate runs index: {e}")
    
    print(f"\n{'='*80}")
    print("All runs completed!")
    print(f"Results directory: {results_dir}")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    main()

