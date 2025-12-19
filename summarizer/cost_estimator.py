#!/usr/bin/env python3
"""Cost estimation tool for processing dossiers with different LLM models."""
import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

try:
    import tiktoken
    HAS_TIKTOKEN = True
except ImportError:
    HAS_TIKTOKEN = False
    print("Warning: tiktoken not installed. Install with: pip install tiktoken")

from .data_loader import load_dossier_from_parquet
from .pipeline import summarize_dossier
from .selector import select_core_document, get_debate_documents, get_advice_documents
from . import prompts


@dataclass
class ModelPricing:
    """Pricing configuration for an LLM model."""
    name: str
    input_price_per_million_tokens: float  # Price per million input tokens
    output_price_per_million_tokens: float  # Price per million output tokens
    encoding_name: str = "cl100k_base"  # tiktoken encoding name (default: GPT-4/Claude compatible)
    
    def cost(self, input_tokens: int, output_tokens: int) -> float:
        """Calculate cost for given input and output tokens."""
        input_cost = (input_tokens / 1_000_000) * self.input_price_per_million_tokens
        output_cost = (output_tokens / 1_000_000) * self.output_price_per_million_tokens
        return input_cost + output_cost


# Predefined model pricing configurations
# Prices are per million tokens (as of common pricing models)
MODEL_PRICING = {
    # OpenAI models
    "gpt-4o-mini": ModelPricing(
        name="gpt-4o-mini",
        input_price_per_million_tokens=0.15,
        output_price_per_million_tokens=0.6,
        encoding_name="o200k_base"
    ),
    "gpt-4o": ModelPricing(
        name="gpt-4o",
        input_price_per_million_tokens=2.5,
        output_price_per_million_tokens=10.0,
        encoding_name="o200k_base"
    ),
    "gpt-4-turbo": ModelPricing(
        name="gpt-4-turbo",
        input_price_per_million_tokens=10.0,
        output_price_per_million_tokens=30.0,
        encoding_name="cl100k_base"
    ),
    
    # Anthropic models
    "claude-3-5-sonnet-20241022": ModelPricing(
        name="claude-3-5-sonnet-20241022",
        input_price_per_million_tokens=3.0,
        output_price_per_million_tokens=15.0,
        encoding_name="cl100k_base"
    ),
    "claude-3-opus-20240229": ModelPricing(
        name="claude-3-opus-20240229",
        input_price_per_million_tokens=15.0,
        output_price_per_million_tokens=75.0,
        encoding_name="cl100k_base"
    ),
    "claude-3-haiku-20240307": ModelPricing(
        name="claude-3-haiku-20240307",
        input_price_per_million_tokens=0.25,
        output_price_per_million_tokens=1.25,
        encoding_name="cl100k_base"
    ),
    
    # Mistral models
    "mistral-large-2512": ModelPricing(
        name="mistral-large-2512",
        input_price_per_million_tokens=0.5,
        output_price_per_million_tokens=1.5,
        encoding_name="cl100k_base"
    ),
    "mistral-medium-2508": ModelPricing(
        name="mistral-medium-2508",
        input_price_per_million_tokens=0.4,
        output_price_per_million_tokens=2,
        encoding_name="cl100k_base"
    ),
    
    # Gemini models (approximate, using cl100k_base as proxy)
    "gemini-3-pro-preview": ModelPricing(
        name="gemini-3-pro-preview",
        input_price_per_million_tokens=1.25,
        output_price_per_million_tokens=5.0,
        encoding_name="cl100k_base"
    ),
    "gemini-3-flash-preview": ModelPricing(
        name="gemini-3-flash-preview",
        input_price_per_million_tokens=0.075,
        output_price_per_million_tokens=0.3,
        encoding_name="cl100k_base"
    ),
    
    # Anthropic Claude 4.5 models
    "claude-sonnet-4-5-20250929": ModelPricing(
        name="claude-sonnet-4-5-20250929",
        input_price_per_million_tokens=3.0,
        output_price_per_million_tokens=15.0,
        encoding_name="cl100k_base"
    ),
    "claude-haiku-4-5-20251001": ModelPricing(
        name="claude-haiku-4-5-20251001",
        input_price_per_million_tokens=0.25,
        output_price_per_million_tokens=1.25,
        encoding_name="cl100k_base"
    ),
    
    # Nebius/Kimi models
    "moonshotai/Kimi-K2-Thinking": ModelPricing(
        name="moonshotai/Kimi-K2-Thinking",
        input_price_per_million_tokens=0.5,
        output_price_per_million_tokens=2.0,
        encoding_name="cl100k_base"
    ),
}


def add_custom_model(
    name: str,
    input_price_per_million: float,
    output_price_per_million: float,
    encoding_name: str = "cl100k_base"
) -> None:
    """
    Add a custom model pricing configuration.
    
    Args:
        name: Model name/identifier
        input_price_per_million: Price per million input tokens
        output_price_per_million: Price per million output tokens
        encoding_name: tiktoken encoding name (default: "cl100k_base")
    """
    MODEL_PRICING[name] = ModelPricing(
        name=name,
        input_price_per_million_tokens=input_price_per_million,
        output_price_per_million_tokens=output_price_per_million,
        encoding_name=encoding_name
    )


def count_tokens(text: str, encoding_name: str = "cl100k_base") -> int:
    """Count tokens in text using tiktoken."""
    if not HAS_TIKTOKEN:
        # Fallback: rough estimate (1 token ≈ 4 characters for English/Dutch)
        return len(text) // 4
    
    try:
        encoding = tiktoken.get_encoding(encoding_name)
        return len(encoding.encode(text))
    except Exception as e:
        print(f"Warning: Could not use encoding {encoding_name}: {e}")
        # Fallback estimate
        return len(text) // 4


def estimate_fact_summary_tokens(
    document_content: str,
    context: str,
    model_pricing: ModelPricing
) -> Tuple[int, int]:
    """
    Estimate input and output tokens for the fact summary step.
    
    Returns:
        (input_tokens, output_tokens)
    """
    system_prompt = prompts.get_system_prompt_fact_summary()
    user_prompt = prompts.get_fact_summary_prompt(
        document=type('obj', (object,), {
            'content': document_content,
            'document_type': type('obj', (object,), {'value': 'Unknown'}),
            'id': 'unknown',
            'authors': []
        })(),
        context=context
    )
    
    input_tokens = count_tokens(system_prompt, model_pricing.encoding_name)
    input_tokens += count_tokens(user_prompt, model_pricing.encoding_name)
    
    # Estimate output: fact summaries are typically 10-20% of input document length
    # But with a minimum of 200 and maximum of 1000 tokens
    doc_tokens = count_tokens(document_content, model_pricing.encoding_name)
    output_tokens = max(200, min(1000, int(doc_tokens * 0.15)))
    
    return input_tokens, output_tokens


def estimate_political_analysis_tokens(
    fact_summary_text: str,
    debate_documents: List[str],
    advice_documents: List[str],
    core_document_content: Optional[str],
    has_debate: bool,
    model_pricing: ModelPricing
) -> Tuple[int, int]:
    """
    Estimate input and output tokens for the political analysis step.
    
    Returns:
        (input_tokens, output_tokens)
    """
    system_prompt = prompts.get_system_prompt_political_analysis()
    
    if has_debate:
        # Create mock documents for prompt generation
        mock_debate_docs = [
            type('obj', (object,), {
                'id': f'doc{i}',
                'date': '2024-01-01',
                'content': content
            })()
            for i, content in enumerate(debate_documents)
        ]
        mock_advice_docs = [
            type('obj', (object,), {
                'id': f'advice{i}',
                'date': '2024-01-01',
                'content': content
            })()
            for i, content in enumerate(advice_documents)
        ]
        mock_fact_summary = type('obj', (object,), {'summary': fact_summary_text})()
        
        user_prompt = prompts.get_political_analysis_prompt_with_debate(
            mock_fact_summary,
            mock_debate_docs,
            mock_advice_docs
        )
        
        input_tokens = count_tokens(system_prompt, model_pricing.encoding_name)
        input_tokens += count_tokens(user_prompt, model_pricing.encoding_name)
        
        # Estimate output: political analysis JSON is typically 5-10% of total input
        # But with a minimum of 300 and maximum of 1500 tokens
        output_tokens = max(300, min(1500, int(input_tokens * 0.08)))
    else:
        # No debate available, no LLM call is made, so no tokens used
        input_tokens = 0
        output_tokens = 0
    
    return input_tokens, output_tokens


def estimate_dossier_tokens(
    dossier_id: str,
    subdocuments_path: str,
    dossiers_path: str,
    markdown_base_path: str,
    model_pricing: ModelPricing
) -> Tuple[int, int, Dict]:
    """
    Estimate tokens needed to process a single dossier.
    
    Returns:
        (total_input_tokens, total_output_tokens, breakdown_dict)
    """
    # Load dossier
    dossier = load_dossier_from_parquet(
        dossier_id=dossier_id,
        subdocuments_path=subdocuments_path,
        dossiers_path=dossiers_path,
        markdown_base_path=markdown_base_path,
    )
    
    if not dossier:
        return 0, 0, {"error": "Dossier not found"}
    
    # Step 1: Select core document
    core_selection = select_core_document(dossier.subdocuments)
    if not core_selection:
        return 0, 0, {"error": "No suitable core document found"}
    
    core_doc = core_selection.document
    if not core_doc.content:
        return 0, 0, {"error": "Core document has no content"}
    
    # Step 1: Estimate fact summary tokens
    fact_input, fact_output = estimate_fact_summary_tokens(
        core_doc.content,
        core_selection.selection_reason,
        model_pricing
    )
    
    # Step 2: Get debate and advice documents
    debate_docs = get_debate_documents(dossier.subdocuments)
    advice_docs = get_advice_documents(dossier.subdocuments)
    
    # Step 2: Estimate political analysis tokens
    has_debate = len(debate_docs) > 0
    debate_contents = [doc.content or "" for doc in debate_docs]
    advice_contents = [doc.content or "" for doc in advice_docs]
    core_doc_for_politics = core_doc.content if not has_debate else None
    
    # Estimate fact summary: use the actual output token estimate from step 1
    # Convert output tokens back to approximate text length for prompt generation
    # Average token is ~4 characters, so estimate text length
    estimated_fact_summary_tokens = max(200, min(1000, int(count_tokens(core_doc.content or "", model_pricing.encoding_name) * 0.15)))
    estimated_fact_summary_chars = estimated_fact_summary_tokens * 4  # Rough estimate: 4 chars per token
    # Create a realistic placeholder fact summary for token counting
    # Use a sample Dutch text pattern that approximates summary length
    sample_text = "Dit document behandelt belangrijke wijzigingen in de wetgeving. " * (estimated_fact_summary_chars // 50)
    estimated_fact_summary = sample_text[:estimated_fact_summary_chars] if estimated_fact_summary_chars > 0 else "Samenvatting van het document."
    
    pol_input, pol_output = estimate_political_analysis_tokens(
        estimated_fact_summary,
        debate_contents,
        advice_contents,
        core_doc_for_politics,
        has_debate,
        model_pricing
    )
    
    total_input = fact_input + pol_input
    total_output = fact_output + pol_output
    
    breakdown = {
        "dossier_id": dossier_id,
        "title": dossier.title,
        "fact_summary": {
            "input_tokens": fact_input,
            "output_tokens": fact_output,
        },
        "political_analysis": {
            "input_tokens": pol_input,
            "output_tokens": pol_output,
            "has_debate": has_debate,
            "debate_docs_count": len(debate_docs),
            "advice_docs_count": len(advice_docs),
        },
        "total": {
            "input_tokens": total_input,
            "output_tokens": total_output,
        }
    }
    
    return total_input, total_output, breakdown


def get_all_dossier_ids(dossiers_path: str) -> List[str]:
    """Get all dossier IDs from the parquet file."""
    try:
        import duckdb
        conn = duckdb.connect()
        query = f"SELECT DISTINCT id FROM '{dossiers_path}' ORDER BY id"
        result = conn.execute(query).df()
        conn.close()
        return result['id'].tolist()
    except ImportError:
        import pandas as pd
        df = pd.read_parquet(dossiers_path)
        return df['id'].unique().tolist()


def estimate_backlog_cost(
    model_pricing: ModelPricing,
    subdocuments_path: str,
    dossiers_path: str,
    markdown_base_path: str,
    sample_size: Optional[int] = None,
    verbose: bool = True
) -> Dict:
    """
    Estimate the cost of processing the full backlog of dossiers.
    
    Args:
        model_pricing: Model pricing configuration
        subdocuments_path: Path to subdocuments.parquet
        dossiers_path: Path to dossiers.parquet
        markdown_base_path: Base path to markdown files
        sample_size: If provided, only process a sample of dossiers for estimation
        verbose: Print progress information
    
    Returns:
        Dictionary with cost estimates and breakdown
    """
    # Get all dossier IDs
    all_dossier_ids = get_all_dossier_ids(dossiers_path)
    
    if sample_size and sample_size < len(all_dossier_ids):
        import random
        sample_dossier_ids = random.sample(all_dossier_ids, sample_size)
        if verbose:
            print(f"Processing sample of {sample_size} dossiers out of {len(all_dossier_ids)} total")
    else:
        sample_dossier_ids = all_dossier_ids
    
    total_input_tokens = 0
    total_output_tokens = 0
    successful_dossiers = 0
    failed_dossiers = 0
    dossier_breakdowns = []
    
    if verbose:
        print(f"\nEstimating costs for model: {model_pricing.name}")
        print(f"Processing {len(sample_dossier_ids)} dossiers...")
        print("-" * 80)
    
    for i, dossier_id in enumerate(sample_dossier_ids, 1):
        try:
            input_tokens, output_tokens, breakdown = estimate_dossier_tokens(
                dossier_id,
                subdocuments_path,
                dossiers_path,
                markdown_base_path,
                model_pricing
            )
            
            if "error" in breakdown:
                failed_dossiers += 1
                if verbose:
                    print(f"[{i}/{len(sample_dossier_ids)}] Dossier {dossier_id}: {breakdown['error']}")
            else:
                total_input_tokens += input_tokens
                total_output_tokens += output_tokens
                successful_dossiers += 1
                dossier_breakdowns.append(breakdown)
                
                if verbose and i % 10 == 0:
                    cost_so_far = model_pricing.cost(total_input_tokens, total_output_tokens)
                    print(f"[{i}/{len(sample_dossier_ids)}] Processed {successful_dossiers} dossiers, "
                          f"cost so far: ${cost_so_far:.2f}")
        
        except Exception as e:
            failed_dossiers += 1
            if verbose:
                print(f"[{i}/{len(sample_dossier_ids)}] Error processing dossier {dossier_id}: {e}")
    
    # Calculate costs
    sample_cost = model_pricing.cost(total_input_tokens, total_output_tokens)
    
    # Extrapolate to full backlog if using sample
    if sample_size and len(sample_dossier_ids) < len(all_dossier_ids):
        avg_tokens_per_dossier_input = total_input_tokens / successful_dossiers if successful_dossiers > 0 else 0
        avg_tokens_per_dossier_output = total_output_tokens / successful_dossiers if successful_dossiers > 0 else 0
        
        estimated_total_input = avg_tokens_per_dossier_input * len(all_dossier_ids)
        estimated_total_output = avg_tokens_per_dossier_output * len(all_dossier_ids)
        estimated_total_cost = model_pricing.cost(estimated_total_input, estimated_total_output)
    else:
        estimated_total_input = total_input_tokens
        estimated_total_output = total_output_tokens
        estimated_total_cost = sample_cost
    
    result = {
        "model": model_pricing.name,
        "sample_size": len(sample_dossier_ids),
        "total_dossiers": len(all_dossier_ids),
        "successful_dossiers": successful_dossiers,
        "failed_dossiers": failed_dossiers,
        "tokens": {
            "sample_input": total_input_tokens,
            "sample_output": total_output_tokens,
            "estimated_total_input": int(estimated_total_input),
            "estimated_total_output": int(estimated_total_output),
        },
        "costs": {
            "sample_cost": sample_cost,
            "estimated_total_cost": estimated_total_cost,
            "cost_per_dossier": sample_cost / successful_dossiers if successful_dossiers > 0 else 0,
        },
        "pricing": {
            "input_per_million": model_pricing.input_price_per_million_tokens,
            "output_per_million": model_pricing.output_price_per_million_tokens,
        },
        "dossier_breakdowns": dossier_breakdowns if verbose else None,
    }
    
    return result


def main():
    """Main function to run cost estimation."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Estimate costs for processing dossiers with different LLM models")
    parser.add_argument("--model", type=str, help="Model name (or 'all' for all models)", default="all")
    parser.add_argument("--sample", type=int, help="Sample size (process subset for faster estimation)", default=None)
    parser.add_argument("--output", type=str, help="Output JSON file path", default=None)
    parser.add_argument("--verbose", action="store_true", help="Print detailed progress")
    
    args = parser.parse_args()
    
    # Set up paths
    base_dir = Path(__file__).parent.parent
    subdocuments_path = str(base_dir / "web" / "src" / "data" / "subdocuments.parquet")
    dossiers_path = str(base_dir / "web" / "src" / "data" / "dossiers.parquet")
    markdown_base_path = str(base_dir / "pdf-parser" / "processed_markdown")
    
    # Check if files exist
    if not os.path.exists(subdocuments_path):
        print(f"Error: {subdocuments_path} not found")
        return
    if not os.path.exists(dossiers_path):
        print(f"Error: {dossiers_path} not found")
        return
    
    # Determine which models to process
    if args.model.lower() == "all":
        models_to_process = list(MODEL_PRICING.keys())
    else:
        if args.model not in MODEL_PRICING:
            print(f"Error: Model '{args.model}' not found in MODEL_PRICING")
            print(f"Available models: {', '.join(MODEL_PRICING.keys())}")
            return
        models_to_process = [args.model]
    
    # Process each model
    all_results = {}
    
    for model_name in models_to_process:
        print(f"\n{'='*80}")
        print(f"Processing model: {model_name}")
        print(f"{'='*80}")
        
        model_pricing = MODEL_PRICING[model_name]
        result = estimate_backlog_cost(
            model_pricing,
            subdocuments_path,
            dossiers_path,
            markdown_base_path,
            sample_size=args.sample,
            verbose=args.verbose
        )
        
        all_results[model_name] = result
        
        # Print summary
        print(f"\n{'='*80}")
        print(f"Results for {model_name}:")
        print(f"{'='*80}")
        print(f"Total dossiers: {result['total_dossiers']}")
        print(f"Successfully processed: {result['successful_dossiers']}")
        print(f"Failed: {result['failed_dossiers']}")
        print(f"\nTokens:")
        print(f"  Estimated total input: {result['tokens']['estimated_total_input']:,}")
        print(f"  Estimated total output: {result['tokens']['estimated_total_output']:,}")
        print(f"\nCosts:")
        print(f"  Estimated total cost: ${result['costs']['estimated_total_cost']:.2f}")
        print(f"  Cost per dossier: ${result['costs']['cost_per_dossier']:.4f}")
        print(f"\nPricing:")
        print(f"  Input: ${result['pricing']['input_per_million']:.2f} per million tokens")
        print(f"  Output: ${result['pricing']['output_per_million']:.2f} per million tokens")
    
    # Save results if output file specified
    if args.output:
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(all_results, f, indent=2, ensure_ascii=False)
        print(f"\nResults saved to: {args.output}")
    
    # Print comparison table if multiple models
    if len(all_results) > 1:
        print(f"\n{'='*80}")
        print("Cost Comparison:")
        print(f"{'='*80}")
        print(f"{'Model':<40} {'Total Cost':<15} {'Cost/Dossier':<15}")
        print("-" * 80)
        for model_name, result in sorted(all_results.items(), key=lambda x: x[1]['costs']['estimated_total_cost']):
            print(f"{model_name:<40} ${result['costs']['estimated_total_cost']:>12.2f}  "
                  f"${result['costs']['cost_per_dossier']:>12.4f}")


if __name__ == "__main__":
    main()

