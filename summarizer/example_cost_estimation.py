#!/usr/bin/env python3
"""Example script showing how to use the cost estimator."""
from pathlib import Path
from summarizer.cost_estimator import (
    ModelPricing,
    MODEL_PRICING,
    add_custom_model,
    estimate_backlog_cost,
)

# Example 1: Use a predefined model
def example_predefined_model():
    """Example using a predefined model."""
    print("Example 1: Using predefined model pricing")
    print("-" * 80)
    
    # Get model pricing
    model_pricing = MODEL_PRICING["gpt-4o-mini"]
    
    # Set up paths
    base_dir = Path(__file__).parent.parent
    subdocuments_path = str(base_dir / "web" / "src" / "data" / "subdocuments.parquet")
    dossiers_path = str(base_dir / "web" / "src" / "data" / "dossiers.parquet")
    markdown_base_path = str(base_dir / "pdf-parser" / "processed_markdown")
    
    # Estimate costs (using a small sample for quick demo)
    result = estimate_backlog_cost(
        model_pricing=model_pricing,
        subdocuments_path=subdocuments_path,
        dossiers_path=dossiers_path,
        markdown_base_path=markdown_base_path,
        sample_size=10,  # Process only 10 dossiers for quick estimation
        verbose=True
    )
    
    print(f"\nEstimated total cost: ${result['costs']['estimated_total_cost']:.2f}")
    print(f"Cost per dossier: ${result['costs']['cost_per_dossier']:.4f}")


# Example 2: Add a custom model
def example_custom_model():
    """Example adding and using a custom model."""
    print("\n\nExample 2: Adding custom model pricing")
    print("-" * 80)
    
    # Add a custom model
    add_custom_model(
        name="my-custom-model",
        input_price_per_million=1.0,
        output_price_per_million=3.0,
        encoding_name="cl100k_base"
    )
    
    # Use the custom model
    custom_model = MODEL_PRICING["my-custom-model"]
    print(f"Added custom model: {custom_model.name}")
    print(f"  Input: ${custom_model.input_price_per_million_tokens}/M tokens")
    print(f"  Output: ${custom_model.output_price_per_million_tokens}/M tokens")
    
    # Calculate cost for example tokens
    example_input_tokens = 10000
    example_output_tokens = 2000
    cost = custom_model.cost(example_input_tokens, example_output_tokens)
    print(f"\nExample cost for {example_input_tokens:,} input + {example_output_tokens:,} output tokens:")
    print(f"  ${cost:.4f}")


# Example 3: Compare multiple models
def example_compare_models():
    """Example comparing costs across multiple models."""
    print("\n\nExample 3: Comparing multiple models")
    print("-" * 80)
    
    models_to_compare = [
        "gpt-4o-mini",
        "claude-3-haiku-20240307",
        "mistral-medium-2508",
    ]
    
    print("Model comparison (using example token counts):")
    print(f"{'Model':<40} {'10K input + 2K output':<25} {'Cost':<15}")
    print("-" * 80)
    
    for model_name in models_to_compare:
        if model_name in MODEL_PRICING:
            model = MODEL_PRICING[model_name]
            cost = model.cost(10000, 2000)
            print(f"{model_name:<40} {'10,000 + 2,000 tokens':<25} ${cost:>12.4f}")


if __name__ == "__main__":
    # Run examples
    example_predefined_model()
    example_custom_model()
    example_compare_models()
    
    print("\n\n" + "=" * 80)
    print("To run full cost estimation from command line:")
    print("  python -m summarizer.cost_estimator --model all --sample 50")
    print("  python -m summarizer.cost_estimator --model gpt-4o-mini --output results.json")
    print("=" * 80)

