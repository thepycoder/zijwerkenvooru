#!/usr/bin/env python3
"""Main execution script for testing the summarization pipeline."""
import json
import os
from pathlib import Path
from .data_loader import load_dossier_from_parquet
from .pipeline import summarize_dossier
from .llm import MockLLMClient, OpenAILLMClient, AnthropicLLMClient, MistralLLMClient, GeminiLLMClient, NebiusLLMClient


def main():
    """Run the pipeline on reference dossiers."""
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
        "200": "Additional Example - Various document types",
    }
    
    # Choose LLM client
    llm_mode = os.getenv("LLM_MODE", "mock").lower()
    model_name = None
    
    if llm_mode == "openai":
        model_name = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        llm_client = OpenAILLMClient(model=model_name)
        print(f"Using OpenAI API with model: {model_name}")
    elif llm_mode == "anthropic":
        model_name = os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-20241022")
        llm_client = AnthropicLLMClient(model=model_name)
        print(f"Using Anthropic API with model: {model_name}")
    elif llm_mode == "mistral":
        model_name = os.getenv("MISTRAL_MODEL", "mistral-medium-2508")
        llm_client = MistralLLMClient(model=model_name)
        print(f"Using Mistral AI API with model: {model_name}")
    elif llm_mode == "gemini":
        model_name = os.getenv("GEMINI_MODEL", "gemini-3-pro-preview")
        llm_client = GeminiLLMClient(model=model_name)
        print(f"Using Google Gemini API with model: {model_name}")
    elif llm_mode == "nebius":
        model_name = os.getenv("NEBIUS_MODEL", "moonshotai/Kimi-K2-Thinking")
        llm_client = NebiusLLMClient(model=model_name)
        print(f"Using Nebius API with model: {model_name}")
    else:
        llm_client = MockLLMClient()
        model_name = "mock"
        print("Using Mock LLM (set LLM_MODE=openai|anthropic|mistral|gemini|nebius to use real API)")
    
    # Create output filename with model name
    # Sanitize model name for filename (replace slashes and special chars)
    safe_model_name = model_name.replace("/", "_").replace(":", "_").replace(" ", "_")
    output_file = base_dir / "summarizer" / f"results_{safe_model_name}.json"
    
    # Initialize results dictionary (will be updated incrementally)
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
    
    for dossier_id, description in reference_dossiers.items():
        print(f"\n{'='*80}")
        print(f"Processing dossier {dossier_id}: {description}")
        print(f"{'='*80}\n")
        
        try:
            # Load dossier
            dossier = load_dossier_from_parquet(
                dossier_id=dossier_id,
                subdocuments_path=subdocuments_path,
                dossiers_path=dossiers_path,
                markdown_base_path=markdown_base_path,
            )
            
            if not dossier:
                print(f"Error: Dossier {dossier_id} not found")
                continue
            
            print(f"Loaded dossier: {dossier.title}")
            print(f"Subdocuments: {len(dossier.subdocuments)}")
            print(f"Document types: {', '.join(set(doc.document_type.value for doc in dossier.subdocuments))}")
            
            # Run pipeline
            summary = summarize_dossier(dossier, llm_client)
            
            if summary:
                result_data = {
                    "dossier_id": summary.dossier_id,
                    "title": summary.title,
                    "generated_title": summary.generated_title,
                    "model": model_name,
                    "fact_summary": {
                        "summary": summary.fact_summary.summary,
                        "document_type": summary.fact_summary.document_type,
                        "selection_reason": summary.fact_summary.selection_reason,
                    },
                    "political_analysis": {
                        "arguments_for": summary.political_analysis.arguments_for,
                        "arguments_against": summary.political_analysis.arguments_against,
                        "neutral_technical": summary.political_analysis.neutral_technical,
                        "notable_changes": summary.political_analysis.notable_changes,
                        "has_debate": summary.political_analysis.has_debate,
                    },
                }
                
                # Update results and write immediately
                results[dossier_id] = result_data
                
                # Write updated results to file immediately
                with open(output_file, "w", encoding="utf-8") as f:
                    json.dump(results, f, indent=2, ensure_ascii=False)
                
                print(f"\n✓ Summary generated successfully and saved to {output_file}")
            else:
                print(f"\n✗ Failed to generate summary for dossier {dossier_id}")
                
        except Exception as e:
            print(f"\n✗ Error processing dossier {dossier_id}: {e}")
            import traceback
            traceback.print_exc()
    
    print(f"\n{'='*80}")
    print(f"All results saved to: {output_file}")
    print(f"{'='*80}\n")
    
    # Print summary
    for dossier_id, result in results.items():
        print(f"\nDossier {dossier_id}: {result['title']}")
        print(f"  Fact summary: {result['fact_summary']['summary'][:100]}...")
        print(f"  Has debate: {result['political_analysis']['has_debate']}")
        print(f"  Arguments for: {len(result['political_analysis']['arguments_for'])}")
        print(f"  Arguments against: {len(result['political_analysis']['arguments_against'])}")


if __name__ == "__main__":
    main()

