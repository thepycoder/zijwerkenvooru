import os
import sys
import pandas as pd
import json
from pathlib import Path
from datetime import datetime
import dataclasses

# Add current directory to path so we can import summarizer modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from summarizer.data_loader import load_dossier_from_parquet
from summarizer.pipeline import summarize_dossier
from summarizer.llm import LLMClient, GeminiLLMClient, AnthropicLLMClient, MistralLLMClient

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "web" / "src" / "data"
DOSSIERS_PATH = DATA_DIR / "dossiers.parquet"
SUBDOCS_PATH = DATA_DIR / "subdocuments.parquet"
SUMMARIES_PATH = DATA_DIR / "dossier_summaries.parquet"
MARKDOWN_DIR = BASE_DIR / "pdf-parser" / "processed_markdown"

def get_existing_summarized_ids():
    if not SUMMARIES_PATH.exists():
        return set()
    try:
        df = pd.read_parquet(SUMMARIES_PATH)
        return set(df['dossier_id'].astype(str).tolist())
    except Exception as e:
        print(f"Error reading existing summaries: {e}")
        return set()

def save_summary(summary, model_name):
    """
    Append a single summary to the parquet file.
    We do this incrementally or batch-wise.
    """
    
    # Convert dataclasses to dicts/json
    political_analysis_dict = dataclasses.asdict(summary.political_analysis)
    fact_summary_dict = dataclasses.asdict(summary.fact_summary)
    
    # Extract cost info if available (it's attached as private attribute in pipeline)
    retry_info = getattr(summary, '_retry_info', None)
    cost_info = getattr(summary, '_cost_info', None)
    
    row = {
        'dossier_id': str(summary.dossier_id),
        'title': summary.title,
        'generated_title': summary.generated_title,
        'fact_summary': json.dumps(fact_summary_dict),
        'political_analysis': json.dumps(political_analysis_dict),
        'model': model_name,
        'timestamp': datetime.now().isoformat(),
        'tokens_input': cost_info.input_tokens if cost_info else 0,
        'tokens_output': cost_info.output_tokens if cost_info else 0,
        'cost_usd': cost_info.total_cost if cost_info else 0.0,
        'retry_count': retry_info.json_parse_retries if retry_info else 0,
    }
    
    df_new = pd.DataFrame([row])
    
    if SUMMARIES_PATH.exists():
        try:
            df_existing = pd.read_parquet(SUMMARIES_PATH)
            # Remove existing entry for this dossier if we are re-processing
            df_existing = df_existing[df_existing['dossier_id'] != str(summary.dossier_id)]
            df_final = pd.concat([df_existing, df_new], ignore_index=True)
        except Exception:
            df_final = df_new
    else:
        df_final = df_new
        
    df_final.to_parquet(SUMMARIES_PATH)
    print(f"Saved summary for dossier {summary.dossier_id}")

def main():
    print("Starting incremental dossier summarization...")
    
    # 1. Check if files exist
    if not DOSSIERS_PATH.exists() or not SUBDOCS_PATH.exists():
        print("Error: Input parquet files not found.")
        return

    # 2. Get processed markdown folders
    if not MARKDOWN_DIR.exists():
        print("No processed markdown directory found.")
        return
        
    available_dossiers = [d.name for d in MARKDOWN_DIR.iterdir() if d.is_dir()]
    print(f"Found {len(available_dossiers)} dossiers with markdown content.")
    
    # 3. Get already summarized dossiers
    existing_ids = get_existing_summarized_ids()
    print(f"Found {len(existing_ids)} existing summaries.")
    
    # 4. Identify new dossiers
    new_dossiers = [d for d in available_dossiers if d not in existing_ids]
    
    # DEBUG: Only process specific reference dossiers for testing
    debug_ids = ["191", "135", "30", "200", "1131", "52", "888", "416", "1177", "433"]
    new_dossiers = [d for d in new_dossiers if d in debug_ids]
    print(f"DEBUG: Filtered to {len(new_dossiers)} reference dossiers.")

    print(f"Found {len(new_dossiers)} new dossiers to summarize.")
    
    if not new_dossiers:
        print("No new dossiers to process.")
        return

    # 5. Initialize LLM    
    provider = "mistral"
    model_name = "mistral-large-2512"

    print(f"Using model: {model_name}")
    
    if provider == "google":
        llm_client = GeminiLLMClient(model=model_name)
    elif provider == "anthropic":
        llm_client = AnthropicLLMClient(model=model_name)
    elif provider == "mistral":
        llm_client = MistralLLMClient(model=model_name)
    else:
        print(f"Error: Unknown provider {provider}")
        return

    # 6. Process
    for dossier_id in new_dossiers:
        print(f"\nProcessing dossier {dossier_id}...")
        try:
            dossier = load_dossier_from_parquet(
                dossier_id=dossier_id,
                subdocuments_path=str(SUBDOCS_PATH),
                dossiers_path=str(DOSSIERS_PATH),
                markdown_base_path=str(MARKDOWN_DIR)
            )
            
            if not dossier:
                print(f"Could not load dossier {dossier_id} from parquet.")
                continue
                
            summary = summarize_dossier(dossier, llm_client)
            
            if summary:
                save_summary(summary, model_name)
            else:
                print(f"Failed to generate summary for {dossier_id}")
                
        except Exception as e:
            print(f"Error processing dossier {dossier_id}: {e}")
            import traceback
            traceback.print_exc()

    print("\nDone!")

if __name__ == "__main__":
    main()

