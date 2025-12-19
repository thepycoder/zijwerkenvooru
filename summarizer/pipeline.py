"""Main pipeline for summarizing dossiers."""
from typing import Optional, Tuple
from .models import Dossier, DossierSummary
from .selector import select_core_document, get_debate_documents, get_advice_documents
from .llm import LLMClient
from .llm_utils import RetryInfo, CostInfo


def summarize_dossier(
    dossier: Dossier,
    llm_client: LLMClient,
) -> Optional[DossierSummary]:
    """
    Run the complete summarization pipeline on a dossier.
    
    Step 1: Select core document and generate fact summary
    Step 2: Analyze political context and debate
    """
    # Step 1: Select core document
    core_selection = select_core_document(dossier.subdocuments)
    
    if not core_selection:
        print(f"Warning: No suitable core document found for dossier {dossier.dossier_id}")
        return None
    
    core_doc = core_selection.document
    
    if not core_doc.content:
        print(f"Warning: Core document {core_doc.id} has no content loaded")
        return None
    
    # Step 1: Generate fact summary
    print(f"Step 1: Summarizing core document {core_doc.id} ({core_doc.document_type.value})...")
    fact_summary = llm_client.summarize_facts(
        document=core_doc,
        context=core_selection.selection_reason,
    )
    
    # Step 2: Get debate and advice documents
    debate_docs = get_debate_documents(dossier.subdocuments)
    advice_docs = get_advice_documents(dossier.subdocuments)
    
    # Step 2: Analyze political context
    print(f"Step 2: Analyzing political context ({len(debate_docs)} verslagen, {len(advice_docs)} adviezen)...")
    
    # If no debate documents, pass the core document for political analysis
    core_doc_for_politics = core_doc if not debate_docs else None
    
    # Call analyze_politics - retry logic and cost tracking are handled inside
    political_analysis = llm_client.analyze_politics(
        fact_summary=fact_summary,
        debate_documents=debate_docs,
        advice_documents=advice_docs,
        core_document=core_doc_for_politics,
        dossier_id=dossier.dossier_id,
    )
    
    # Extract retry and cost info if available
    retry_info = llm_client.get_retry_info(dossier.dossier_id)
    cost_info = llm_client.get_cost_info(dossier.dossier_id)
    
    # Generate a clear title
    print(f"Step 3: Generating clear title...")
    generated_title = llm_client.generate_title(dossier.title, fact_summary)
    
    summary = DossierSummary(
        dossier_id=dossier.dossier_id,
        title=dossier.title,
        generated_title=generated_title,
        fact_summary=fact_summary,
        political_analysis=political_analysis,
    )
    
    # Attach retry and cost info to summary (we'll extract this in run_all_models)
    summary._retry_info = retry_info
    summary._cost_info = cost_info
    
    return summary

