"""Centralized prompts for LLM summarization pipeline."""
from typing import List
from .models import Subdocument, FactSummary


def get_fact_summary_prompt(
    document: Subdocument,
    context: str,
) -> str:
    """
    Generate the prompt for Step 1: Objective fact-based summary.
    
    Args:
        document: The document to summarize
        context: Selection reason/context for the document
    
    Returns:
        Formatted prompt string
    """
    authors_text = ', '.join(document.authors) if document.authors else 'Onbekend'
    content = document.content if document.content else '[Geen inhoud]'
    
    return f"""Je bent een objectieve samenvatter van parlementaire documenten.

Context: {context}

Document type: {document.document_type.value}
Document ID: {document.id}
Auteurs: {authors_text}

Taak: Vat dit document objectief samen in heldere taal voor een burger zonder juridische kennis.
Gebruik geen onnodige tekst, kort en bondig. Een gewone kiezer moet dit kunnen lezen, te lang en je bent ze kwijt.

Focus op:
1. Welk probleem lost dit op of wat is het doel?
2. Wat verandert er concreet in de praktijk?
3. Wie wordt hierdoor geraakt?

Gebruik deze 3 punten als titels in je samenvatting. Start je samenvatting met de eerste titel, geen extra tekst.

Houd het objectief - geen politieke interpretatie, alleen feiten.

Document inhoud:
{content}
"""


def get_political_analysis_prompt_with_debate(
    fact_summary: FactSummary,
    debate_documents: List[Subdocument],
    advice_documents: List[Subdocument],
) -> str:
    """
    Generate the prompt for Step 2: Political analysis when debate documents are available.
    
    Args:
        fact_summary: The fact summary from Step 1
        debate_documents: List of Verslag documents
        advice_documents: List of advice documents (Raad van State, etc.)
    
    Returns:
        Formatted prompt string
    """
    debate_text = "\n\n---\n\n".join([
        f"Verslag {doc.id} ({doc.date}):\n{doc.content if doc.content else '[Geen inhoud]'}"
        for doc in debate_documents
    ])
    
    advice_text = ""
    if advice_documents:
        advice_text = "\n\n---\n\n".join([
            f"Advies {doc.id} ({doc.date}):\n{doc.content if doc.content else '[Geen inhoud]'}"
            for doc in advice_documents
        ])
    
    return f"""Je bent een politiek analist. Je taak is om een dossier te analyseren en samen te vatten voor een gewone kiezer. Focus op wat de kiezer interesseert, geen juridische details.

Taak:
1. Identificeer de argumenten VOOR dit voorstel
2. Identificeer de argumenten TEGEN of bezorgdheden
3. Geef aan of er consensus is of strijd
4. Als er in amendementen of adviezen fundamentele wijzigingen aan de inhoud besproken worden, geef deze op in de "notable_changes" sectie. Anders laat je deze sectie leeg.

Formatteer je antwoord als JSON met deze structuur:
{{
    "arguments_for": [{{"party": "Partij naam/namen", "argument": "Argument tekst"}}],
    "arguments_against": [{{"party": "Partij naam/namen", "argument": "Argument tekst"}}],
    "notable_changes": "Eventuele opvallende wijzigingen of amendementen die werden besproken"
}}

SAMENVATTING VAN HET DOSSIER:
{fact_summary.summary}

VERSLAGEN VAN DE DEBATTEN:
{debate_text}

TECHNISCHE OPMERKINGEN:
{advice_text}' if advice_text else 'geen'

"""


def get_system_prompt_fact_summary() -> str:
    """System prompt for fact summarization."""
    return "Je bent een objectieve samenvatter van parlementaire documenten."


def get_system_prompt_political_analysis() -> str:
    """System prompt for political analysis."""
    return "Je bent een politiek analist van het Belgische parlement. Je antwoordt altijd in geldig JSON formaat."


def get_title_generation_prompt(
    original_title: str,
    fact_summary: FactSummary,
) -> str:
    """
    Generate the prompt for generating a clear, non-legalese title.
    
    Args:
        original_title: The original legal title
        fact_summary: The fact summary from Step 1
    
    Returns:
        Formatted prompt string
    """
    return f"""Je bent een communicatiespecialist. Hier is de originele titel van een parlementair dossier:

ORIGINELE TITEL:
{original_title}

SAMENVATTING:
{fact_summary.summary}

Taak: Genereer een nieuwe, duidelijke titel die beter weergeeft waar dit dossier over gaat, zonder juridische jargon. De titel moet:
- Begrijpelijk zijn voor burgers zonder juridische kennis
- Direct duidelijk maken wat het onderwerp is
- Kort en bondig zijn (maximaal 15 woorden)
- Geen juridische termen zoals "Wetsontwerp tot wijziging van..." bevatten

Geef alleen de nieuwe titel terug, zonder extra uitleg of aanhalingstekens.
"""

