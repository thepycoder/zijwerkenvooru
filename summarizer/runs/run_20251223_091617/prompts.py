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
Gebruik geen onnodige tekst, kort en bondig. Gebruik zo weinig mogelijk jargon en technische termen.

BELANGRIJK - Formatteer je samenvatting EXACT als volgt:

## 1. Welk probleem lost dit op of wat is het doel?

[Schrijf hier één volledige alinea (1-2 zinnen) die het probleem, doel of reden van invoering uitlegt. Gebruik **vetgedrukt** voor belangrijke termen, concepten, of kernwoorden die de lezer moet onthouden.]

## 2. Wat verandert er concreet in de praktijk?

- [Eerste concrete verandering - kort en helder]
- [Tweede concrete verandering - kort en helder]
- [Derde concrete verandering - kort en helder]
[Voeg meer bulletpoints toe indien nodig]

BELANGRIJK:
- Gebruik EXACT de bovenstaande markdown titels (## 1. en ## 2.) - kopieer ze letterlijk
- Start direct met de eerste titel, geen extra tekst ervoor
- Voor sectie 1: gebruik één alinea met **vetgedrukte** belangrijke termen
- Voor sectie 2: gebruik ENKEL enkele bulletpoints (geen 'nested' bullets, geen nummering)
- Maak het visueel aantrekkelijk - gebruik strategisch vetgedrukt voor kernconcepten
- Houd het objectief - geen politieke interpretatie, alleen feiten

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
1. Vat het algemene debat samen in het kort. Geef aan of er consensus is of strijd. De argumenten zullen erboven al toegelicht zijn, dus focus op het algemene beeld.
2. Identificeer de standpunten van de verschillende partijen en vat ze samen in bulletpoints per partij.
3. Indien beschikbaar, voeg 1 extra "partij" label toe met technische adviezen, zoals van de Raad van State.


BELANGRIJK:
- Herhaal niet de samenvatting van het dossier zelf, focus enkel op het debat.
- Focus op de MEEST RELEVANTE argumenten - niet elk klein detail
- Streef naar 1-3 argumenten per partij
- Als er veel argumenten zijn, kies de belangrijkste die het debat echt karakteriseren
- Groepeer argumenten waar mogelijk: als dezelfde partijen meerdere gerelateerde argumenten hebben, combineer ze tot één samenhangend partijlabel (Partij A, Partij B, etc.)
- Kwaliteit boven kwantiteit: liever 3 sterke, duidelijke argumenten dan 10 kleine details
- Er is vaak heel veel achtergrond en context in de documenten, focus op de hoofdstukken waar echt debat wordt gevoerd.

Formatteer je antwoord als JSON met deze structuur:
{{
    "summary_debate": "Vat de grote lijnen van de debatten heel kort samen. Gebruik in beperkte mate **vetgedrukt** voor belangrijke termen en concepten."
    "arguments": [{{"party": "Partij naam of lijst van partijnamen / Raad van State / Technisch advies", "arguments": "Argumenten tekst in bulletpoints"}}],
}}

Zorg zeker voor een correcte JSON structuur en geen extra tekst.

SAMENVATTING VAN HET DOSSIER:
{fact_summary.summary}

VERSLAGEN VAN DE DEBATTEN:
{debate_text}

TECHNISCHE OPMERKINGEN:
{advice_text if advice_text else 'geen'}

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
- Niet sensationeel, droog en professioneel.

Geef alleen de nieuwe titel terug, zonder extra uitleg of aanhalingstekens.
"""

