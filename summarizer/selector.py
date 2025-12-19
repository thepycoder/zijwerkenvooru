"""Document selector logic for choosing the core document to summarize."""
from typing import List, Optional
from .models import DocumentType, Subdocument, CoreDocumentSelection


def select_core_document(subdocuments: List[Subdocument]) -> Optional[CoreDocumentSelection]:
    """
    Selects the core document to summarize based on hierarchy.
    
    Priority order:
    1. AangenomenTekst (definitive law)
    2. Special documents (Regeerakkoord, Begroting, Beleidsverklaring)
    3. WetsOntwerp, WetsVoorstel, VoorstelTotHerziening (proposals)
    4. VoorstelVanResolutie (resolution proposal)
    
    Returns None if no suitable document found.
    """
    if not subdocuments:
        return None
    
    # Group documents by type
    doc_by_type = {}
    for doc in subdocuments:
        if doc.document_type not in doc_by_type:
            doc_by_type[doc.document_type] = []
        doc_by_type[doc.document_type].append(doc)
    
    # Check for floating amendments (amendments without Verslag)
    has_amendments = DocumentType.AMENDEMENT in doc_by_type
    has_verslag = DocumentType.VERSLAG in doc_by_type
    has_aangenomen_tekst = DocumentType.AANGENOMEN_TEKST in doc_by_type
    
    floating_amendments = has_amendments and not has_verslag and not has_aangenomen_tekst
    amendments_count = len(doc_by_type.get(DocumentType.AMENDEMENT, []))
    
    # Priority 1: AangenomenTekst (definitive law)
    if DocumentType.AANGENOMEN_TEKST in doc_by_type:
        # Take the most recent one if multiple exist
        docs = sorted(doc_by_type[DocumentType.AANGENOMEN_TEKST], 
                     key=lambda d: d.date, reverse=True)
        return CoreDocumentSelection(
            document=docs[0],
            selection_reason="Dit is de definitieve, aangenomen wet.",
            has_floating_amendments=False,
            floating_amendments_count=0
        )
    
    # Priority 2: Special documents
    special_types = [
        DocumentType.REGEERAKKOORD,
        DocumentType.BEGROTING,
        DocumentType.BELEIDSVERKLARING,
    ]
    
    for doc_type in special_types:
        if doc_type in doc_by_type:
            docs = sorted(doc_by_type[doc_type], 
                         key=lambda d: d.date, reverse=True)
            type_name = doc_type.value
            return CoreDocumentSelection(
                document=docs[0],
                selection_reason=f"Dit is een {type_name.lower()}.",
                has_floating_amendments=floating_amendments,
                floating_amendments_count=amendments_count if floating_amendments else 0
            )
    
    # Priority 3: WetsOntwerp, WetsVoorstel, VoorstelTotHerziening (proposals)
    proposal_types = [
        DocumentType.WETS_ONTWERP,
        DocumentType.WETS_VOORSTEL,
        DocumentType.VOORSTEL_TOT_HERZIENING,
    ]
    
    proposal_docs = []
    for doc_type in proposal_types:
        if doc_type in doc_by_type:
            proposal_docs.extend(doc_by_type[doc_type])
    
    if proposal_docs:
        # Sort all proposals by date and take the most recent
        docs = sorted(proposal_docs, key=lambda d: d.date, reverse=True)
        selected_doc = docs[0]
        
        # Generate reason based on document type
        type_reasons = {
            DocumentType.WETS_ONTWERP: "Dit is een wetsontwerp van de regering.",
            DocumentType.WETS_VOORSTEL: "Dit is een initiatief van een parlementslid.",
            DocumentType.VOORSTEL_TOT_HERZIENING: "Dit is een voorstel tot herziening.",
        }
        reason = type_reasons.get(selected_doc.document_type, "Dit is een voorstel.")
        if floating_amendments:
            reason += f" Er zijn {amendments_count} amendementen ingediend die nog niet zijn behandeld in een verslag."
        
        return CoreDocumentSelection(
            document=selected_doc,
            selection_reason=reason,
            has_floating_amendments=floating_amendments,
            floating_amendments_count=amendments_count if floating_amendments else 0
        )
    
    # Priority 4: VoorstelVanResolutie
    if DocumentType.VOORSTEL_VAN_RESOLUTIE in doc_by_type:
        docs = sorted(doc_by_type[DocumentType.VOORSTEL_VAN_RESOLUTIE], 
                     key=lambda d: d.date, reverse=True)
        return CoreDocumentSelection(
            document=docs[0],
            selection_reason="Dit is een voorstel van resolutie.",
            has_floating_amendments=floating_amendments,
            floating_amendments_count=amendments_count if floating_amendments else 0
        )
    
    # No suitable document found
    return None


def get_debate_documents(subdocuments: List[Subdocument]) -> List[Subdocument]:
    """
    Returns all Verslag documents (reports/debates) for political analysis.
    """
    return [doc for doc in subdocuments if doc.document_type == DocumentType.VERSLAG]


def get_advice_documents(subdocuments: List[Subdocument]) -> List[Subdocument]:
    """
    Returns advice documents (Raad van State, etc.) for context.
    """
    advice_types = [
        DocumentType.ADVIES_VAN_DE_RAAD_VAN_STATE,
        DocumentType.ADVIES,
    ]
    return [doc for doc in subdocuments if doc.document_type in advice_types]

