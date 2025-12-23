"""Data models for parliamentary documents and dossiers."""
from dataclasses import dataclass
from typing import List, Optional
from enum import Enum


class DocumentType(str, Enum):
    """Document types in the Belgian parliament."""
    AANGENOMEN_TEKST = "AangenomenTekst"
    AMENDEMENT = "Amendement"
    ADVIES = "Advies"
    ADVIES_VAN_DE_RAAD_VAN_STATE = "AdviesVanDeRaadVanState"
    VERSLAG = "Verslag"
    WETS_ONTWERP = "WetsOntwerp"
    OVERGEZONDEN_ONTWERP = "OvergezondenOntwerp"
    WETS_VOORSTEL = "WetsVoorstel"
    VOORSTEL_VAN_RESOLUTIE = "VoorstelVanResolutie"
    VOORSTEL_TOT_HERZIENING = "VoorstelTotHerziening"
    VOORSTEL_ONDERZOEKSCOMMISSIE = "VoorstelOnderzoekscommissie"
    VOORSTEL_REGLEMENT = "VoorstelReglement"
    ARTIKELEN_BIJ_EERSTE_STEMMING_AANGENOMEN = "ArtikelenBijEersteStemmingAangenomen"
    TABELLEN_OF_LIJSTEN = "TabellenOfLijsten"
    BELEIDSNOTA = "Beleidsnota"
    ARTIKELEN_AANGENOMEN_IN_PLENUM = "ArtikelenAangenomenInPlenum"
    KAFT = "Kaft"
    REGEERAKKOORD = "Regeerakkoord"
    CORRIGENDUM = "Corrigendum"
    BIJLAGE = "Bijlage"
    VOORSTEL_VAN_VERKLARING = "VoorstelVanVerklaring"
    BESLISSING_OVERLEGCOMMISSIE = "BeslissingOverlegcommissie"
    BEGROTING = "Begroting"
    VOORDRACHT_VAN_KANDIDATEN = "VoordrachtVanKandidaten"
    LIJST_VAN_VERZOEKSCHRIFTEN = "LijstVanVerzoekschriften"
    BELEIDSVERKLARING = "Beleidsverklaring"
    VERANTWOORDING = "Verantwoording"
    NIET_GEEVOCEERD_ONTWERP = "NietGeevoceerdOntwerp"
    ERRATA = "Errata"
    OPMERKINGEN_VAN_HET_REKENHOF = "OpmerkingenVanHetRekenhof"
    UNKNOWN = "Unknown"


@dataclass
class Subdocument:
    """A subdocument within a dossier."""
    dossier_id: str
    id: str
    document_type: DocumentType
    date: str
    authors: List[str]
    content: Optional[str] = None  # Markdown content loaded from file


@dataclass
class Dossier:
    """A parliamentary dossier with metadata and subdocuments."""
    dossier_id: str
    title: str
    authors: List[str]
    submission_date: str
    end_date: Optional[str]
    vote_date: Optional[str]
    document_type: str
    status: str
    subdocuments: List[Subdocument]


@dataclass
class CoreDocumentSelection:
    """Result of selecting the core document for summarization."""
    document: Subdocument
    selection_reason: str
    has_floating_amendments: bool = False
    floating_amendments_count: int = 0


@dataclass
class FactSummary:
    """Output from Step 1: Objective fact-based summary."""
    summary: str
    document_type: str
    selection_reason: str


@dataclass
class PoliticalAnalysis:
    """Output from Step 2: Political context and debate analysis."""
    arguments: Optional[List[dict]] = None  # New format: [{"party": "...", "arguments": "..."}]
    arguments_for: List[dict] = None  # Old format: [{"party": "...", "argument": "..."}]
    arguments_against: List[dict] = None
    neutral_technical: List[dict] = None  # [{"source": "...", "argument": "..."}]
    summary_debate: Optional[str] = None  # Summary of the debate
    notable_changes: Optional[str] = None
    has_debate: bool = False
    
    def __post_init__(self):
        """Initialize default values for backward compatibility."""
        if self.arguments_for is None:
            self.arguments_for = []
        if self.arguments_against is None:
            self.arguments_against = []
        if self.neutral_technical is None:
            self.neutral_technical = []


@dataclass
class DossierSummary:
    """Complete summary of a dossier."""
    dossier_id: str
    title: str
    generated_title: str  # LLM-generated title without legalese
    fact_summary: FactSummary
    political_analysis: PoliticalAnalysis

