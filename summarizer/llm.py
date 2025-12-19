"""LLM interaction layer for summarization pipeline."""
import os
from typing import List, Optional, Dict
from .models import (
    Subdocument,
    FactSummary,
    PoliticalAnalysis,
)
from . import prompts
from .llm_utils import (
    parse_json_with_retry,
    RetryInfo,
    CostInfo,
    extract_tokens_from_response,
)


class LLMClient:
    """Abstract base class for LLM clients."""
    
    def __init__(self):
        """Initialize tracking for retries and costs."""
        self._retry_info: Dict[str, RetryInfo] = {}
        self._cost_info: Dict[str, CostInfo] = {}
    
    def get_retry_info(self, dossier_id: str) -> Optional[RetryInfo]:
        """Get retry information for a dossier."""
        return self._retry_info.get(dossier_id)
    
    def get_cost_info(self, dossier_id: str) -> Optional[CostInfo]:
        """Get cost information for a dossier."""
        return self._cost_info.get(dossier_id)
    
    def summarize_facts(self, document: Subdocument, context: str) -> FactSummary:
        """Step 1: Generate objective fact-based summary."""
        raise NotImplementedError
    
    def analyze_politics(
        self,
        fact_summary: FactSummary,
        debate_documents: List[Subdocument],
        advice_documents: List[Subdocument],
        core_document: Optional[Subdocument] = None,
    ) -> PoliticalAnalysis:
        """Step 2: Analyze political context and debate."""
        raise NotImplementedError
    
    def generate_title(self, original_title: str, fact_summary: FactSummary) -> str:
        """Generate a clear, non-legalese title for the dossier."""
        raise NotImplementedError


class MockLLMClient(LLMClient):
    """Mock LLM client for testing without API calls."""
    
    def __init__(self):
        super().__init__()
    
    def summarize_facts(self, document: Subdocument, context: str) -> FactSummary:
        """Mock fact summary."""
        return FactSummary(
            summary=f"[MOCK] Objectieve samenvatting van {document.document_type.value} document {document.id}.",
            document_type=document.document_type.value,
            selection_reason=context,
        )
    
    def analyze_politics(
        self,
        fact_summary: FactSummary,
        debate_documents: List[Subdocument],
        advice_documents: List[Subdocument],
        core_document: Optional[Subdocument] = None,
        dossier_id: Optional[str] = None,
    ) -> PoliticalAnalysis:
        """Mock political analysis."""
        has_debate = len(debate_documents) > 0
        
        if has_debate:
            return PoliticalAnalysis(
                arguments_for=[
                    {"party": "Vooruit", "argument": "[MOCK] Argument voor: dit voorstel verbetert..."},
                ],
                arguments_against=[
                    {"party": "N-VA", "argument": "[MOCK] Argument tegen: dit kost te veel..."},
                ],
                neutral_technical=[
                    {"source": "Raad van State", "argument": "[MOCK] Technische opmerking: mogelijke schending..."},
                ],
                summary_debate="[MOCK] Samenvatting van het debat...",
                has_debate=True,
            )
        else:
            # No debate, only summarize - no pro/con arguments
            return PoliticalAnalysis(
                arguments_for=[],
                arguments_against=[],
                neutral_technical=[],
                summary_debate=None,
                notable_changes="[MOCK] Samenvatting van het dossier op basis van de beschikbare documenten.",
                has_debate=False,
            )
    
    def generate_title(self, original_title: str, fact_summary: FactSummary) -> str:
        """Mock title generation."""
        return f"[MOCK] Duidelijke titel voor: {original_title[:50]}..."


class OpenAILLMClient(LLMClient):
    """OpenAI API client."""
    
    def __init__(self, model: str = "gpt-4o-mini", api_key: Optional[str] = None):
        super().__init__()
        try:
            from openai import OpenAI
        except ImportError:
            raise ImportError("openai package not installed. Install with: pip install openai")
        
        self.client = OpenAI(api_key=api_key or os.getenv("OPENAI_API_KEY"))
        self.model = model
        self.provider = "openai"
    
    def summarize_facts(self, document: Subdocument, context: str) -> FactSummary:
        """Generate objective fact-based summary using OpenAI."""
        if not document.content:
            raise ValueError(f"Document {document.id} has no content loaded")
        
        prompt = prompts.get_fact_summary_prompt(document, context)
        
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": prompts.get_system_prompt_fact_summary()},
                {"role": "user", "content": prompt},
            ],
            temperature=0.3,
        )
        
        summary = response.choices[0].message.content
        
        return FactSummary(
            summary=summary,
            document_type=document.document_type.value,
            selection_reason=context,
        )
    
    def analyze_politics(
        self,
        fact_summary: FactSummary,
        debate_documents: List[Subdocument],
        advice_documents: List[Subdocument],
        core_document: Optional[Subdocument] = None,
        dossier_id: Optional[str] = None,
    ) -> PoliticalAnalysis:
        """Analyze political context using OpenAI."""
        has_debate = len(debate_documents) > 0
        
        if not has_debate:
            # No debate available, return empty arguments
            return PoliticalAnalysis(
                arguments_for=[],
                arguments_against=[],
                neutral_technical=[],
                summary_debate=None,
                notable_changes=None,
                has_debate=False,
            )
        
        prompt = prompts.get_political_analysis_prompt_with_debate(
            fact_summary, debate_documents, advice_documents
        )
        
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": prompts.get_system_prompt_political_analysis()},
                {"role": "user", "content": prompt},
            ],
            temperature=0.5,
            response_format={"type": "json_object"},
        )
        
        # Extract tokens and track cost
        input_tokens, output_tokens = extract_tokens_from_response(response, self.provider)
        key = dossier_id or fact_summary.summary[:50]
        cost_info = CostInfo(input_tokens=input_tokens, output_tokens=output_tokens)
        self._cost_info[key] = cost_info
        
        # Parse JSON with retry logic
        import json
        json_text = response.choices[0].message.content
        retry_info = RetryInfo()
        try:
            result, retry_info = parse_json_with_retry(json_text, max_retries=3, retry_info=retry_info)
            self._retry_info[key] = retry_info
        except json.JSONDecodeError as e:
            retry_info.json_parse_failed = True
            retry_info.json_parse_error = str(e)
            self._retry_info[key] = retry_info
            # Return empty analysis if JSON parsing fails after retries
            return PoliticalAnalysis(
                arguments_for=[],
                arguments_against=[],
                neutral_technical=[],
                summary_debate=None,
                notable_changes=None,
                has_debate=has_debate,
            )
        
        return PoliticalAnalysis(
            arguments_for=result.get("arguments_for", []),
            arguments_against=result.get("arguments_against", []),
            neutral_technical=result.get("neutral_technical", []),
            summary_debate=result.get("summary_debate"),
            notable_changes=result.get("notable_changes"),
            has_debate=has_debate,
        )
    
    def generate_title(self, original_title: str, fact_summary: FactSummary) -> str:
        """Generate a clear title using OpenAI."""
        prompt = prompts.get_title_generation_prompt(original_title, fact_summary)
        
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": "Je bent een communicatiespecialist die duidelijke, begrijpelijke titels schrijft."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.5,
        )
        
        return response.choices[0].message.content.strip()


class AnthropicLLMClient(LLMClient):
    """Anthropic Claude API client."""
    
    def __init__(self, model: str = "claude-3-5-sonnet-20241022", api_key: Optional[str] = None):
        super().__init__()
        try:
            from anthropic import Anthropic
        except ImportError:
            raise ImportError("anthropic package not installed. Install with: pip install anthropic")
        
        self.client = Anthropic(api_key=api_key or os.getenv("ANTHROPIC_API_KEY"))
        self.model = model
        self.provider = "anthropic"
    
    def summarize_facts(self, document: Subdocument, context: str) -> FactSummary:
        """Generate objective fact-based summary using Anthropic."""
        if not document.content:
            raise ValueError(f"Document {document.id} has no content loaded")
        
        prompt = prompts.get_fact_summary_prompt(document, context)
        
        message = self.client.messages.create(
            model=self.model,
            max_tokens=2000,
            messages=[
                {"role": "user", "content": prompt},
            ],
        )
        
        summary = message.content[0].text
        
        return FactSummary(
            summary=summary,
            document_type=document.document_type.value,
            selection_reason=context,
        )
    
    def analyze_politics(
        self,
        fact_summary: FactSummary,
        debate_documents: List[Subdocument],
        advice_documents: List[Subdocument],
        core_document: Optional[Subdocument] = None,
        dossier_id: Optional[str] = None,
    ) -> PoliticalAnalysis:
        """Analyze political context using Anthropic."""
        has_debate = len(debate_documents) > 0
        
        if not has_debate:
            # No debate available, return empty arguments
            return PoliticalAnalysis(
                arguments_for=[],
                arguments_against=[],
                neutral_technical=[],
                summary_debate=None,
                notable_changes=None,
                has_debate=False,
            )
        
        prompt = prompts.get_political_analysis_prompt_with_debate(
            fact_summary, debate_documents, advice_documents
        )
        
        message = self.client.messages.create(
            model=self.model,
            max_tokens=2000,
            messages=[
                {"role": "user", "content": prompt},
            ],
        )
        
        # Extract tokens and track cost
        input_tokens, output_tokens = extract_tokens_from_response(message, self.provider)
        key = dossier_id or fact_summary.summary[:50]
        cost_info = CostInfo(input_tokens=input_tokens, output_tokens=output_tokens)
        self._cost_info[key] = cost_info
        
        # Parse JSON with retry logic
        import json
        json_text = message.content[0].text
        retry_info = RetryInfo()
        try:
            result, retry_info = parse_json_with_retry(json_text, max_retries=3, retry_info=retry_info)
            self._retry_info[key] = retry_info
        except json.JSONDecodeError as e:
            retry_info.json_parse_failed = True
            retry_info.json_parse_error = str(e)
            self._retry_info[key] = retry_info
            # Return empty analysis if JSON parsing fails after retries
            return PoliticalAnalysis(
                arguments_for=[],
                arguments_against=[],
                neutral_technical=[],
                summary_debate=None,
                notable_changes=None,
                has_debate=has_debate,
            )
        
        return PoliticalAnalysis(
            arguments_for=result.get("arguments_for", []),
            arguments_against=result.get("arguments_against", []),
            neutral_technical=result.get("neutral_technical", []),
            summary_debate=result.get("summary_debate"),
            notable_changes=result.get("notable_changes"),
            has_debate=has_debate,
        )
    
    def generate_title(self, original_title: str, fact_summary: FactSummary) -> str:
        """Generate a clear title using Anthropic."""
        prompt = prompts.get_title_generation_prompt(original_title, fact_summary)
        
        message = self.client.messages.create(
            model=self.model,
            max_tokens=200,
            messages=[
                {"role": "user", "content": prompt},
            ],
        )
        
        return message.content[0].text.strip()


class MistralLLMClient(LLMClient):
    """Mistral AI API client."""
    
    def __init__(self, model: str = "mistral-large-2512", api_key: Optional[str] = None):
        super().__init__()
        try:
            from mistralai import Mistral
        except ImportError:
            raise ImportError("mistralai package not installed. Install with: pip install mistralai")
        
        self.client = Mistral(api_key=api_key or os.getenv("MISTRAL_API_TOKEN"))
        self.model = model
        self.provider = "mistral"
    
    def summarize_facts(self, document: Subdocument, context: str) -> FactSummary:
        """Generate objective fact-based summary using Mistral."""
        if not document.content:
            raise ValueError(f"Document {document.id} has no content loaded")
        
        prompt = prompts.get_fact_summary_prompt(document, context)
        
        response = self.client.chat.complete(
            model=self.model,
            messages=[
                {"role": "system", "content": prompts.get_system_prompt_fact_summary()},
                {"role": "user", "content": prompt},
            ],
            temperature=0.3,
        )
        
        summary = response.choices[0].message.content
        
        return FactSummary(
            summary=summary,
            document_type=document.document_type.value,
            selection_reason=context,
        )
    
    def analyze_politics(
        self,
        fact_summary: FactSummary,
        debate_documents: List[Subdocument],
        advice_documents: List[Subdocument],
        core_document: Optional[Subdocument] = None,
        dossier_id: Optional[str] = None,
    ) -> PoliticalAnalysis:
        """Analyze political context using Mistral."""
        has_debate = len(debate_documents) > 0
        
        if not has_debate:
            # No debate available, return empty arguments
            return PoliticalAnalysis(
                arguments_for=[],
                arguments_against=[],
                neutral_technical=[],
                summary_debate=None,
                notable_changes=None,
                has_debate=False,
            )
        
        prompt = prompts.get_political_analysis_prompt_with_debate(
            fact_summary, debate_documents, advice_documents
        )
        
        response = self.client.chat.complete(
            model=self.model,
            messages=[
                {"role": "system", "content": prompts.get_system_prompt_political_analysis()},
                {"role": "user", "content": prompt},
            ],
            temperature=0.5,
            response_format={"type": "json_object"},
        )
        
        # Extract tokens and track cost
        input_tokens, output_tokens = extract_tokens_from_response(response, self.provider)
        key = dossier_id or fact_summary.summary[:50]
        cost_info = CostInfo(input_tokens=input_tokens, output_tokens=output_tokens)
        self._cost_info[key] = cost_info
        
        # Parse JSON with retry logic
        import json
        json_text = response.choices[0].message.content
        retry_info = RetryInfo()
        try:
            result, retry_info = parse_json_with_retry(json_text, max_retries=3, retry_info=retry_info)
            self._retry_info[key] = retry_info
        except json.JSONDecodeError as e:
            retry_info.json_parse_failed = True
            retry_info.json_parse_error = str(e)
            self._retry_info[key] = retry_info
            # Return empty analysis if JSON parsing fails after retries
            return PoliticalAnalysis(
                arguments_for=[],
                arguments_against=[],
                neutral_technical=[],
                summary_debate=None,
                notable_changes=None,
                has_debate=has_debate,
            )
        
        return PoliticalAnalysis(
            arguments_for=result.get("arguments_for", []),
            arguments_against=result.get("arguments_against", []),
            neutral_technical=result.get("neutral_technical", []),
            summary_debate=result.get("summary_debate"),
            notable_changes=result.get("notable_changes"),
            has_debate=has_debate,
        )
    
    def generate_title(self, original_title: str, fact_summary: FactSummary) -> str:
        """Generate a clear title using Mistral."""
        prompt = prompts.get_title_generation_prompt(original_title, fact_summary)
        
        response = self.client.chat.complete(
            model=self.model,
            messages=[
                {"role": "system", "content": "Je bent een communicatiespecialist die duidelijke, begrijpelijke titels schrijft."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.5,
        )
        
        return response.choices[0].message.content.strip()


class GeminiLLMClient(LLMClient):
    """Google Gemini API client using Vertex AI."""
    
    def __init__(self, model: str = "gemini-3-pro-preview", api_key: Optional[str] = None):
        super().__init__()
        try:
            from google import genai
            from google.genai import types
        except ImportError:
            raise ImportError("google-genai package not installed. Install with: pip install google-genai")
        
        self.genai = genai
        self.types = types
        api_key = api_key or os.getenv("GOOGLE_CLOUD_API_KEY")
        if not api_key:
            raise ValueError("GOOGLE_CLOUD_API_KEY environment variable or api_key parameter required")
        
        self.client = genai.Client(
            vertexai=True,
            api_key=api_key,
        )
        self.model = model
        self.provider = "gemini"
    
    def summarize_facts(self, document: Subdocument, context: str) -> FactSummary:
        """Generate objective fact-based summary using Gemini."""
        if not document.content:
            raise ValueError(f"Document {document.id} has no content loaded")
        
        prompt = prompts.get_fact_summary_prompt(document, context)
        system_prompt = prompts.get_system_prompt_fact_summary()
        
        contents = [
            self.types.Content(
                role="user",
                parts=[self.types.Part.from_text(text=prompt)]
            )
        ]
        
        generate_content_config = self.types.GenerateContentConfig(
            temperature=0.3,
            system_instruction=[self.types.Part.from_text(text=system_prompt)],
        )
        
        response = self.client.models.generate_content(
            model=self.model,
            contents=contents,
            config=generate_content_config,
        )
        
        summary = response.text
        
        return FactSummary(
            summary=summary,
            document_type=document.document_type.value,
            selection_reason=context,
        )
    
    def analyze_politics(
        self,
        fact_summary: FactSummary,
        debate_documents: List[Subdocument],
        advice_documents: List[Subdocument],
        core_document: Optional[Subdocument] = None,
        dossier_id: Optional[str] = None,
    ) -> PoliticalAnalysis:
        """Analyze political context using Gemini."""
        has_debate = len(debate_documents) > 0
        
        if not has_debate:
            # No debate available, return empty arguments
            return PoliticalAnalysis(
                arguments_for=[],
                arguments_against=[],
                neutral_technical=[],
                summary_debate=None,
                notable_changes=None,
                has_debate=False,
            )
        
        prompt = prompts.get_political_analysis_prompt_with_debate(
            fact_summary, debate_documents, advice_documents
        )
        
        system_prompt = prompts.get_system_prompt_political_analysis()
        
        contents = [
            self.types.Content(
                role="user",
                parts=[self.types.Part.from_text(text=prompt)]
            )
        ]
        
        generate_content_config = self.types.GenerateContentConfig(
            temperature=0.5,
            system_instruction=[self.types.Part.from_text(text=system_prompt)],
            response_mime_type="application/json",
        )
        
        response = self.client.models.generate_content(
            model=self.model,
            contents=contents,
            config=generate_content_config,
        )
        
        # Extract tokens and track cost
        input_tokens, output_tokens = extract_tokens_from_response(response, self.provider)
        key = dossier_id or fact_summary.summary[:50]
        cost_info = CostInfo(input_tokens=input_tokens, output_tokens=output_tokens)
        self._cost_info[key] = cost_info
        
        # Parse JSON with retry logic
        import json
        json_text = response.text
        retry_info = RetryInfo()
        try:
            result, retry_info = parse_json_with_retry(json_text, max_retries=3, retry_info=retry_info)
            self._retry_info[key] = retry_info
        except json.JSONDecodeError as e:
            retry_info.json_parse_failed = True
            retry_info.json_parse_error = str(e)
            self._retry_info[key] = retry_info
            # Return empty analysis if JSON parsing fails after retries
            return PoliticalAnalysis(
                arguments_for=[],
                arguments_against=[],
                neutral_technical=[],
                summary_debate=None,
                notable_changes=None,
                has_debate=has_debate,
            )
        
        return PoliticalAnalysis(
            arguments_for=result.get("arguments_for", []),
            arguments_against=result.get("arguments_against", []),
            neutral_technical=result.get("neutral_technical", []),
            summary_debate=result.get("summary_debate"),
            notable_changes=result.get("notable_changes"),
            has_debate=has_debate,
        )
    
    def generate_title(self, original_title: str, fact_summary: FactSummary) -> str:
        """Generate a clear title using Gemini."""
        prompt = prompts.get_title_generation_prompt(original_title, fact_summary)
        system_prompt = "Je bent een communicatiespecialist die duidelijke, begrijpelijke titels schrijft."
        
        contents = [
            self.types.Content(
                role="user",
                parts=[self.types.Part.from_text(text=prompt)]
            )
        ]
        
        generate_content_config = self.types.GenerateContentConfig(
            temperature=0.5,
            system_instruction=[self.types.Part.from_text(text=system_prompt)],
        )
        
        response = self.client.models.generate_content(
            model=self.model,
            contents=contents,
            config=generate_content_config,
        )
        
        return response.text.strip()


class NebiusLLMClient(LLMClient):
    """Nebius API client (OpenAI-compatible)."""
    
    def __init__(self, model: str = "moonshotai/Kimi-K2-Thinking", api_key: Optional[str] = None, base_url: Optional[str] = None):
        super().__init__()
        try:
            from openai import OpenAI
        except ImportError:
            raise ImportError("openai package not installed. Install with: pip install openai")
        
        api_key = api_key or os.getenv("NEBIUS_API_KEY")
        if not api_key:
            raise ValueError("NEBIUS_API_KEY environment variable or api_key parameter required")
        
        # Nebius uses OpenAI-compatible API
        self.client = OpenAI(
            base_url=base_url or "https://api.studio.nebius.com/v1/",
            api_key=api_key
        )
        self.model = model
        self.provider = "nebius"
    
    def summarize_facts(self, document: Subdocument, context: str) -> FactSummary:
        """Generate objective fact-based summary using Nebius."""
        if not document.content:
            raise ValueError(f"Document {document.id} has no content loaded")
        
        prompt = prompts.get_fact_summary_prompt(document, context)
        
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": prompts.get_system_prompt_fact_summary()},
                {"role": "user", "content": prompt},
            ],
            temperature=0.3,
        )
        
        summary = response.choices[0].message.content
        
        return FactSummary(
            summary=summary,
            document_type=document.document_type.value,
            selection_reason=context,
        )
    
    def analyze_politics(
        self,
        fact_summary: FactSummary,
        debate_documents: List[Subdocument],
        advice_documents: List[Subdocument],
        core_document: Optional[Subdocument] = None,
        dossier_id: Optional[str] = None,
    ) -> PoliticalAnalysis:
        """Analyze political context using Nebius."""
        has_debate = len(debate_documents) > 0
        
        if not has_debate:
            # No debate available, return empty arguments
            return PoliticalAnalysis(
                arguments_for=[],
                arguments_against=[],
                neutral_technical=[],
                summary_debate=None,
                notable_changes=None,
                has_debate=False,
            )
        
        prompt = prompts.get_political_analysis_prompt_with_debate(
            fact_summary, debate_documents, advice_documents
        )
        
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": prompts.get_system_prompt_political_analysis()},
                {"role": "user", "content": prompt},
            ],
            temperature=0.5,
            response_format={"type": "json_object"},
        )
        
        # Extract tokens and track cost
        input_tokens, output_tokens = extract_tokens_from_response(response, self.provider)
        key = dossier_id or fact_summary.summary[:50]
        cost_info = CostInfo(input_tokens=input_tokens, output_tokens=output_tokens)
        self._cost_info[key] = cost_info
        
        # Parse JSON with retry logic
        import json
        json_text = response.choices[0].message.content
        retry_info = RetryInfo()
        try:
            result, retry_info = parse_json_with_retry(json_text, max_retries=3, retry_info=retry_info)
            self._retry_info[key] = retry_info
        except json.JSONDecodeError as e:
            retry_info.json_parse_failed = True
            retry_info.json_parse_error = str(e)
            self._retry_info[key] = retry_info
            # Return empty analysis if JSON parsing fails after retries
            return PoliticalAnalysis(
                arguments_for=[],
                arguments_against=[],
                neutral_technical=[],
                summary_debate=None,
                notable_changes=None,
                has_debate=has_debate,
            )
        
        return PoliticalAnalysis(
            arguments_for=result.get("arguments_for", []),
            arguments_against=result.get("arguments_against", []),
            neutral_technical=result.get("neutral_technical", []),
            summary_debate=result.get("summary_debate"),
            notable_changes=result.get("notable_changes"),
            has_debate=has_debate,
        )
    
    def generate_title(self, original_title: str, fact_summary: FactSummary) -> str:
        """Generate a clear title using Nebius."""
        prompt = prompts.get_title_generation_prompt(original_title, fact_summary)
        
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": "Je bent een communicatiespecialist die duidelijke, begrijpelijke titels schrijft."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.5,
        )
        
        return response.choices[0].message.content.strip()
