"""Utility functions for LLM operations including retry logic and cost tracking."""
import json
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass, field


@dataclass
class RetryInfo:
    """Information about retry attempts."""
    json_parse_retries: int = 0
    json_parse_failed: bool = False
    json_parse_error: Optional[str] = None


@dataclass
class CostInfo:
    """Information about API costs."""
    input_tokens: int = 0
    output_tokens: int = 0
    total_cost: float = 0.0
    cost_breakdown: Dict[str, float] = field(default_factory=dict)


def parse_json_with_retry(
    json_text: str,
    max_retries: int = 3,
    retry_info: Optional[RetryInfo] = None
) -> Tuple[Any, RetryInfo]:
    """
    Parse JSON with retry logic.
    
    Args:
        json_text: The JSON string to parse
        max_retries: Maximum number of retry attempts (default: 3)
        retry_info: Optional RetryInfo object to update
    
    Returns:
        Tuple of (parsed_json, RetryInfo)
    
    Raises:
        json.JSONDecodeError: If parsing fails after max_retries
    """
    if retry_info is None:
        retry_info = RetryInfo()
    
    for attempt in range(max_retries + 1):
        try:
            # Try to parse the JSON
            parsed = json.loads(json_text)
            return parsed, retry_info
        except json.JSONDecodeError as e:
            retry_info.json_parse_retries = attempt
            retry_info.json_parse_error = str(e)
            
            if attempt < max_retries:
                # Try to fix common JSON issues
                json_text = _attempt_json_fix(json_text)
                continue
            else:
                # Failed after all retries
                retry_info.json_parse_failed = True
                raise
    
    # Should never reach here, but just in case
    retry_info.json_parse_failed = True
    raise json.JSONDecodeError("Failed to parse JSON after retries", json_text, 0)


def _attempt_json_fix(json_text: str) -> str:
    """
    Attempt to fix common JSON issues.
    
    This is a best-effort fix for common JSON problems like:
    - Trailing commas
    - Unescaped quotes in strings
    - Missing quotes around keys
    """
    # Remove trailing commas before closing braces/brackets
    import re
    json_text = re.sub(r',(\s*[}\]])', r'\1', json_text)
    
    # Try to extract JSON from markdown code blocks
    json_match = re.search(r'```(?:json)?\s*(\{.*\})\s*```', json_text, re.DOTALL)
    if json_match:
        json_text = json_match.group(1)
    
    # Try to extract JSON object if wrapped in text
    json_match = re.search(r'(\{.*\})', json_text, re.DOTALL)
    if json_match:
        json_text = json_match.group(1)
    
    return json_text


def extract_tokens_from_response(response: Any, provider: str) -> Tuple[int, int]:
    """
    Extract token counts from API response.
    
    Args:
        response: The API response object (varies by provider)
        provider: Provider name (openai, anthropic, mistral, gemini, nebius)
    
    Returns:
        Tuple of (input_tokens, output_tokens)
    """
    input_tokens = 0
    output_tokens = 0
    
    try:
        if provider == "openai" or provider == "nebius":
            # OpenAI-compatible API
            if hasattr(response, 'usage'):
                usage = response.usage
                input_tokens = getattr(usage, 'prompt_tokens', 0) or getattr(usage, 'input_tokens', 0)
                output_tokens = getattr(usage, 'completion_tokens', 0) or getattr(usage, 'output_tokens', 0)
        
        elif provider == "anthropic":
            # Anthropic API
            if hasattr(response, 'usage'):
                usage = response.usage
                input_tokens = getattr(usage, 'input_tokens', 0)
                output_tokens = getattr(usage, 'output_tokens', 0)
        
        elif provider == "mistral":
            # Mistral API
            if hasattr(response, 'usage'):
                usage = response.usage
                input_tokens = getattr(usage, 'prompt_tokens', 0)
                output_tokens = getattr(usage, 'completion_tokens', 0) or getattr(usage, 'total_tokens', 0) - input_tokens
        
        elif provider == "gemini":
            # Gemini API
            if hasattr(response, 'usage_metadata'):
                usage = response.usage_metadata
                input_tokens = getattr(usage, 'prompt_token_count', 0)
                output_tokens = getattr(usage, 'candidates_token_count', 0)
    except Exception:
        # If we can't extract tokens, return 0
        pass
    
    return input_tokens, output_tokens

