"""Central configuration for all models to test."""
from dataclasses import dataclass
from typing import Optional
from .llm import (
    OpenAILLMClient,
    AnthropicLLMClient,
    MistralLLMClient,
    GeminiLLMClient,
    NebiusLLMClient,
    LLMClient,
)


@dataclass
class ModelConfig:
    """Configuration for a single model to test."""
    name: str  # Display name for the model
    provider: str  # Provider name (openai, anthropic, mistral, gemini, nebius)
    model: str  # Model identifier
    client_class: type[LLMClient]  # Client class to instantiate
    api_key_env: Optional[str] = None  # Environment variable name for API key (if different from default)


# Define all models you want to test here
MODELS_TO_TEST = [
    ModelConfig(
        name="Claude Sonnet 4.5",
        provider="anthropic",
        model="claude-sonnet-4-5-20250929",
        client_class=AnthropicLLMClient,
    ),
    ModelConfig(
        name="Claude Haiku 4.5",
        provider="anthropic",
        model="claude-haiku-4-5-20251001",
        client_class=AnthropicLLMClient,
    ),
    ModelConfig(
        name="Gemini 3 Pro Preview",
        provider="gemini",
        model="gemini-3-pro-preview",
        client_class=GeminiLLMClient,
    ),
    ModelConfig(
        name="Gemini 3 Flash Preview",
        provider="gemini",
        model="gemini-3-flash-preview",
        client_class=GeminiLLMClient,
    ),
    ModelConfig(
        name="Mistral Large 2512",
        provider="mistral",
        model="mistral-large-2512",
        client_class=MistralLLMClient,
    ),
    ModelConfig(
        name="Mistral Medium 2508",
        provider="mistral",
        model="mistral-medium-2508",
        client_class=MistralLLMClient,
    ),
    # ModelConfig(
    #     name="Kimi K2 Thinking",
    #     provider="nebius",
    #     model="moonshotai/Kimi-K2-Thinking",
    #     client_class=NebiusLLMClient,
    # ),
    # Add more models here as needed
    # Example:
    # ModelConfig(
    #     name="GPT-4o Mini",
    #     provider="openai",
    #     model="gpt-4o-mini",
    #     client_class=OpenAILLMClient,
    # ),
]


def get_client_for_config(config: ModelConfig) -> LLMClient:
    """Create an LLM client instance from a model configuration."""
    return config.client_class(model=config.model)

