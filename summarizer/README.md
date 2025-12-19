# Summarization Pipeline

A 2-step LLM pipeline for summarizing Belgian parliamentary dossiers.

## Architecture

1. **Step 1: Fact Summary** - Selects core document and generates objective summary
2. **Step 2: Political Analysis** - Analyzes debate/positions using Step 1 output as context

## Installation

```bash
# Install dependencies
pip install -r requirements.txt

# For parquet reading, you need either:
# Option 1: duckdb (recommended)
pip install duckdb

# Option 2: pandas + pyarrow
pip install pandas pyarrow
```

## Usage

### Mock Mode (No API calls)

```bash
python -m summarizer.run
```

### With OpenAI API

```bash
export OPENAI_API_KEY=your_key_here
export LLM_MODE=openai
python -m summarizer.run
```

### With Anthropic API

```bash
export ANTHROPIC_API_KEY=your_key_here
export LLM_MODE=anthropic
python -m summarizer.run
```

### With Mistral AI API

```bash
export MISTRAL_API_KEY=your_key_here
export LLM_MODE=mistral
python -m summarizer.run
```

### With Google Gemini API

```bash
export GEMINI_API_KEY=your_key_here
export LLM_MODE=gemini
python -m summarizer.run
```

## Reference Dossiers

The script processes these test cases:

- **191**: Completed Law (Standard) - Has AangenomenTekst + Verslag
- **135**: Debated Bill (Ongoing) - Has Verslag + Voorstel, no Law
- **30**: Floating Amendment (Stalled) - Has Voorstel + Amendement, no Verslag
- **449**: Special (Budget) - Begroting

## Output

Results are saved to `summarizer/results.json` with the following structure:

```json
{
  "dossier_id": "...",
  "title": "...",
  "fact_summary": {
    "summary": "...",
    "document_type": "...",
    "selection_reason": "..."
  },
  "political_analysis": {
    "arguments_for": [...],
    "arguments_against": [...],
    "neutral_technical": [...],
    "notable_changes": "...",
    "has_debate": true/false
  }
}
```

