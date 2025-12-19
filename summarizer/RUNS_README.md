# LLM Model Validation Test Bench

This directory contains an improved system for versioning and tracking prompts and their outputs across different LLM models.

## Features

- **Central Configuration**: All models to test are defined in `model_config.py`
- **Parallel Execution**: Run all models simultaneously for faster testing
- **Organized Results**: Each run creates a timestamped directory with all outputs
- **Prompt Versioning**: Prompts are automatically copied to each run directory
- **Run Metadata**: Each run includes metadata (timestamp, git commit, etc.)
- **Compare Interface**: Updated compare.html supports comparing models from different runs

## Quick Start

### 1. Configure Models

Edit `model_config.py` to add or modify the models you want to test:

```python
MODELS_TO_TEST = [
    ModelConfig(
        name="Claude Sonnet 4.5",
        provider="anthropic",
        model="claude-sonnet-4-5-20250929",
        client_class=AnthropicLLMClient,
    ),
    # Add more models...
]
```

### 2. Run All Models

```bash
python -m summarizer.run_all_models
```

This will:
- Create a new run directory: `runs/run_YYYYMMDD_HHMMSS/`
- Run all configured models in parallel
- Save results to `results_<model>.json` files in the run directory
- Copy `prompts.py` to the run directory
- Generate `run_metadata.json` and `run_summary.json`
- Update `runs/runs_index.json`

### 3. Compare Results

Open `compare.html` in a web browser. The interface will:
- Automatically discover all runs from the `runs/` directory
- Group models by run in the dropdown menus
- Show run metadata (timestamp, git commit) for each model
- Allow comparing models from different runs side-by-side

## Directory Structure

```
summarizer/
├── model_config.py          # Central configuration for all models
├── run_all_models.py        # Parallel runner script
├── generate_runs_index.py   # Script to generate runs index
├── compare.html             # Web interface for comparing results
├── prompts.py               # Current prompts (copied to each run)
└── runs/                    # All run results
    ├── runs_index.json      # Index of all runs
    └── run_20240101_120000/ # Individual run directory
        ├── run_metadata.json
        ├── run_summary.json
        ├── prompts.py       # Prompts used for this run
        ├── results_claude-sonnet-4-5-20250929.json
        ├── results_gemini-3-pro-preview.json
        └── ...
```

## Run Metadata

Each run directory contains:

- **run_metadata.json**: Timestamp, git commit hash, git branch
- **run_summary.json**: Summary of which models succeeded/failed, result counts
- **prompts.py**: Exact prompts used for this run (for reproducibility)
- **results_*.json**: Individual model outputs

## Legacy Support

The old `run.py` script still works for single-model testing. The new system is backward compatible - `compare.html` will also load results from the old `results_*.json` files in the main directory.

## Tips

1. **Version Control**: Commit `model_config.py` to track which models you're testing
2. **Prompt Changes**: When you modify `prompts.py`, run a new test to compare with previous versions
3. **Run Comparison**: Use `compare.html` to compare the same model across different runs to see the impact of prompt changes
4. **Cost Tracking**: Each run can be associated with cost estimates if you add that functionality

## Additional Improvements

Some ideas for further enhancement:

- Add cost tracking per model/run
- Add performance metrics (latency, token counts)
- Add A/B testing framework for prompt variations
- Add automated quality scoring
- Add export functionality for results analysis
- Add filtering/search in the compare interface

