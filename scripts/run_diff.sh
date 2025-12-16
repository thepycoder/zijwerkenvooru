#!/bin/bash
set -e

SCRIPT_DIR=$(dirname "$0")
VENV_DIR="$SCRIPT_DIR/.venv"

if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment in $VENV_DIR..."
    python3 -m venv "$VENV_DIR"
    echo "Installing dependencies..."
    "$VENV_DIR/bin/pip" install pandas pyarrow
fi

"$VENV_DIR/bin/python" "$SCRIPT_DIR/diff_parquet.py"

