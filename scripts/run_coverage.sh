#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
PYTHON_BIN="${PYTHON_BIN:-python}"
if [[ -x ".venv/bin/python" ]]; then
  PYTHON_BIN=".venv/bin/python"
fi

"$PYTHON_BIN" -m coverage erase
"$PYTHON_BIN" -m coverage run --branch --source=src -m src.run_pipeline
"$PYTHON_BIN" -m coverage run --branch --source=src -a -m pytest -q
"$PYTHON_BIN" -m coverage report --show-missing
