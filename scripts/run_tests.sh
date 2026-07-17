#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
if [[ -z "${PYTHON_BIN:-}" ]]; then
  PYTHON_BIN="python"
  if [[ -x ".venv/bin/python" ]]; then
    PYTHON_BIN=".venv/bin/python"
  fi
fi

"$PYTHON_BIN" -m ruff check .
"$PYTHON_BIN" -m ruff format --check .
"$PYTHON_BIN" -m pytest -q "$@"
"$PYTHON_BIN" scripts/check_lock_drift.py
"$PYTHON_BIN" -m pip check
