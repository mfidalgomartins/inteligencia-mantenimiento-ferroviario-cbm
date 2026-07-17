#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
if [[ -z "${PYTHON_BIN:-}" ]]; then
  PYTHON_BIN="python"
  if [[ -x ".venv/bin/python" ]]; then
    PYTHON_BIN=".venv/bin/python"
  fi
fi

export PYTHONPATH="$PWD/src${PYTHONPATH:+:$PYTHONPATH}"

"$PYTHON_BIN" -m coverage erase
echo "Cobertura: flujo completo, publicación y pruebas"
"$PYTHON_BIN" -m coverage run --branch --source=railway_cbm -m railway_cbm.run_pipeline
"$PYTHON_BIN" scripts/build_publication_outputs.py
"$PYTHON_BIN" -m coverage run --branch --source=railway_cbm -a -m pytest -q
"$PYTHON_BIN" -m coverage report --show-missing
