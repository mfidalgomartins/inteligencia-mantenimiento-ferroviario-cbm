#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
PYTHON_BIN="${PYTHON_BIN:-python}"
if [[ -x ".venv/bin/python" ]]; then
  PYTHON_BIN=".venv/bin/python"
fi

"$PYTHON_BIN" -m ruff check .
"$PYTHON_BIN" -m pytest -q "$@"
"$PYTHON_BIN" - <<'PY'
from __future__ import annotations

import importlib.metadata as metadata
import sys
from pathlib import Path

installed = {
    dist.metadata["Name"].lower().replace("_", "-"): dist.version
    for dist in metadata.distributions()
}
drift: list[str] = []

for raw_line in Path("requirements-lock.txt").read_text(encoding="utf-8").splitlines():
    line = raw_line.strip()
    if not line or line.startswith("#"):
        continue
    if "==" not in line:
        drift.append(f"{line}: entrada sem pin exato")
        continue
    name, expected = line.split("==", 1)
    actual = installed.get(name.lower().replace("_", "-"))
    if actual != expected:
        drift.append(f"{name}: esperado {expected}, instalado {actual or 'ausente'}")

if drift:
    print("Drift entre requirements-lock.txt e ambiente ativo:", file=sys.stderr)
    for item in drift:
        print(f"- {item}", file=sys.stderr)
    sys.exit(1)
PY
