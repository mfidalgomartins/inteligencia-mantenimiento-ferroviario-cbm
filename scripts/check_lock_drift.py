#!/usr/bin/env python3
"""Verifica que el entorno activo coincide exactamente con requirements-lock.txt.

Falla con código 1 si algún paquete fijado está ausente, tiene otra versión, o si
alguna entrada del lock no usa un pin exacto (``==``). Se usa como puerta de calidad
en local (`run_tests.sh`) y en CI.
"""

from __future__ import annotations

import importlib.metadata as metadata
import sys
from pathlib import Path

LOCK_FILE = Path(__file__).resolve().parents[1] / "requirements-lock.txt"


def _normalize(name: str) -> str:
    return name.lower().replace("_", "-")


def check_drift() -> list[str]:
    installed = {
        _normalize(dist.metadata["Name"]): dist.version
        for dist in metadata.distributions()
    }
    drift: list[str] = []

    for raw_line in LOCK_FILE.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "==" not in line:
            drift.append(f"{line}: entrada sin pin exacto")
            continue
        name, expected = line.split("==", 1)
        actual = installed.get(_normalize(name))
        if actual != expected:
            drift.append(f"{name}: esperado {expected}, instalado {actual or 'ausente'}")

    return drift


def main() -> int:
    drift = check_drift()
    if drift:
        print("Deriva entre requirements-lock.txt y el entorno activo:", file=sys.stderr)
        for item in drift:
            print(f"- {item}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
