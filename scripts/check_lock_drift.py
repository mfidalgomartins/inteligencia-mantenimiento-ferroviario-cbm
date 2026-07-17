#!/usr/bin/env python3
"""Valida el lock, los manifiestos de dependencias y el entorno activo."""

from __future__ import annotations

import importlib.metadata as metadata
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LOCK_FILE = ROOT / "requirements-lock.txt"
MANIFEST_FILES = (ROOT / "requirements.txt", ROOT / "requirements-dev.txt")

_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*")


def _normalize(name: str) -> str:
    """Aplica la normalización de nombres definida por PEP 503."""
    return re.sub(r"[-_.]+", "-", name).lower()


def _requirement_name(line: str) -> str | None:
    """Extrae el nombre de una dependencia de una línea de requirements."""
    match = _NAME_PATTERN.match(line.strip())
    return _normalize(match.group(0)) if match else None


def read_lock() -> tuple[dict[str, str], list[str]]:
    """Devuelve los pins exactos y los errores estructurales del lock."""
    locked: dict[str, str] = {}
    errors: list[str] = []

    for line_number, raw_line in enumerate(LOCK_FILE.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.count("==") != 1:
            errors.append(f"requirements-lock.txt:{line_number}: se requiere un único pin exacto (==)")
            continue
        name, version = (part.strip() for part in line.split("==", 1))
        normalized = _normalize(name)
        if not name or not version:
            errors.append(f"requirements-lock.txt:{line_number}: entrada incompleta")
        elif normalized in locked:
            errors.append(f"requirements-lock.txt:{line_number}: dependencia duplicada: {name}")
        else:
            locked[normalized] = version

    return locked, errors


def declared_requirements() -> set[str]:
    """Obtiene las dependencias directas declaradas en runtime y desarrollo."""
    declared: set[str] = set()
    for path in MANIFEST_FILES:
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith(("#", "-r ", "--requirement ")):
                continue
            name = _requirement_name(line)
            if name:
                declared.add(name)
    return declared


def check_drift() -> list[str]:
    """Detecta incoherencias del lock y diferencias frente al entorno activo."""
    locked, errors = read_lock()
    missing_from_lock = sorted(declared_requirements().difference(locked))
    errors.extend(f"{name}: dependencia directa ausente del lock" for name in missing_from_lock)

    installed = {_normalize(dist.metadata["Name"]): dist.version for dist in metadata.distributions()}
    for name, expected in locked.items():
        actual = installed.get(name)
        if actual != expected:
            errors.append(f"{name}: esperado {expected}, instalado {actual or 'ausente'}")

    return errors


def main() -> int:
    """Expone la validación como puerta de calidad para local y CI."""
    drift = check_drift()
    if drift:
        print("Deriva o inconsistencia en dependencias:", file=sys.stderr)
        for item in drift:
            print(f"- {item}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
