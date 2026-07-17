"""Rutas, parámetros y preparación del espacio de datos del proyecto."""

import shutil
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_RAW_DIR = ROOT_DIR / "data" / "raw"
DATA_PROCESSED_DIR = ROOT_DIR / "data" / "processed"
SQL_DIR = ROOT_DIR / "sql"
OUTPUTS_DASHBOARD_DIR = ROOT_DIR / "outputs" / "dashboard"
OUTPUTS_REPORTS_DIR = ROOT_DIR / "outputs" / "reports"
DOCS_DIR = ROOT_DIR / "docs"
NOTEBOOKS_DIR = ROOT_DIR / "notebooks"

RANDOM_SEED = 42
START_DATE = "2024-01-01"
END_DATE = "2025-12-31"

GENERATED_DATA_DIRS = (DATA_RAW_DIR, DATA_PROCESSED_DIR)

for path in [
    DATA_RAW_DIR,
    DATA_PROCESSED_DIR,
    SQL_DIR,
    OUTPUTS_DASHBOARD_DIR,
    OUTPUTS_REPORTS_DIR,
    DOCS_DIR,
    NOTEBOOKS_DIR,
]:
    path.mkdir(parents=True, exist_ok=True)


def _clear_generated_directory(directory: Path) -> None:
    """Vacía un directorio generado sin seguir enlaces ni borrar ``.gitkeep``."""
    directory.mkdir(parents=True, exist_ok=True)
    for path in directory.iterdir():
        if path.name == ".gitkeep":
            continue
        if path.is_symlink() or path.is_file():
            path.unlink()
        else:
            shutil.rmtree(path)


def reset_generated_data() -> None:
    """Elimina artefactos de ejecuciones anteriores antes de regenerar el flujo."""
    for directory in GENERATED_DATA_DIRS:
        _clear_generated_directory(directory)
