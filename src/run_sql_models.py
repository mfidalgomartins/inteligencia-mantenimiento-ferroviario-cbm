from __future__ import annotations

import warnings

from src.run_sql_layer import run_sql_layer


def run_sql_models() -> None:
    """Compatibilidad legacy: redirige al runner SQL oficial basado en DuckDB."""
    warnings.warn(
        "run_sql_models está deprecado; use run_sql_layer (DuckDB) como source of truth.",
        DeprecationWarning,
        stacklevel=2,
    )
    run_sql_layer()


if __name__ == "__main__":
    run_sql_models()
