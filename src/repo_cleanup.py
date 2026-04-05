from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.config import OUTPUTS_REPORTS_DIR


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def run_repo_cleanup_inventory() -> None:
    OUTPUTS_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    inventory = pd.DataFrame(
        [
            {
                "artifact_path": "src/run_pipeline.py",
                "category": "pipeline",
                "status": "mantener",
                "source_of_truth": True,
                "reason": "Entry point único del pipeline.",
            },
            {
                "artifact_path": "src/run_sql_layer.py",
                "category": "sql_runner",
                "status": "mantener",
                "source_of_truth": True,
                "reason": "Runner oficial DuckDB.",
            },
            {
                "artifact_path": "src/run_sql_models.py",
                "category": "sql_runner_legacy",
                "status": "deprecar",
                "source_of_truth": False,
                "reason": "Shim de compatibilidad; redirige al runner oficial.",
            },
            {
                "artifact_path": "src/explore_data_audit.py",
                "category": "data_audit",
                "status": "mantener",
                "source_of_truth": True,
                "reason": "Auditoría oficial por tabla/capa.",
            },
            {
                "artifact_path": "src/data_profile_audit.py",
                "category": "data_audit_legacy",
                "status": "deprecar",
                "source_of_truth": False,
                "reason": "Shim de compatibilidad; delega a auditoría oficial.",
            },
            {
                "artifact_path": "src/build_dashboard.py",
                "category": "dashboard",
                "status": "mantener",
                "source_of_truth": True,
                "reason": "Dashboard ejecutivo offline único.",
            },
            {
                "artifact_path": "docs/repo_architecture.md",
                "category": "governance",
                "status": "mantener",
                "source_of_truth": True,
                "reason": "Guía de arquitectura y ownership.",
            },
        ]
    )

    inventory["exists"] = inventory["artifact_path"].apply(lambda p: (PROJECT_ROOT / p).exists())
    inventory["dead_route_flag"] = ~inventory["exists"]
    inventory.to_csv(OUTPUTS_REPORTS_DIR / "repo_cleanup_map.csv", index=False)

    dead = inventory[inventory["dead_route_flag"]].copy()
    dead.to_csv(OUTPUTS_REPORTS_DIR / "repo_dead_routes.csv", index=False)

    changelog = [
        "# Changelog de Cleanup Arquitectónico",
        "",
        "## Cambios aplicados",
        "- Consolidado SQL runner oficial en `src/run_sql_layer.py`.",
        "- `src/run_sql_models.py` convertido a shim de compatibilidad (deprecado).",
        "- Consolidada auditoría oficial en `src/explore_data_audit.py`.",
        "- `src/data_profile_audit.py` convertido a shim de compatibilidad (deprecado).",
        "- Documentación de arquitectura activa en `docs/repo_architecture.md`.",
        "",
        "## Resultado",
        f"- Artefactos inventariados: {len(inventory)}",
        f"- Rutas muertas detectadas: {len(dead)}",
    ]
    (OUTPUTS_REPORTS_DIR / "repo_cleanup_changelog.md").write_text("\n".join(changelog), encoding="utf-8")


if __name__ == "__main__":
    run_repo_cleanup_inventory()
