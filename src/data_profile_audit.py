from __future__ import annotations

import warnings

import pandas as pd

from src.config import OUTPUTS_REPORTS_DIR
from src.explore_data_audit import run_explore_data_audit


def run_data_profile_audit() -> None:
    """
    Compatibilidad legacy: ejecuta la auditoría oficial y genera artefactos resumen históricos.
    """
    warnings.warn(
        "run_data_profile_audit está deprecado; use run_explore_data_audit como source of truth.",
        DeprecationWarning,
        stacklevel=2,
    )
    run_explore_data_audit()

    # Compatibilidad con nombres históricos consumidos por algunos reportes.
    report_dir = OUTPUTS_REPORTS_DIR / "explore_data"
    table_profile = pd.read_csv(report_dir / "table_profile_summary.csv")
    issue_df = pd.read_csv(report_dir / "issues_prioritized.csv")

    legacy_summary = table_profile[["tabla", "n_filas", "n_columnas", "null_rate_promedio", "duplicados"]].rename(
        columns={"null_rate_promedio": "pct_null_medio", "duplicados": "n_duplicados"}
    )
    legacy_summary["pct_null_max"] = legacy_summary["pct_null_medio"]
    legacy_summary.to_csv(OUTPUTS_REPORTS_DIR / "data_profile_summary.csv", index=False)

    checks = issue_df.groupby("issue", as_index=False).agg(n_errores=("tabla", "count")).rename(columns={"issue": "check"})
    checks.to_csv(OUTPUTS_REPORTS_DIR / "data_quality_checks.csv", index=False)

    md_lines = [
        "# Reporte de Profiling y Auditoría de Calidad",
        "",
        "Artefacto de compatibilidad. La auditoría oficial está en `outputs/reports/explore_data/`.",
        "",
        f"- Tablas perfiladas: {legacy_summary['tabla'].nunique()}",
        f"- Issues detectados: {len(issue_df)}",
    ]
    (OUTPUTS_REPORTS_DIR / "data_quality_report.md").write_text("\n".join(md_lines), encoding="utf-8")


if __name__ == "__main__":
    run_data_profile_audit()
