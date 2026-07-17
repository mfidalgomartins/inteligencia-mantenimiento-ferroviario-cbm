"""Orquesta el flujo reproducible de datos, decisión y publicación."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter

import pandas as pd

from railway_cbm import __version__
from railway_cbm.build_dashboard import build_dashboard
from railway_cbm.capacity_optimization import run_capacity_optimization
from railway_cbm.config import DATA_PROCESSED_DIR, RANDOM_SEED, reset_generated_data
from railway_cbm.decision_governance import run_decision_governance
from railway_cbm.early_warning import run_early_warning_rules
from railway_cbm.explore_data_audit import run_explore_data_audit
from railway_cbm.feature_engineering import build_feature_tables
from railway_cbm.generate_synthetic_data import generate_synthetic_data
from railway_cbm.governance_contracts import run_governance_contracts
from railway_cbm.impact_analysis import run_defer_impact_analysis
from railway_cbm.ingestion import _copy_external_snapshot, validate_external_snapshot, write_input_manifest
from railway_cbm.inspection_module import run_inspection_module
from railway_cbm.model_monitoring import run_model_monitoring
from railway_cbm.notebooks_builder import build_notebooks
from railway_cbm.reporting_governance import sync_narrative_artifacts
from railway_cbm.risk_scoring import run_risk_scoring
from railway_cbm.rul_estimation import estimate_rul
from railway_cbm.run_sql_layer import run_sql_layer
from railway_cbm.strategy_comparison import run_strategy_comparison
from railway_cbm.workshop_prioritization import run_workshop_prioritization

PipelineStep = tuple[str, Callable[[], object]]


def build_pipeline_steps(
    *, source_mode: str = "synthetic", input_dir: str | Path | None = None, seed: int = RANDOM_SEED
) -> list[PipelineStep]:
    """Construye el flujo para una fuente sintética o un snapshot externo."""
    if source_mode not in {"synthetic", "external"}:
        raise ValueError("source_mode debe ser 'synthetic' o 'external'")
    if source_mode == "external" and input_dir is None:
        raise ValueError("input_dir es obligatorio cuando source_mode='external'")
    if source_mode == "synthetic" and input_dir is not None:
        raise ValueError("input_dir sólo se admite cuando source_mode='external'")
    if not isinstance(seed, int) or seed < 0:
        raise ValueError("seed debe ser un entero no negativo")

    if source_mode == "synthetic":
        source_step: PipelineStep = ("Generar datos sintéticos", lambda: generate_synthetic_data(seed=seed))
        source_name = "deterministic_generator"
        preflight_steps: list[PipelineStep] = []
    else:
        source_path = Path(input_dir).expanduser().resolve()  # type: ignore[arg-type]
        preflight_steps = [("Validar snapshot externo", lambda: validate_external_snapshot(source_path))]
        source_step = ("Cargar snapshot externo", lambda: _copy_external_snapshot(source_path))
        source_name = source_path.name

    return [
        *preflight_steps,
        ("Limpiar datos generados", reset_generated_data),
        source_step,
        (
            "Registrar trazabilidad de entrada",
            lambda: write_input_manifest(source_mode=source_mode, source_name=source_name),
        ),
        ("Auditar datos brutos", run_explore_data_audit),
        ("Construir capa SQL", run_sql_layer),
        ("Construir variables", build_feature_tables),
        ("Calcular puntuación de riesgo", run_risk_scoring),
        ("Validar temporalmente y monitorizar modelo", run_model_monitoring),
        ("Estimar RUL", estimate_rul),
        ("Aplicar reglas de alerta temprana", run_early_warning_rules),
        ("Priorizar taller y planificación", run_workshop_prioritization),
        ("Diagnosticar y optimizar capacidad", run_capacity_optimization),
        ("Registrar decisiones y aprobaciones", run_decision_governance),
        ("Evaluar inspección automática", run_inspection_module),
        ("Comparar estrategias", run_strategy_comparison),
        ("Analizar diferimiento", run_defer_impact_analysis),
        ("Sincronizar métricas y texto ejecutivo", lambda: sync_narrative_artifacts(force_recompute=True)),
        ("Construir notebooks", build_notebooks),
        ("Validar contratos de gobernanza", lambda: run_governance_contracts(fail_on_blocker=True)),
        ("Construir panel de control", build_dashboard),
    ]


PIPELINE_STEPS = build_pipeline_steps()


def _dataset_digest() -> str:
    manifest = pd.read_csv(DATA_PROCESSED_DIR / "input_data_manifest.csv")
    canonical = "".join(manifest.sort_values("file_name")["sha256"].astype(str))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _write_execution_manifest(
    *, rows: list[dict[str, object]], source_mode: str, seed: int | None, started_at: datetime
) -> None:
    completed_at = datetime.now(UTC)
    dataset_digest = _dataset_digest() if (DATA_PROCESSED_DIR / "input_data_manifest.csv").exists() else "unavailable"
    run_identity = f"{dataset_digest}|{source_mode}|{__version__}"
    run_id = f"RUN-{hashlib.sha256(run_identity.encode('utf-8')).hexdigest()[:16].upper()}"
    manifest = pd.DataFrame(rows)
    manifest.insert(0, "run_id", run_id)
    manifest["source_mode"] = source_mode
    manifest["seed"] = seed
    manifest["package_version"] = __version__
    manifest["dataset_sha256"] = dataset_digest
    manifest["started_at_utc"] = started_at.isoformat()
    manifest["completed_at_utc"] = completed_at.isoformat()
    manifest["total_elapsed_seconds"] = round((completed_at - started_at).total_seconds(), 6)
    manifest.to_csv(DATA_PROCESSED_DIR / "pipeline_execution_manifest.csv", index=False)


def run_pipeline(
    *, source_mode: str = "synthetic", input_dir: str | Path | None = None, seed: int = RANDOM_SEED
) -> None:
    """Ejecuta el flujo oficial y registra estado, duración y linaje de la fuente."""
    steps = build_pipeline_steps(source_mode=source_mode, input_dir=input_dir, seed=seed)
    started_at = datetime.now(UTC)
    started = perf_counter()
    total = len(steps)
    execution_rows: list[dict[str, object]] = []
    for idx, (label, func) in enumerate(steps, start=1):
        step_started = perf_counter()
        print(f"[{idx:02d}/{total:02d}] {label}...", flush=True)
        try:
            func()
        except Exception as exc:
            elapsed = perf_counter() - step_started
            execution_rows.append(
                {
                    "step_order": idx,
                    "step_name": label,
                    "status": "failed",
                    "elapsed_seconds": round(elapsed, 6),
                    "error_type": type(exc).__name__,
                }
            )
            _write_execution_manifest(
                rows=execution_rows,
                source_mode=source_mode,
                seed=seed if source_mode == "synthetic" else None,
                started_at=started_at,
            )
            raise
        elapsed = perf_counter() - step_started
        execution_rows.append(
            {
                "step_order": idx,
                "step_name": label,
                "status": "success",
                "elapsed_seconds": round(elapsed, 6),
                "error_type": "",
            }
        )
        print(f"[{idx:02d}/{total:02d}] Correcto {label} ({elapsed:.1f}s)", flush=True)

    _write_execution_manifest(
        rows=execution_rows,
        source_mode=source_mode,
        seed=seed if source_mode == "synthetic" else None,
        started_at=started_at,
    )
    print(f"Flujo completo ({perf_counter() - started:.1f}s)", flush=True)


if __name__ == "__main__":
    run_pipeline()
