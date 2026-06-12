from __future__ import annotations

from collections.abc import Callable
from time import perf_counter

from src.build_dashboard import build_dashboard
from src.early_warning import run_early_warning_rules
from src.explore_data_audit import run_explore_data_audit
from src.feature_engineering import build_feature_tables
from src.generate_synthetic_data import generate_synthetic_data
from src.governance_contracts import run_governance_contracts
from src.impact_analysis import run_defer_impact_analysis
from src.inspection_module import run_inspection_module
from src.notebooks_builder import build_notebooks
from src.reporting_governance import sync_narrative_artifacts
from src.risk_scoring import run_risk_scoring
from src.rul_estimation import estimate_rul
from src.run_sql_layer import run_sql_layer
from src.strategy_comparison import run_strategy_comparison
from src.workshop_prioritization import run_workshop_prioritization

PIPELINE_STEPS: list[tuple[str, Callable[[], object]]] = [
    ("Generar datos sintéticos", generate_synthetic_data),
    ("Auditar datos raw", run_explore_data_audit),
    ("Construir capa SQL", run_sql_layer),
    ("Construir features", build_feature_tables),
    ("Calcular scoring de riesgo", run_risk_scoring),
    ("Estimar RUL", estimate_rul),
    ("Aplicar reglas early warning", run_early_warning_rules),
    ("Priorizar taller y scheduling", run_workshop_prioritization),
    ("Evaluar inspección automática", run_inspection_module),
    ("Comparar estrategias", run_strategy_comparison),
    ("Analizar diferimiento", run_defer_impact_analysis),
    ("Sincronizar métricas y narrativa", lambda: sync_narrative_artifacts(force_recompute=True)),
    ("Construir notebooks", build_notebooks),
    ("Validar contratos de governance", lambda: run_governance_contracts(fail_on_blocker=True)),
    ("Construir dashboard", build_dashboard),
]


def run_pipeline() -> None:
    started = perf_counter()
    total = len(PIPELINE_STEPS)
    for idx, (label, func) in enumerate(PIPELINE_STEPS, start=1):
        step_started = perf_counter()
        print(f"[{idx:02d}/{total:02d}] {label}...", flush=True)
        func()
        elapsed = perf_counter() - step_started
        print(f"[{idx:02d}/{total:02d}] OK {label} ({elapsed:.1f}s)", flush=True)

    print(f"Pipeline completa ({perf_counter() - started:.1f}s)", flush=True)


if __name__ == "__main__":
    run_pipeline()
