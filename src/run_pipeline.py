from __future__ import annotations

import shutil

from src.advanced_analysis import run_advanced_analysis
from src.build_dashboard import build_dashboard
from src.early_warning import run_early_warning_rules
from src.explore_data_audit import run_explore_data_audit
from src.feature_engineering import build_feature_tables
from src.generate_synthetic_data import generate_synthetic_data
from src.impact_analysis import run_defer_impact_analysis
from src.inspection_module import run_inspection_module
from src.notebooks_builder import build_notebooks
from src.governance_contracts import run_governance_contracts
from src.repo_cleanup import run_repo_cleanup_inventory
from src.release_hardening import run_release_hardening
from src.reporting import generate_executive_outputs
from src.risk_scoring import run_risk_scoring
from src.rul_estimation import estimate_rul
from src.score_calibration_hardening import run_score_calibration_hardening
from src.run_sql_layer import run_sql_layer
from src.strategy_comparison import run_strategy_comparison
from src.validation import run_validation
from src.visualization import run_visualizations
from src.workshop_prioritization import run_workshop_prioritization
from src.config import DATA_PROCESSED_DIR, OUTPUTS_REPORTS_DIR


def run_pipeline() -> None:
    generate_synthetic_data()
    run_explore_data_audit()
    run_sql_layer()

    build_feature_tables()
    run_risk_scoring()
    estimate_rul()
    run_early_warning_rules()
    run_workshop_prioritization()
    run_strategy_comparison()
    run_defer_impact_analysis()
    run_inspection_module()

    run_advanced_analysis()
    run_visualizations()
    run_score_calibration_hardening(label="after")
    build_notebooks()
    run_governance_contracts(fail_on_blocker=True)
    run_repo_cleanup_inventory()
    generate_executive_outputs()
    build_dashboard()
    run_validation()
    run_release_hardening()

    _publish_key_outputs()


def _publish_key_outputs() -> None:
    OUTPUTS_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    mapping = {
        "scoring_componentes.csv": "scoring_componentes.csv",
        "rul_instancia.csv": "rul_instancia.csv",
        "priorizacion_intervenciones.csv": "priorizacion_intervenciones.csv",
        "plan_taller_14d.csv": "plan_taller_14d.csv",
        "comparativo_estrategias.csv": "comparativo_estrategias.csv",
        "impacto_diferimiento_resumen.csv": "impacto_diferimiento_resumen.csv",
        "workshop_priority_table.csv": "workshop_priority_table.csv",
        "workshop_scheduling_recommendation.csv": "workshop_scheduling_recommendation.csv",
        "component_health_score.csv": "component_health_score.csv",
        "component_failure_risk_score.csv": "component_failure_risk_score.csv",
        "component_rul_estimate.csv": "component_rul_estimate.csv",
        "unit_unavailability_risk_score.csv": "unit_unavailability_risk_score.csv",
    }

    for src_name, dst_name in mapping.items():
        src = DATA_PROCESSED_DIR / src_name
        if src.exists():
            shutil.copy2(src, OUTPUTS_REPORTS_DIR / dst_name)


if __name__ == "__main__":
    run_pipeline()
