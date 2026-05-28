from __future__ import annotations

from src.build_dashboard import build_dashboard
from src.early_warning import run_early_warning_rules
from src.explore_data_audit import run_explore_data_audit
from src.feature_engineering import build_feature_tables
from src.generate_synthetic_data import generate_synthetic_data
from src.impact_analysis import run_defer_impact_analysis
from src.inspection_module import run_inspection_module
from src.notebooks_builder import build_notebooks
from src.governance_contracts import run_governance_contracts
from src.risk_scoring import run_risk_scoring
from src.rul_estimation import estimate_rul
from src.run_sql_layer import run_sql_layer
from src.strategy_comparison import run_strategy_comparison
from src.workshop_prioritization import run_workshop_prioritization


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

    build_notebooks()
    run_governance_contracts(fail_on_blocker=True)
    build_dashboard()


if __name__ == "__main__":
    run_pipeline()
