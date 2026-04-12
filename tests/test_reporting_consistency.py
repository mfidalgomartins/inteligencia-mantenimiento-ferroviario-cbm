from __future__ import annotations

import re
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"
REPORTS = ROOT / "outputs" / "reports"
DOCS = ROOT / "docs"
DASHBOARD = ROOT / "outputs" / "dashboard" / "index.html"


def _load_metrics() -> dict[str, str]:
    df = pd.read_csv(PROCESSED / "narrative_metrics_official.csv")
    return {str(r["metric_id"]): str(r["metric_value"]) for _, r in df.iterrows()}


def _extract_float(text: str, pattern: str) -> float | None:
    m = re.search(pattern, text, flags=re.IGNORECASE)
    if not m:
        return None
    return float(m.group(1))


def _extract_token(text: str, pattern: str) -> str | None:
    m = re.search(pattern, text, flags=re.IGNORECASE)
    if not m:
        return None
    return m.group(1)


def test_reporting_governance_artifacts_exist():
    expected = [
        PROCESSED / "narrative_metrics_official.csv",
        REPORTS / "narrative_metrics_official.csv",
        REPORTS / "narrative_artifact_mapping.csv",
        REPORTS / "narrative_hardcoded_audit.csv",
        REPORTS / "backlog_kpi_before_after.csv",
        REPORTS / "backlog_metric_taxonomy.csv",
        DOCS / "gobierno_metricas.md",
        DOCS / "backlog_metric_governance.md",
        ROOT / "README.md",
        DOCS / "memo_ejecutivo_es.md",
        DASHBOARD,
    ]
    for path in expected:
        assert path.exists(), f"Falta artefacto de reporting governance: {path}"


def test_narrative_metrics_required_ids():
    metrics = _load_metrics()
    required = {
        "fleet_availability_pct",
        "high_risk_units_count",
        "backlog_physical_items_count",
        "backlog_overdue_items_count",
        "backlog_critical_physical_count",
        "high_deferral_risk_cases_count",
        "backlog_exposure_adjusted_mean",
        "cbm_operational_savings_eur",
        "deferral_cost_delta_14d_eur",
        "deferral_downtime_delta_14d_h",
        "top_unit_by_priority",
        "top_component_by_priority",
        "top_component_family_by_priority",
        "mean_depot_saturation_pct",
    }
    assert required.issubset(set(metrics.keys()))


def test_readme_consistent_with_ssot_metrics():
    metrics = _load_metrics()
    readme = (ROOT / "README.md").read_text(encoding="utf-8")

    avail = _extract_float(readme, r"disponibilidad media de flota:\s*\*\*([0-9]+(?:\.[0-9]+)?)%")
    high = _extract_float(readme, r"unidades de alto riesgo:\s*\*\*([0-9]+)")
    backlog_physical = _extract_float(readme, r"backlog f[ií]sico:\s*\*\*([0-9]+)")
    backlog_overdue = _extract_float(readme, r"backlog vencido:\s*\*\*([0-9]+)")
    backlog_critical = _extract_float(readme, r"backlog cr[ií]tico f[ií]sico:\s*\*\*([0-9]+)")
    deferral_high = _extract_float(readme, r"casos alto riesgo de diferimiento:\s*\*\*([0-9]+)")
    top_unit = _extract_token(readme, r"Unidad que debe entrar primero:\*\*\s*`([A-Z0-9]+)`")
    top_comp = _extract_token(readme, r"Componente que debe sustituirse primero:\*\*\s*`([A-Z0-9]+)`")

    assert avail is not None and abs(avail - float(metrics["fleet_availability_pct"])) <= 0.05
    assert high is not None and int(high) == int(float(metrics["high_risk_units_count"]))
    assert backlog_physical is not None and int(backlog_physical) == int(float(metrics["backlog_physical_items_count"]))
    assert backlog_overdue is not None and int(backlog_overdue) == int(float(metrics["backlog_overdue_items_count"]))
    assert backlog_critical is not None and int(backlog_critical) == int(float(metrics["backlog_critical_physical_count"]))
    assert deferral_high is not None and int(deferral_high) == int(float(metrics["high_deferral_risk_cases_count"]))
    assert top_unit == metrics["top_unit_by_priority"]
    assert top_comp == metrics["top_component_by_priority"]


def test_memo_dashboard_summary_aligned_to_ssot():
    metrics = _load_metrics()
    memo = (DOCS / "memo_ejecutivo_es.md").read_text(encoding="utf-8")
    dashboard = DASHBOARD.read_text(encoding="utf-8")
    memo_unit = _extract_token(memo, r"Unidad prioritaria:\s*([A-Z0-9]+)")
    memo_comp = _extract_token(memo, r"Componente prioritario:\s*([A-Z0-9]+)")
    dash_unit = _extract_token(dashboard, r"Unidad que debe entrar primero:</strong>\s*([A-Z0-9]+)")
    dash_comp = _extract_token(dashboard, r"Componente que debe sustituirse primero:</strong>\s*([A-Z0-9]+)")

    assert memo_unit == metrics["top_unit_by_priority"]
    assert memo_comp == metrics["top_component_by_priority"]
    assert dash_unit == metrics["top_unit_by_priority"]
    assert dash_comp == metrics["top_component_by_priority"]
