from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"
DOCS = ROOT / "docs"
DASHBOARD = ROOT / "outputs" / "dashboard" / "centro-control-mantenimiento-ferroviario.html"


def _load_metrics() -> dict[str, str]:
    df = pd.read_csv(PROCESSED / "narrative_metrics_official.csv")
    return {str(r["metric_id"]): str(r["metric_value"]) for _, r in df.iterrows()}


def _extract_float(text: str, pattern: str) -> float | None:
    m = re.search(pattern, text, flags=re.IGNORECASE)
    if not m:
        return None
    return float(m.group(1).replace(",", "."))


def _extract_token(text: str, pattern: str) -> str | None:
    m = re.search(pattern, text, flags=re.IGNORECASE)
    if not m:
        return None
    return m.group(1)


def _extract_int(text: str, pattern: str) -> int | None:
    m = re.search(pattern, text, flags=re.IGNORECASE)
    if not m:
        return None
    return int(m.group(1).replace(".", "").replace(",", ""))


def test_reporting_governance_artifacts_exist():
    expected = [
        PROCESSED / "narrative_metrics_official.csv",
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
        "n_flotas",
        "n_unidades",
        "n_depositos",
        "n_componentes",
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

    avail = _extract_float(readme, r"\| Disponibilidad media de flota \| \*\*([0-9]+(?:[\.,][0-9]+)?) %\*\*")
    high = _extract_int(readme, r"\| Unidades de alto riesgo .* \| \*\*([0-9\.,]+)\*\*")
    backlog_physical = _extract_int(readme, r"\| Backlog f[ií]sico \| \*\*([0-9\.,]+) pendientes\*\*")
    backlog_overdue = _extract_int(readme, r"\| Backlog vencido \| \*\*([0-9\.,]+) pendientes\*\*")
    backlog_critical = _extract_int(readme, r"\| Backlog cr[ií]tico f[ií]sico \| \*\*([0-9\.,]+) pendientes\*\*")
    deferral_high = _extract_int(readme, r"\| Casos de alto riesgo de diferimiento \| \*\*([0-9\.,]+)\*\*")
    top_unit = _extract_token(readme, r"intervenir primero la unidad `([A-Z0-9]+)`")
    top_comp = _extract_token(readme, r"componente `([A-Z0-9]+)`")

    assert avail is not None and abs(avail - float(metrics["fleet_availability_pct"])) <= 0.05
    assert high is not None and high == int(float(metrics["high_risk_units_count"]))
    assert backlog_physical is not None and backlog_physical == int(float(metrics["backlog_physical_items_count"]))
    assert backlog_overdue is not None and backlog_overdue == int(float(metrics["backlog_overdue_items_count"]))
    assert backlog_critical is not None and backlog_critical == int(float(metrics["backlog_critical_physical_count"]))
    assert deferral_high is not None and deferral_high == int(float(metrics["high_deferral_risk_cases_count"]))
    assert top_unit == metrics["top_unit_by_priority"]
    assert top_comp == metrics["top_component_by_priority"]


def test_memo_dashboard_summary_aligned_to_ssot():
    metrics = _load_metrics()
    memo = (DOCS / "memo_ejecutivo_es.md").read_text(encoding="utf-8")
    dashboard = DASHBOARD.read_text(encoding="utf-8")
    memo_unit = _extract_token(memo, r"Unidad prioritaria:\s*([A-Z0-9]+)")
    memo_comp = _extract_token(memo, r"Componente prioritario:\s*([A-Z0-9]+)")
    dash_unit = _extract_token(dashboard, r"Unidad que debe entrar primero:</strong>\s*([A-Z0-9]+)")
    dash_comp = _extract_token(dashboard, r"Componente prioritario:</strong>\s*([A-Z0-9]+)")

    assert memo_unit == metrics["top_unit_by_priority"]
    assert memo_comp == metrics["top_component_by_priority"]
    assert dash_unit == metrics["top_unit_by_priority"]
    assert dash_comp == metrics["top_component_by_priority"]


def test_signed_cbm_differential_is_labeled_honestly():
    metrics = _load_metrics()
    delta = float(metrics["cbm_operational_savings_eur"])
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    memo = (DOCS / "memo_ejecutivo_es.md").read_text(encoding="utf-8")
    if delta < 0:
        assert "Coste incremental proxy CBM vs reactiva" in readme
        assert "Coste incremental proxy estimado CBM vs reactiva" in memo
    else:
        assert "Ahorro operativo proxy CBM vs reactiva" in readme
        assert "Ahorro operativo proxy estimado CBM vs reactiva" in memo
