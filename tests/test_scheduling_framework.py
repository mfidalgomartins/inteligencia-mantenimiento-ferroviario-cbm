from __future__ import annotations

from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"
REPORTS = ROOT / "outputs" / "reports"
DOCS = ROOT / "docs"


def test_scheduling_artifacts_exist():
    expected = [
        PROCESSED / "workshop_scheduling_recommendation.csv",
        PROCESSED / "workshop_capacity_calendar.csv",
        PROCESSED / "scheduling_before_after_metrics.csv",
        PROCESSED / "scheduling_before_after_deltas.csv",
        PROCESSED / "scheduling_status_distribution.csv",
        PROCESSED / "scheduling_bottleneck_diagnosis.csv",
        REPORTS / "scheduling_before_after_metrics.csv",
        REPORTS / "scheduling_before_after_deltas.csv",
        REPORTS / "scheduling_status_distribution.csv",
        DOCS / "scheduling_framework.md",
    ]
    for path in expected:
        assert path.exists(), f"Falta artefacto de scheduling hardening: {path}"


def test_scheduling_status_taxonomy_and_temporal_consistency():
    sched = pd.read_csv(PROCESSED / "workshop_scheduling_recommendation.csv")

    valid = {
        "programada",
        "programable_proxima_ventana",
        "pendiente_repuesto",
        "pendiente_capacidad",
        "pendiente_conflicto_operativo",
        "escalar_decision",
    }
    assert set(sched["estado_intervencion"].dropna().unique()).issubset(valid)

    scheduled = sched["estado_intervencion"].isin({"programada", "programable_proxima_ventana"})
    pending = sched["estado_intervencion"].isin(
        {"pendiente_repuesto", "pendiente_capacidad", "pendiente_conflicto_operativo", "escalar_decision"}
    )
    assert sched.loc[scheduled, "ventana_temporal_sugerida"].notna().all()
    assert sched.loc[pending, "ventana_temporal_sugerida"].isna().all()


def test_scheduling_capacity_sanity():
    cap = pd.read_csv(PROCESSED / "workshop_capacity_calendar.csv")
    assert (cap["total_used_h"] <= (cap["total_capacity_h"] + 1e-6)).all()
    assert (cap["total_capacity_h"] > 0).all()


def test_scheduling_before_after_actionability_improves():
    metrics = pd.read_csv(PROCESSED / "scheduling_before_after_metrics.csv")
    before = float(metrics.loc[metrics["scenario"] == "baseline_greedy_21d", "actionable_pct"].iloc[0])
    after = float(metrics.loc[metrics["scenario"] == "heuristica_redisenada_35d", "actionable_pct"].iloc[0])
    assert after >= before + 5.0
