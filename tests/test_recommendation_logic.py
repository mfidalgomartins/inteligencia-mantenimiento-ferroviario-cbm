from pathlib import Path

import pandas as pd
import pytest

from src.recommendation_engine import assign_component_recommendations, assign_operational_decisions

ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"


def test_decision_type_not_collapsed():
    prio = pd.read_csv(PROCESSED / "workshop_priority_table.csv")
    dominant_share = float(prio["decision_type"].value_counts(normalize=True).max())
    n_classes = int(prio["decision_type"].nunique())
    assert dominant_share <= 0.60
    assert n_classes >= 6


def test_high_risk_not_mapped_to_low_action():
    score = pd.read_csv(PROCESSED / "scoring_componentes.csv")
    high = score[score["prob_fallo_30d"] >= 0.80]
    if high.empty:
        return
    low_actions = {"no_accion_por_ahora", "mantener_bajo_observacion"}
    share_low = float(high["recommended_action_initial"].isin(low_actions).mean())
    assert share_low <= 0.10


def test_no_action_has_low_average_risk():
    score = pd.read_csv(PROCESSED / "scoring_componentes.csv")
    subset = score[score["recommended_action_initial"] == "no_accion_por_ahora"]
    if subset.empty:
        return
    assert float(subset["prob_fallo_30d"].mean()) <= 0.30


def test_escalation_is_traceable_to_conflict_rule():
    score = pd.read_csv(PROCESSED / "scoring_componentes.csv")
    escalated = score[score["recommended_action_initial"] == "escalado_tecnico_manual_review"]
    if escalated.empty:
        return
    assert escalated["recommendation_conflict_flag"].eq(1).all()
    assert escalated["recommendation_rule_id"].eq("R07_escalado_conflicto").all()


def test_component_recommendations_validate_required_columns():
    with pytest.raises(ValueError, match="columnas obligatorias ausentes"):
        assign_component_recommendations(pd.DataFrame({"component_health_score": [80.0]}))


def test_operational_decisions_validate_required_columns():
    with pytest.raises(ValueError, match="columnas obligatorias ausentes"):
        assign_operational_decisions(pd.DataFrame({"prob_fallo_30d": [0.1]}))


def test_component_recommendations_do_not_mutate_input():
    df = pd.DataFrame(
        {
            "component_failure_risk_score": [0.05, 0.55, 0.95],
            "component_health_score": [92.0, 55.0, 18.0],
            "deterioration_index": [4.0, 44.0, 91.0],
            "predicted_unavailability_risk": [0.03, 0.35, 0.84],
            "impact_on_service_proxy": [8.0, 45.0, 96.0],
            "defect_confidence_recent": [0.9, 0.65, 0.88],
            "critical_alerts_count": [0, 1, 3],
            "backlog_exposure_flag": [0, 0, 1],
            "confidence_flag": ["alta", "media", "alta"],
        }
    )
    before = df.copy(deep=True)

    out = assign_component_recommendations(df)

    pd.testing.assert_frame_equal(df, before)
    assert "recommended_action_initial" in out.columns
    assert set(out["recommended_action_initial"]).issubset(
        {
            "intervencion_inmediata",
            "intervencion_proxima_ventana",
            "inspeccion_prioritaria",
            "monitorizacion_intensiva",
            "mantener_bajo_observacion",
            "no_accion_por_ahora",
            "escalado_tecnico_manual_review",
        }
    )
