from pathlib import Path

import pandas as pd


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


def test_escalation_has_lower_confidence_profile():
    score = pd.read_csv(PROCESSED / "scoring_componentes.csv")
    escalated = score[score["recommended_action_initial"] == "escalado_tecnico_manual_review"]
    if escalated.empty:
        return
    share_low = float((escalated["confidence_flag"] == "baja").mean())
    assert share_low >= 0.30
