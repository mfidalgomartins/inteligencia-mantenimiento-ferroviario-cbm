from __future__ import annotations

import pandas as pd

from src import recommendation_engine as rec
from src.recommendation_engine import assign_component_recommendations, assign_operational_decisions


def _component_cases() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "case": [
                "safe",
                "observe",
                "inspect_low_conf",
                "next_window",
                "immediate",
                "conflict_low_conf",
                "missing_inputs",
                "pendientes",
            ],
            "component_failure_risk_score": [0.02, 0.18, 0.50, 0.70, 0.95, 0.90, None, 0.60],
            "component_health_score": [98, 82, 62, 45, 20, 60, None, 55],
            "deterioration_index": [2, 20, 70, 58, 90, 92, None, 65],
            "predicted_unavailability_risk": [0.02, 0.08, 0.15, 0.50, 0.85, 0.85, None, 0.35],
            "impact_on_service_proxy": [5, 18, 25, 55, 96, 95, None, 70],
            "defect_confidence_recent": [0.90, 0.80, 0.20, 0.70, 0.90, 0.05, None, 0.65],
            "critical_alerts_count": [0, 0, 0, 1, 3, 0, None, 0],
            "backlog_exposure_flag": [0, 0, 0, 0, 1, 0, None, 1],
            "confidence_flag": ["alta", "media", "baja", "media", "alta", "baja", "desconocida", "media"],
        }
    )


def _operational_cases() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "case": [
                "safe",
                "observe",
                "inspection_from_component",
                "next_window",
                "immediate",
                "conflict_capacity",
                "blocked_immediate",
                "missing_inputs",
            ],
            "prob_fallo_30d": [0.01, 0.16, 0.48, 0.70, 0.96, 0.82, 0.86, None],
            "health_score": [98, 80, 60, 48, 30, 50, 35, None],
            "component_rul_estimate": [340, 240, 150, 24, 8, 6, 10, None],
            "intervention_priority_score": [5, 25, 40, 75, 98, 85, 95, None],
            "deferral_risk_score": [2, 20, 35, 80, 96, 88, 98, None],
            "service_impact_score": [5, 30, 35, 68, 96, 90, 96, None],
            "workshop_fit_score": [92, 85, 80, 70, 75, 20, 80, None],
            "ventana_operativa_disponible": [3, 2, 2, 1, 1, 0, 0, None],
            "saturation_ratio": [0.4, 0.6, 0.8, 0.9, 0.9, 1.3, 0.8, None],
            "criticidad_servicio": [0.2, 0.4, 0.45, 0.7, 0.95, 0.9, 0.9, None],
            "predicted_unavailability_risk": [0.02, 0.1, 0.25, 0.6, 0.9, 0.8, 0.85, None],
            "confidence_rul": [0.9, 0.8, 0.4, 0.75, 0.9, 0.35, 0.9, None],
            "confidence_flag": ["alta", "media", "baja", "media", "alta", "baja", "alta", "desconocida"],
            "recommended_action_initial": [
                "no_accion_por_ahora",
                "mantener_bajo_observacion",
                "inspeccion_prioritaria",
                "intervencion_proxima_ventana",
                "intervencion_inmediata",
                "monitorizacion_intensiva",
                "intervencion_inmediata",
                "valor_no_valido",
            ],
        }
    )


def test_assign_component_recommendations_covers_core_rules_and_defaults():
    out = assign_component_recommendations(_component_cases()).set_index("case")

    assert out.loc["safe", "recommended_action_initial"] == "no_accion_por_ahora"
    assert out.loc["safe", "recommendation_rule_id"] == "R06_no_accion"
    assert out.loc["observe", "recommended_action_initial"] == "mantener_bajo_observacion"
    assert out.loc["inspect_low_conf", "recommended_action_initial"] == "inspeccion_prioritaria"
    assert out.loc["immediate", "recommended_action_initial"] == "intervencion_inmediata"
    assert out.loc["conflict_low_conf", "recommended_action_initial"] == "escalado_tecnico_revision_manual"
    assert int(out.loc["conflict_low_conf", "recommendation_conflict_flag"]) == 1

    assert out.loc["missing_inputs", "component_failure_risk_score"] == 0.0
    assert out.loc["missing_inputs", "component_health_score"] == 50.0
    assert out["recommended_action_initial"].isin(rec.COMPONENT_ACTIONS).all()
    assert out["recommendation_rationale"].str.contains("riesgo=").all()


def test_assign_operational_decisions_handles_conflicts_capacity_and_defaults():
    out = assign_operational_decisions(_operational_cases()).set_index("case")

    assert out.loc["safe", "decision_type"] == "no acción por ahora"
    assert out.loc["observe", "decision_type"] == "mantener bajo observación"
    assert out.loc["inspection_from_component", "decision_type"] == "inspección prioritaria"
    assert out.loc["immediate", "decision_type"] == "intervención inmediata"
    assert out.loc["conflict_capacity", "decision_type"] == "escalado técnico/revisión manual"
    assert int(out.loc["conflict_capacity", "decision_conflict_flag"]) == 1
    assert out.loc["blocked_immediate", "decision_rule_id"] == "D02B_inmediata_bloqueada_capacidad"

    assert out.loc["missing_inputs", "prob_fallo_30d"] == 0.0
    assert out.loc["missing_inputs", "component_rul_estimate"] == 365.0
    assert out["decision_type"].isin(rec.OPERATIONAL_DECISIONS).all()
    assert out["decision_rationale"].str.contains("ajuste=").all()


def test_write_recommendation_reports_and_logic_doc_use_target_dirs(tmp_path, monkeypatch):
    reports_dir = tmp_path / "reports"
    docs_dir = tmp_path / "docs"
    monkeypatch.setattr(rec, "OUTPUTS_REPORTS_DIR", reports_dir)
    monkeypatch.setattr(rec, "DOCS_DIR", docs_dir)

    score = pd.DataFrame(
        {
            "recommended_action_initial": [
                "intervencion_inmediata",
                "intervencion_inmediata",
                "monitorizacion_intensiva",
            ]
        }
    )
    priorities = pd.DataFrame(
        {
            "decision_type": [
                "intervención inmediata",
                "monitorización intensiva",
                "monitorización intensiva",
            ]
        }
    )
    examples = pd.DataFrame(
        {
            "caso": ["alta_urgencia"],
            "decision_rule_id": ["D01_inmediata"],
            "decision_type": ["intervención inmediata"],
        }
    )

    rec.write_recommendation_distribution_reports(score, priorities)
    rec.write_recommendation_logic_doc(examples)

    action_dist = pd.read_csv(reports_dir / "recommendation_action_distribution.csv")
    decision_dist = pd.read_csv(reports_dir / "recommendation_decision_distribution.csv")
    assert action_dist["proporcion"].sum().round(6) == 1.0
    assert decision_dist["proporcion"].sum().round(6) == 1.0
    assert (reports_dir / "recommendation_rules_component.csv").exists()
    assert (reports_dir / "recommendation_rules_operational.csv").exists()

    doc = (docs_dir / "recommendation_decision_logic.md").read_text(encoding="utf-8")
    assert "D01_inmediata" in doc
    assert "alta_urgencia" in doc
