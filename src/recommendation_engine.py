from __future__ import annotations

from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.config import DOCS_DIR, OUTPUTS_CHARTS_DIR, OUTPUTS_REPORTS_DIR

matplotlib.use("Agg")


COMPONENT_ACTIONS = [
    "intervencion_inmediata",
    "intervencion_proxima_ventana",
    "inspeccion_prioritaria",
    "monitorizacion_intensiva",
    "mantener_bajo_observacion",
    "no_accion_por_ahora",
    "escalado_tecnico_manual_review",
]

OPERATIONAL_DECISIONS = [
    "intervención inmediata",
    "intervención en próxima ventana",
    "inspección prioritaria",
    "monitorización intensiva",
    "mantener bajo observación",
    "no acción por ahora",
    "escalado técnico/manual review",
]


def _confidence_value(series: pd.Series) -> pd.Series:
    return series.map({"alta": 0.92, "media": 0.74, "baja": 0.52}).fillna(0.66)


def assign_component_recommendations(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    out["component_failure_risk_score"] = out["component_failure_risk_score"].fillna(0.0).clip(0, 1)
    out["component_health_score"] = out["component_health_score"].fillna(50.0).clip(0, 100)
    out["deterioration_index"] = out["deterioration_index"].fillna((100 - out["component_health_score"])).clip(0, 100)
    out["predicted_unavailability_risk"] = out["predicted_unavailability_risk"].fillna(0.0).clip(0, 1)
    out["impact_on_service_proxy"] = out["impact_on_service_proxy"].fillna(45.0).clip(0, 100)
    out["defect_confidence_recent"] = out["defect_confidence_recent"].fillna(0.62).clip(0, 1)
    out["critical_alerts_count"] = out["critical_alerts_count"].fillna(0).clip(lower=0)
    out["backlog_exposure_flag"] = out["backlog_exposure_flag"].fillna(0).astype(int).clip(0, 1)

    model_conf = _confidence_value(out["confidence_flag"])
    signal_conf = (0.55 * model_conf + 0.45 * out["defect_confidence_recent"]).clip(0, 1)
    impact_norm = (0.65 * out["impact_on_service_proxy"] / 100.0 + 0.35 * out["predicted_unavailability_risk"]).clip(0, 1)
    urgency_core = (
        out["component_failure_risk_score"] * 0.45
        + (1 - out["component_health_score"] / 100.0) * 0.22
        + out["deterioration_index"] / 100.0 * 0.13
        + impact_norm * 0.12
        + out["backlog_exposure_flag"] * 0.08
    ).clip(0, 1)

    risk_q20 = float(out["component_failure_risk_score"].quantile(0.20))
    risk_q35 = float(out["component_failure_risk_score"].quantile(0.35))
    risk_q70 = float(out["component_failure_risk_score"].quantile(0.70))
    risk_q88 = float(out["component_failure_risk_score"].quantile(0.88))
    risk_q95 = float(out["component_failure_risk_score"].quantile(0.95))

    health_q65 = float(out["component_health_score"].quantile(0.65))
    health_q80 = float(out["component_health_score"].quantile(0.80))

    impact_q35 = float(impact_norm.quantile(0.35))
    impact_q45 = float(impact_norm.quantile(0.45))
    impact_q75 = float(impact_norm.quantile(0.75))

    out["recommended_action_initial"] = "monitorizacion_intensiva"
    out["recommendation_rule_id"] = "R04_monitorizacion"
    out["recommendation_conflict_flag"] = 0

    c_no_action = (
        (out["component_failure_risk_score"] <= risk_q20)
        & (out["component_health_score"] >= health_q80)
        & (out["deterioration_index"] <= out["deterioration_index"].quantile(0.30))
        & (impact_norm <= impact_q35)
        & (out["critical_alerts_count"] == 0)
        & (out["backlog_exposure_flag"] == 0)
    )
    out.loc[c_no_action, "recommended_action_initial"] = "no_accion_por_ahora"
    out.loc[c_no_action, "recommendation_rule_id"] = "R06_no_accion"

    c_observe = (
        (out["component_failure_risk_score"] <= risk_q35)
        & (out["component_health_score"] >= health_q65)
        & (impact_norm <= impact_q45)
        & (out["critical_alerts_count"] <= 1)
        & (~c_no_action)
    )
    out.loc[c_observe, "recommended_action_initial"] = "mantener_bajo_observacion"
    out.loc[c_observe, "recommendation_rule_id"] = "R05_observacion"

    c_inspect = (
        (
            (out["component_failure_risk_score"].between(max(0.40, risk_q35), max(0.55, risk_q70), inclusive="left"))
            & (signal_conf < 0.62)
        )
        | ((out["deterioration_index"] >= out["deterioration_index"].quantile(0.70)) & (impact_norm < impact_q45) & (out["component_failure_risk_score"] < risk_q88))
        | ((out["critical_alerts_count"] >= 2) & (signal_conf < 0.68))
    )
    out.loc[c_inspect, "recommended_action_initial"] = "inspeccion_prioritaria"
    out.loc[c_inspect, "recommendation_rule_id"] = "R03_inspeccion"

    c_next_window = (
        (out["component_failure_risk_score"] >= risk_q70)
        | (out["component_health_score"] <= out["component_health_score"].quantile(0.35))
        | (urgency_core >= 0.62)
    )
    out.loc[c_next_window, "recommended_action_initial"] = "intervencion_proxima_ventana"
    out.loc[c_next_window, "recommendation_rule_id"] = "R02_proxima_ventana"

    c_immediate = (
        ((out["component_failure_risk_score"] >= risk_q95) & (impact_norm >= impact_q75) & (signal_conf >= 0.50))
        | ((out["component_health_score"] <= out["component_health_score"].quantile(0.12)) & (impact_norm >= impact_q75) & (signal_conf >= 0.55))
        | (
            (out["component_failure_risk_score"] >= risk_q88)
            & (impact_norm >= impact_q75)
            & (out["critical_alerts_count"] >= 2)
            & (signal_conf >= 0.58)
        )
        | (
            (out["component_failure_risk_score"] >= 0.88)
            & (out["critical_alerts_count"] >= 1)
            & (impact_norm >= 0.55)
        )
    )
    out.loc[c_immediate, "recommended_action_initial"] = "intervencion_inmediata"
    out.loc[c_immediate, "recommendation_rule_id"] = "R01_inmediata"

    c_conflict = (
        ((out["component_failure_risk_score"] >= risk_q88) & (signal_conf < 0.56))
        | ((out["component_health_score"] <= out["component_health_score"].quantile(0.12)) & (out["component_failure_risk_score"] <= risk_q35))
        | ((out["deterioration_index"] >= out["deterioration_index"].quantile(0.88)) & (impact_norm >= impact_q75) & (signal_conf < 0.60))
    ) & (~c_immediate)
    out.loc[c_conflict, "recommended_action_initial"] = "escalado_tecnico_manual_review"
    out.loc[c_conflict, "recommendation_rule_id"] = "R07_escalado_conflicto"
    out.loc[c_conflict, "recommendation_conflict_flag"] = 1

    out["recommendation_rationale"] = (
        out["recommendation_rule_id"]
        + " | risk="
        + out["component_failure_risk_score"].round(3).astype(str)
        + ", health="
        + out["component_health_score"].round(1).astype(str)
        + ", det="
        + out["deterioration_index"].round(1).astype(str)
        + ", conf="
        + signal_conf.round(2).astype(str)
    )
    out["recommended_action_initial"] = out["recommended_action_initial"].where(
        out["recommended_action_initial"].isin(COMPONENT_ACTIONS),
        "monitorizacion_intensiva",
    )
    return out


def assign_operational_decisions(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    out["prob_fallo_30d"] = out["prob_fallo_30d"].fillna(0.0).clip(0, 1)
    out["health_score"] = out["health_score"].fillna(50.0).clip(0, 100)
    out["component_rul_estimate"] = out["component_rul_estimate"].fillna(365).clip(1, 365)
    out["intervention_priority_score"] = out["intervention_priority_score"].fillna(0).clip(0, 100)
    out["deferral_risk_score"] = out["deferral_risk_score"].fillna(0).clip(0, 100)
    out["service_impact_score"] = out["service_impact_score"].fillna(0).clip(0, 100)
    out["workshop_fit_score"] = out["workshop_fit_score"].fillna(50).clip(0, 100)
    out["ventana_operativa_disponible"] = out["ventana_operativa_disponible"].fillna(0).clip(lower=0)
    out["saturation_ratio"] = out["saturation_ratio"].fillna(1.0).clip(lower=0)
    out["criticidad_servicio"] = out["criticidad_servicio"].fillna(0.55).clip(0, 1)
    out["predicted_unavailability_risk"] = out["predicted_unavailability_risk"].fillna(0).clip(0, 1)
    out["confidence_rul"] = out["confidence_rul"].fillna(0.6).clip(0, 1)

    model_conf = _confidence_value(out["confidence_flag"])
    confidence_joint = (0.55 * model_conf + 0.45 * out["confidence_rul"]).clip(0, 1)

    impact_operativo = (
        0.50 * out["service_impact_score"] / 100.0
        + 0.30 * out["predicted_unavailability_risk"]
        + 0.20 * out["criticidad_servicio"]
    ).clip(0, 1)
    risk_q15 = float(out["prob_fallo_30d"].quantile(0.15))
    risk_q30 = float(out["prob_fallo_30d"].quantile(0.30))
    risk_q65 = float(out["prob_fallo_30d"].quantile(0.65))
    risk_q80 = float(out["prob_fallo_30d"].quantile(0.80))
    risk_q92 = float(out["prob_fallo_30d"].quantile(0.92))

    prio_q30 = float(out["intervention_priority_score"].quantile(0.30))
    prio_q45 = float(out["intervention_priority_score"].quantile(0.45))
    prio_q70 = float(out["intervention_priority_score"].quantile(0.70))
    prio_q92 = float(out["intervention_priority_score"].quantile(0.92))

    impact_q25 = float(out["service_impact_score"].quantile(0.25))
    impact_q45 = float(out["service_impact_score"].quantile(0.45))
    impact_q80 = float(out["service_impact_score"].quantile(0.80))

    rul_q10_raw = float(out["component_rul_estimate"].quantile(0.10))
    rul_q90_raw = float(out["component_rul_estimate"].quantile(0.90))
    rul_has_signal = (rul_q90_raw - rul_q10_raw) >= 30 and rul_q10_raw < 220
    if rul_has_signal:
        rul_q20 = float(out["component_rul_estimate"].quantile(0.20))
        rul_q05 = float(out["component_rul_estimate"].quantile(0.05))
    else:
        rul_q20 = 21.0
        rul_q05 = 7.0

    capacity_block = (
        (out["workshop_fit_score"] < 45)
        | (out["saturation_ratio"] > 1.05)
    )
    window_block = out["ventana_operativa_disponible"] <= 0
    high_urgency = (
        (out["prob_fallo_30d"] >= risk_q92)
        | (rul_has_signal & (out["component_rul_estimate"] <= min(rul_q05, 14)))
        | (out["intervention_priority_score"] >= prio_q92)
        | (out["deferral_risk_score"] >= out["deferral_risk_score"].quantile(0.92))
    )

    out["decision_type"] = "monitorización intensiva"
    out["decision_rule_id"] = "D04_monitorizacion"
    out["decision_conflict_flag"] = 0

    c_no_action = (
        (out["prob_fallo_30d"] <= risk_q15)
        & (out["health_score"] >= out["health_score"].quantile(0.80))
        & ((~rul_has_signal) | (out["component_rul_estimate"] > out["component_rul_estimate"].quantile(0.70)))
        & (out["service_impact_score"] <= impact_q25)
        & (out["intervention_priority_score"] <= prio_q30)
    )
    out.loc[c_no_action, "decision_type"] = "no acción por ahora"
    out.loc[c_no_action, "decision_rule_id"] = "D06_no_accion"

    c_observe = (
        (out["prob_fallo_30d"] <= risk_q30)
        & (out["health_score"] >= out["health_score"].quantile(0.62))
        & ((~rul_has_signal) | (out["component_rul_estimate"] > out["component_rul_estimate"].quantile(0.50)))
        & (out["service_impact_score"] <= impact_q45)
        & (out["intervention_priority_score"] <= prio_q45)
        & (~c_no_action)
    )
    out.loc[c_observe, "decision_type"] = "mantener bajo observación"
    out.loc[c_observe, "decision_rule_id"] = "D05_observacion"

    c_inspection = (
        ((out["prob_fallo_30d"] >= max(0.42, risk_q30)) & (out["prob_fallo_30d"] < risk_q80) & (confidence_joint < 0.63))
        | ((out["health_score"] <= out["health_score"].quantile(0.35)) & (impact_operativo < 0.52) & (out["prob_fallo_30d"] < risk_q80))
        | (out["recommended_action_initial"] == "inspeccion_prioritaria")
    )
    out.loc[c_inspection, "decision_type"] = "inspección prioritaria"
    out.loc[c_inspection, "decision_rule_id"] = "D03_inspeccion"

    c_next_window = (
        (out["prob_fallo_30d"] >= risk_q65)
        | (rul_has_signal & (out["component_rul_estimate"] <= min(rul_q20, 28)))
        | (out["intervention_priority_score"] >= prio_q70)
        | (out["deferral_risk_score"] >= out["deferral_risk_score"].quantile(0.70))
    )
    out.loc[c_next_window, "decision_type"] = "intervención en próxima ventana"
    out.loc[c_next_window, "decision_rule_id"] = "D02_proxima_ventana"

    c_immediate = (
        high_urgency
        & (out["service_impact_score"] >= impact_q80)
        & (confidence_joint >= 0.56)
        & (~capacity_block)
    )
    out.loc[c_immediate, "decision_type"] = "intervención inmediata"
    out.loc[c_immediate, "decision_rule_id"] = "D01_inmediata"

    c_conflict = (
        ((out["prob_fallo_30d"] >= risk_q80) & (confidence_joint < 0.56))
        | (
            rul_has_signal
            & (out["component_rul_estimate"] <= min(rul_q05, 14))
            & capacity_block
            & (out["prob_fallo_30d"] >= risk_q65)
        )
        | ((out["health_score"] <= out["health_score"].quantile(0.15)) & (out["prob_fallo_30d"] <= risk_q30))
    )
    out.loc[c_conflict, "decision_type"] = "escalado técnico/manual review"
    out.loc[c_conflict, "decision_rule_id"] = "D07_escalado_conflicto"
    out.loc[c_conflict, "decision_conflict_flag"] = 1

    c_immediate_blocked = c_immediate & (window_block | capacity_block) & (out["prob_fallo_30d"] < 0.88)
    out.loc[c_immediate_blocked, "decision_type"] = "intervención en próxima ventana"
    out.loc[c_immediate_blocked, "decision_rule_id"] = "D02B_inmediata_bloqueada_capacidad"

    out["decision_rationale"] = (
        out["decision_rule_id"]
        + " | risk="
        + out["prob_fallo_30d"].round(3).astype(str)
        + ", rul="
        + out["component_rul_estimate"].round(1).astype(str)
        + ", impact="
        + out["service_impact_score"].round(1).astype(str)
        + ", fit="
        + out["workshop_fit_score"].round(1).astype(str)
    )
    out["decision_type"] = out["decision_type"].where(out["decision_type"].isin(OPERATIONAL_DECISIONS), "monitorización intensiva")
    return out


def build_recommendation_before_after_outputs(score_after: pd.DataFrame, priorities_after: pd.DataFrame) -> None:
    OUTPUTS_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUTS_CHARTS_DIR.mkdir(parents=True, exist_ok=True)

    after_comp = score_after["recommended_action_initial"].value_counts(dropna=False).rename_axis("action").reset_index(name="count")
    after_comp["share"] = after_comp["count"] / after_comp["count"].sum()
    after_comp.to_csv(OUTPUTS_REPORTS_DIR / "recommendation_action_distribution_after.csv", index=False)

    after_dec = priorities_after["decision_type"].value_counts(dropna=False).rename_axis("decision").reset_index(name="count")
    after_dec["share"] = after_dec["count"] / after_dec["count"].sum()
    after_dec.to_csv(OUTPUTS_REPORTS_DIR / "recommendation_decision_distribution_after.csv", index=False)

    before_comp_path = OUTPUTS_REPORTS_DIR / "recommendation_action_distribution_before.csv"
    before_dec_path = OUTPUTS_REPORTS_DIR / "recommendation_decision_distribution_before.csv"
    if not before_comp_path.exists():
        after_comp.to_csv(before_comp_path, index=False)
    if not before_dec_path.exists():
        after_dec.to_csv(before_dec_path, index=False)

    before_comp = pd.read_csv(before_comp_path)
    before_dec = pd.read_csv(before_dec_path)

    cmp_comp = before_comp.merge(
        after_comp[["action", "share"]].rename(columns={"share": "share_after"}),
        on="action",
        how="outer",
    ).rename(columns={"share": "share_before"})
    cmp_comp["share_before"] = cmp_comp["share_before"].fillna(0.0)
    cmp_comp["share_after"] = cmp_comp["share_after"].fillna(0.0)
    cmp_comp["delta_share"] = cmp_comp["share_after"] - cmp_comp["share_before"]
    cmp_comp.to_csv(OUTPUTS_REPORTS_DIR / "recommendation_action_distribution_before_after.csv", index=False)

    cmp_dec = before_dec.merge(
        after_dec[["decision", "share"]].rename(columns={"share": "share_after"}),
        on="decision",
        how="outer",
    ).rename(columns={"share": "share_before"})
    cmp_dec["share_before"] = cmp_dec["share_before"].fillna(0.0)
    cmp_dec["share_after"] = cmp_dec["share_after"].fillna(0.0)
    cmp_dec["delta_share"] = cmp_dec["share_after"] - cmp_dec["share_before"]
    cmp_dec.to_csv(OUTPUTS_REPORTS_DIR / "recommendation_decision_distribution_before_after.csv", index=False)

    fig, axes = plt.subplots(1, 2, figsize=(13.4, 4.8))
    comp_order = sorted(set(cmp_comp["action"]))
    dec_order = sorted(set(cmp_dec["decision"]))

    c_before = cmp_comp.set_index("action")["share_before"].to_dict()
    c_after = cmp_comp.set_index("action")["share_after"].to_dict()
    d_before = cmp_dec.set_index("decision")["share_before"].to_dict()
    d_after = cmp_dec.set_index("decision")["share_after"].to_dict()

    x0 = np.arange(len(comp_order))
    axes[0].bar(x0 - 0.18, [c_before.get(k, 0) for k in comp_order], width=0.35, label="before", color="#8d99ae")
    axes[0].bar(x0 + 0.18, [c_after.get(k, 0) for k in comp_order], width=0.35, label="after", color="#2a9d8f")
    axes[0].set_xticks(x0)
    axes[0].set_xticklabels(comp_order, rotation=35, ha="right")
    axes[0].set_ylim(0, 1)
    axes[0].set_title("Recommended Action Initial | Before vs After")
    axes[0].legend()

    x1 = np.arange(len(dec_order))
    axes[1].bar(x1 - 0.18, [d_before.get(k, 0) for k in dec_order], width=0.35, label="before", color="#8d99ae")
    axes[1].bar(x1 + 0.18, [d_after.get(k, 0) for k in dec_order], width=0.35, label="after", color="#e76f51")
    axes[1].set_xticks(x1)
    axes[1].set_xticklabels(dec_order, rotation=35, ha="right")
    axes[1].set_ylim(0, 1)
    axes[1].set_title("Decision Type | Before vs After")
    axes[1].legend()
    fig.tight_layout()
    fig.savefig(OUTPUTS_CHARTS_DIR / "16_recommendation_distribution_before_after.png", dpi=170, bbox_inches="tight")
    plt.close(fig)


def write_recommendation_logic_doc(examples_df: pd.DataFrame) -> None:
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUTS_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    rules_component = pd.DataFrame(
        [
            {"rule_id": "R01_inmediata", "accion": "intervencion_inmediata", "condicion": "riesgo muy alto + impacto alto + confianza suficiente"},
            {"rule_id": "R02_proxima_ventana", "accion": "intervencion_proxima_ventana", "condicion": "riesgo alto o RUL operativo estrecho sin condición de emergencia inmediata"},
            {"rule_id": "R03_inspeccion", "accion": "inspeccion_prioritaria", "condicion": "señal degradada con confianza baja o conflicto moderado"},
            {"rule_id": "R04_monitorizacion", "accion": "monitorizacion_intensiva", "condicion": "riesgo/interacción moderada sin gatillos de intervención"},
            {"rule_id": "R05_observacion", "accion": "mantener_bajo_observacion", "condicion": "riesgo bajo con salud aceptable"},
            {"rule_id": "R06_no_accion", "accion": "no_accion_por_ahora", "condicion": "riesgo muy bajo, salud alta, sin alertas ni backlog"},
            {"rule_id": "R07_escalado_conflicto", "accion": "escalado_tecnico_manual_review", "condicion": "conflicto señal-riesgo o riesgo alto con baja confianza"},
        ]
    )
    rules_component.to_csv(OUTPUTS_REPORTS_DIR / "recommendation_rules_component.csv", index=False)

    rules_operational = pd.DataFrame(
        [
            {"rule_id": "D01_inmediata", "decision": "intervención inmediata", "condicion": "urgencia alta + impacto operativo alto + capacidad disponible + confianza suficiente"},
            {"rule_id": "D02_proxima_ventana", "decision": "intervención en próxima ventana", "condicion": "riesgo alto o RUL corto en escenario no emergente"},
            {"rule_id": "D03_inspeccion", "decision": "inspección prioritaria", "condicion": "incertidumbre alta o degradación sin impacto operacional crítico"},
            {"rule_id": "D04_monitorizacion", "decision": "monitorización intensiva", "condicion": "riesgo medio con seguimiento reforzado"},
            {"rule_id": "D05_observacion", "decision": "mantener bajo observación", "condicion": "baja criticidad y ventana de seguridad amplia"},
            {"rule_id": "D06_no_accion", "decision": "no acción por ahora", "condicion": "riesgo mínimo y salud elevada"},
            {"rule_id": "D07_escalado_conflicto", "decision": "escalado técnico/manual review", "condicion": "riesgo alto con baja confianza o RUL crítico sin capacidad de taller"},
            {"rule_id": "D02B_inmediata_bloqueada_capacidad", "decision": "intervención en próxima ventana", "condicion": "urgencia alta pero bloqueo de capacidad/ventana"},
        ]
    )
    rules_operational.to_csv(OUTPUTS_REPORTS_DIR / "recommendation_rules_operational.csv", index=False)

    lines = [
        "# Recommendation Decision Logic",
        "",
        "## Objetivo",
        "Evitar colapso en una acción dominante y forzar una lógica jerárquica interpretable para operación ferroviaria.",
        "",
        "## Categorías de acción (componente)",
        "- intervencion_inmediata",
        "- intervencion_proxima_ventana",
        "- inspeccion_prioritaria",
        "- monitorizacion_intensiva",
        "- mantener_bajo_observacion",
        "- no_accion_por_ahora",
        "- escalado_tecnico_manual_review",
        "",
        "## Categorías de decisión (operación/taller)",
        "- intervención inmediata",
        "- intervención en próxima ventana",
        "- inspección prioritaria",
        "- monitorización intensiva",
        "- mantener bajo observación",
        "- no acción por ahora",
        "- escalado técnico/manual review",
        "",
        "## Tabla de reglas | Capa componente",
        rules_component.to_markdown(index=False),
        "",
        "## Tabla de reglas | Capa operativa",
        rules_operational.to_markdown(index=False),
        "",
        "## Resolución de conflictos",
        "- Riesgo alto + confianza baja: `escalado_tecnico_manual_review` / `escalado técnico/manual review`.",
        "- Degradación alta + impacto operacional bajo: `inspeccion_prioritaria`.",
        "- RUL bajo + ventana/capacidad inexistente: `intervención en próxima ventana` con marcador de bloqueo de capacidad.",
        "",
        "## Ejemplos sintéticos paso a paso",
    ]
    if not examples_df.empty:
        lines.extend(
            [
                "Los casos siguientes muestran input clave -> regla disparada -> decisión final:",
                examples_df.to_markdown(index=False),
            ]
        )
    else:
        lines.append("Sin ejemplos disponibles en ejecución actual.")

    (DOCS_DIR / "recommendation_decision_logic.md").write_text("\n".join(lines), encoding="utf-8")
