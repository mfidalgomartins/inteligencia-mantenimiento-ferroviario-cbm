from __future__ import annotations

import pandas as pd

from src.config import DATA_PROCESSED_DIR, OUTPUTS_REPORTS_DIR
from src.reporting_governance import load_or_compute_narrative_metrics


def run_advanced_analysis() -> str:
    fleet_week = pd.read_csv(DATA_PROCESSED_DIR / "fleet_week_features.csv")
    component_scores = pd.read_csv(DATA_PROCESSED_DIR / "scoring_componentes.csv")
    component_day = pd.read_csv(DATA_PROCESSED_DIR / "component_day_features.csv")
    unit_day = pd.read_csv(DATA_PROCESSED_DIR / "unit_day_features.csv")
    priorities = pd.read_csv(DATA_PROCESSED_DIR / "workshop_priority_table.csv")
    scheduling = pd.read_csv(DATA_PROCESSED_DIR / "workshop_scheduling_recommendation.csv")
    strategy = pd.read_csv(DATA_PROCESSED_DIR / "comparativo_estrategias.csv")
    inspection_value = pd.read_csv(DATA_PROCESSED_DIR / "inspection_module_value_comparison.csv")
    narrative_metrics = load_or_compute_narrative_metrics(force_recompute=False)

    fleet_week["week_start"] = pd.to_datetime(fleet_week["week_start"], errors="coerce")
    component_day["fecha"] = pd.to_datetime(component_day["fecha"], errors="coerce")
    unit_day["fecha"] = pd.to_datetime(unit_day["fecha"], errors="coerce")

    latest_day = component_day["fecha"].max()
    latest_unit = unit_day[unit_day["fecha"] == latest_day].copy()

    # Supporting tables
    support_top_units = (
        latest_unit.sort_values("predicted_unavailability_risk", ascending=False)
        .head(20)
    )
    support_top_components = component_scores.sort_values("riesgo_ajustado_negocio", ascending=False).head(30)
    support_backlog = priorities.sort_values("deferral_risk_score", ascending=False).head(30)

    OUTPUTS_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    support_top_units.to_csv(OUTPUTS_REPORTS_DIR / "analysis_top_units_risk.csv", index=False)
    support_top_components.to_csv(OUTPUTS_REPORTS_DIR / "analysis_top_components_risk.csv", index=False)
    support_backlog.to_csv(OUTPUTS_REPORTS_DIR / "analysis_backlog_priority.csv", index=False)

    availability_mean = float(fleet_week["availability_rate"].mean() * 100)
    availability_trend = float((fleet_week.sort_values("week_start").tail(8)["availability_rate"].mean() - fleet_week.sort_values("week_start").head(8)["availability_rate"].mean()) * 100)

    critical_families = (
        component_scores.groupby("component_family", as_index=False)
        .agg(
            risk_mean=("prob_fallo_30d", "mean"),
            health_mean=("health_score", "mean"),
        )
        .sort_values("risk_mean", ascending=False)
    )

    alert_precision = pd.read_csv(DATA_PROCESSED_DIR / "early_warning_practical_accuracy.csv").iloc[0]
    unresolved_states = {
        "pendiente_capacidad",
        "pendiente_repuesto",
        "pendiente_conflicto_operativo",
        "escalar_decision",
    }
    unresolved_sched = scheduling[scheduling["estado_intervencion"].isin(unresolved_states)].copy()
    top_depot = unresolved_sched["deposito_recomendado"].value_counts().head(1)
    top_depot_text = top_depot.index[0] if len(top_depot) else "sin cuello de botella dominante"

    cbm = strategy[strategy["estrategia"] == "basada_en_condicion"].iloc[0]
    react = strategy[strategy["estrategia"] == "reactiva"].iloc[0]

    inspection_delta = (
        inspection_value.loc[inspection_value["scenario"] == "sin_inspeccion_automatica", "horas_indisponibilidad_estimadas"].iloc[0]
        - inspection_value.loc[inspection_value["scenario"] == "con_inspeccion_automatica", "horas_indisponibilidad_estimadas"].iloc[0]
    )

    findings = pd.DataFrame(
        [
            {
                "prioridad": 1,
                "hallazgo": "Concentración de riesgo en subconjunto reducido de unidades/componentes",
                "evidencia": f"Top 20 unidades concentran {support_top_units['predicted_unavailability_risk'].sum()/latest_unit['predicted_unavailability_risk'].sum()*100:.1f}% del riesgo agregado",
                "implicacion": "Intervenir primero en cola priorizada reduce indisponibilidad marginal más rápido",
            },
            {
                "prioridad": 2,
                "hallazgo": "Familias de componente con degradación diferencial",
                "evidencia": f"Mayor riesgo medio en {critical_families.iloc[0]['component_family']}",
                "implicacion": "Ajustar umbrales y frecuencia de inspección por familia",
            },
            {
                "prioridad": 3,
                "hallazgo": "Cuello de botella de taller localizado",
                "evidencia": f"Depósito con más pendientes: {top_depot_text}",
                "implicacion": "Rebalanceo de carga y reasignación de entradas",
            },
            {
                "prioridad": 4,
                "hallazgo": "Valor tangible de inspección automática",
                "evidencia": f"Reducción estimada de {inspection_delta:.1f} h de indisponibilidad",
                "implicacion": "Escalar inspección en familias con mayor pre-failure detection rate y menor false alert proxy",
            },
            {
                "prioridad": 5,
                "hallazgo": "CBM supera estrategias tradicionales",
                "evidencia": f"CBM mejora disponibilidad en {cbm['fleet_availability'] - react['fleet_availability']:.2f} p.p.",
                "implicacion": "Priorizar transición progresiva a mantenimiento basado en condición",
            },
        ]
    )
    findings.to_csv(OUTPUTS_REPORTS_DIR / "hallazgos_priorizados.csv", index=False)

    report = [
        "# Informe Analítico Avanzado",
        "",
        "## Bloque Resumen SSOT",
        f"- Disponibilidad media (SSOT): {float(narrative_metrics['fleet_availability_pct']):.2f}%",
        f"- Unidad prioritaria (SSOT): {narrative_metrics['top_unit_by_priority']}",
        f"- Componente prioritario (SSOT): {narrative_metrics['top_component_by_priority']}",
        f"- Ahorro CBM vs reactiva (SSOT): {float(narrative_metrics['cbm_operational_savings_eur']):.0f} EUR",
        "",
        "## 1. Salud general de la flota",
        f"- Insight principal: disponibilidad media de flota en {availability_mean:.2f}% con tendencia reciente de {availability_trend:+.2f} p.p.",
        f"- Evidencia cuantitativa: MTBF medio {fleet_week['mtbf_proxy'].mean():.2f} | MTTR medio {fleet_week['mttr_proxy'].mean():.2f}",
        "- Lectura operativa: existe margen de mejora focalizando componentes de mayor degradación.",
        "- Lectura estratégica: disponibilidad se sostiene mejor con intervención anticipada y backlog controlado.",
        "- Caveats: métricas derivadas de datos sintéticos, no equivalentes a contrato de servicio real.",
        "- Recomendación: reforzar seguimiento semanal de riesgo por flota y depósito.",
        "",
        "## 2. Componentes y subsistemas críticos",
        f"- Insight principal: la familia con peor perfil de riesgo es {critical_families.iloc[0]['component_family']}.",
        f"- Evidencia cuantitativa: riesgo medio {critical_families.iloc[0]['risk_mean']:.3f} y salud media {critical_families.iloc[0]['health_mean']:.2f}.",
        "- Lectura operativa: concentración de fallas potenciales en pocas familias sugiere plan de acción específico.",
        "- Lectura estratégica: invertir en sensórica y reglas por familia mejora retorno de CBM.",
        "- Caveats: la segmentación depende de reglas de mapeo de familia.",
        "- Recomendación: calibrar thresholds por familia wheel/brake/bogie/pantograph.",
        "",
        "## 3. Fallo, degradación y alerta temprana",
        f"- Insight principal: precisión práctica de alerta temprana {alert_precision['precision']:.3f}, recall {alert_precision['recall']:.3f}.",
        "- Evidencia cuantitativa: la señal de degradación + backlog domina el driver de riesgo en top componentes.",
        "- Lectura operativa: las alertas son útiles para priorización, no para reemplazar inspección manual en casos de baja confianza.",
        "- Lectura estratégica: mantener la lógica interpretable facilita adopción por mantenimiento y operaciones.",
        "- Caveats: la precisión varía con umbral elegido y densidad de fallas.",
        "- Recomendación: revisión trimestral de reglas y performance por familia.",
        "",
        "## 4. Taller, backlog y priorización",
        f"- Insight principal: principal cuello de botella en {top_depot_text}.",
        (
            "- Evidencia cuantitativa: pendientes no ejecutables "
            f"{int(unresolved_sched.shape[0])} "
            f"(capacidad={int((scheduling['estado_intervencion']=='pendiente_capacidad').sum())}, "
            f"repuesto={int((scheduling['estado_intervencion']=='pendiente_repuesto').sum())}, "
            f"conflicto={int((scheduling['estado_intervencion']=='pendiente_conflicto_operativo').sum())}, "
            f"escalar={int((scheduling['estado_intervencion']=='escalar_decision').sum())})."
        ),
        "- Lectura operativa: secuencia recomendada reduce riesgo de diferimiento en componentes de mayor impacto.",
        "- Lectura estratégica: balancear especialización y saturación de depósitos eleva throughput útil.",
        "- Caveats: la heurística no optimiza globalmente, prioriza robustez y trazabilidad.",
        "- Recomendación: usar la cola sugerida como base diaria y ajustar por restricciones reales.",
        "",
        "## 5. Impacto operativo",
        f"- Insight principal: intervención temprana evita escalada no lineal de indisponibilidad al diferir acciones críticas.",
        f"- Evidencia cuantitativa: coste esperado y downtime crecen monotónicamente en escenarios de diferimiento.",
        "- Lectura operativa: actuar en ventana de 2-7 días para top riesgo reduce sustituciones y cancelaciones.",
        "- Lectura estratégica: el control de diferimiento es palanca directa de calidad de servicio.",
        "- Caveats: proxies económicos dependen de supuestos de coste unitario.",
        "- Recomendación: establecer política de diferimiento máximo por tier de riesgo.",
        "",
        "## 6. Implicaciones estratégicas",
        f"- Insight principal: CBM aporta mayor valor en familias críticas y depósitos con backlog persistente.",
        f"- Evidencia cuantitativa: ahorro operativo proxy CBM vs reactiva = {react['coste_operativo_proxy'] - cbm['coste_operativo_proxy']:.0f} EUR.",
        "- Lectura operativa: priorizar digitalización de inspección y reglas en familias con mayor detección pre-falla y menor tasa de falsa alerta.",
        "- Lectura estratégica: transición gradual a CBM con governance de datos y validación periódica.",
        "- Caveats: el business case debe recalibrarse con costos y SLA reales.",
        "- Recomendación: roadmap de 3 olas: pilotos, escalado por familia, despliegue multi-depósito.",
    ]

    out_path = OUTPUTS_REPORTS_DIR / "informe_analitico_avanzado.md"
    out_path.write_text("\n".join(report), encoding="utf-8")
    return str(out_path)


if __name__ == "__main__":
    run_advanced_analysis()
