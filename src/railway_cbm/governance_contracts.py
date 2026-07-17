"""Define y valida contratos de datos, métricas y linaje de publicación."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from railway_cbm.config import DATA_PROCESSED_DIR, DATA_RAW_DIR, DOCS_DIR


@dataclass(frozen=True)
class MetricContract:
    business_name: str
    technical_name: str
    definition: str
    formula: str
    valid_grain: str
    unit: str
    owner: str
    consumers: str
    aggregation_rule: str
    mandatory_tests: str
    anti_interpretation: str
    source_of_truth: str
    maturity: str


@dataclass(frozen=True)
class DataContract:
    table_name: str
    layer: str
    grain: str
    primary_key: str
    source_of_truth: str
    required_columns: str
    freshness_expectation: str
    owner: str
    consumers: str
    quality_rules: str
    notes: str


def _metric_contracts() -> list[MetricContract]:
    return [
        MetricContract(
            business_name="Salud de componente",
            technical_name="component_health_score",
            definition="Nivel de condición técnica actual del componente (alto=mejor).",
            formula="Puntuación ponderada de estimated_health_index, deterioration_index inverso, degradation_velocity inversa, defect_score inverso y maintenance_restoration_index.",
            valid_grain="componente_corte",
            unit="puntuacion_0_100",
            owner="Analítica de fiabilidad",
            consumers="Mantenimiento, Operaciones, Panel ejecutivo",
            aggregation_rule="No promediar sin contexto; para unidad usar media ponderada por criticidad.",
            mandatory_tests="range_0_100, monotonicidad_inversa_con_deterioration, cross_output_consistency",
            anti_interpretation="No usar como probabilidad de fallo; es estado, no riesgo.",
            source_of_truth="data/processed/scoring_componentes.csv",
            maturity="alta",
        ),
        MetricContract(
            business_name="Riesgo de fallo 30 días",
            technical_name="prob_fallo_30d",
            definition="Puntuación relativa de propensión a fallo a corto plazo por componente.",
            formula="Percentil de signal_risk con ajustes por familia, estrés y restauración.",
            valid_grain="componente_corte",
            unit="ratio_0_1",
            owner="Analítica de fiabilidad",
            consumers="Planificación Taller, Alerta temprana, Dirección Mantenimiento",
            aggregation_rule="Para unidad usar media + p90, no suma.",
            mandatory_tests="range_0_1, entropy_min, class_collapse_check, semantic_sign_consistency",
            anti_interpretation="No interpretar como probabilidad calibrada ni causalidad; requiere calibración temporal externa.",
            source_of_truth="data/processed/scoring_componentes.csv",
            maturity="media",
        ),
        MetricContract(
            business_name="Vida útil remanente estimada",
            technical_name="component_rul_estimate",
            definition="Días estimados hasta umbral crítico por familia bajo condiciones actuales.",
            formula="health_buffer / (effective_daily_damage * nonlinear_accel), con restablecimiento parcial y penalización repetitiva.",
            valid_grain="componente_corte",
            unit="dias",
            owner="Analítica de fiabilidad",
            consumers="Planificación Taller, Alerta temprana, Planificación",
            aggregation_rule="No promediar para cartera sin segmentar por familia/grupo.",
            mandatory_tests="distribution_spread, saturation_reduction_vs_legacy, family_discrimination, failure_linkage",
            anti_interpretation="No usar como fecha exacta de fallo; usar por grupos de intervención.",
            source_of_truth="data/processed/component_rul_estimate.csv",
            maturity="media",
        ),
        MetricContract(
            business_name="Pendientes físicos",
            technical_name="backlog_physical_items_count",
            definition="Pendientes reales abiertos de mantenimiento.",
            formula="count(*) en backlog_mantenimiento para última fecha disponible.",
            valid_grain="corte_deposito_y_global",
            unit="conteo",
            owner="Planificación de mantenimiento",
            consumers="Dirección Taller, Operaciones",
            aggregation_rule="Suma directa por depósito/región.",
            mandatory_tests="non_negative, temporal_consistency, overdue_le_physical",
            anti_interpretation="No confundir con riesgo de diferimiento; mide carga real, no severidad futura.",
            source_of_truth="data/raw/backlog_mantenimiento.csv",
            maturity="alta",
        ),
        MetricContract(
            business_name="Riesgo de diferimiento alto",
            technical_name="high_deferral_risk_cases_count",
            definition="Casos cuyo aplazamiento eleva riesgo operativo significativamente.",
            formula="count(deferral_risk_score >= 70) sobre workshop_priority_table.",
            valid_grain="componente_intervencion_corte",
            unit="conteo",
            owner="Oficina de decisión de mantenimiento",
            consumers="Dirección Mantenimiento y Operaciones",
            aggregation_rule="Suma por depósito/unidad/familia.",
            mandatory_tests="range_0_100_deferral_score, decision_consistency, backlog_semantic_separation",
            anti_interpretation="No equivale a pendientes críticos físicos.",
            source_of_truth="data/processed/workshop_priority_table.csv",
            maturity="alta",
        ),
        MetricContract(
            business_name="Disponibilidad de flota",
            technical_name="fleet_availability_pct",
            definition="Porcentaje de horas disponibles respecto a horas planificadas.",
            formula="mean(availability_rate)*100 sobre fleet_week_features.",
            valid_grain="flota_semana",
            unit="pct",
            owner="Analítica de operaciones",
            consumers="Dirección Operaciones, Dirección Servicio",
            aggregation_rule="Media simple por flota-semana.",
            mandatory_tests="range_0_100, denominator_non_zero, temporal_coverage_min",
            anti_interpretation="No comparar sin alinear periodo/filtro de servicio.",
            source_of_truth="data/processed/narrative_metrics_official.csv",
            maturity="alta",
        ),
        MetricContract(
            business_name="Diferencial operativo CBM vs reactiva",
            technical_name="cbm_operational_savings_eur",
            definition="Diferencia firmada de coste operativo aproximado entre estrategia reactiva y CBM.",
            formula="coste_operativo_proxy(reactiva) - coste_operativo_proxy(CBM).",
            valid_grain="estrategia_corte",
            unit="eur_aproximado",
            owner="Estrategia de mantenimiento + Analítica financiera",
            consumers="Comité de inversión, Dirección Mantenimiento",
            aggregation_rule="No agregar con otras métricas sin escenario explícito.",
            mandatory_tests="strategy_semantic_consistency, uncertainty_non_degenerate, value_range_monotonicity",
            anti_interpretation="Positivo indica ahorro y negativo coste incremental; no tratar como cuenta de resultados real.",
            source_of_truth="data/processed/comparativo_estrategias.csv",
            maturity="media",
        ),
        MetricContract(
            business_name="Impacto de diferimiento a 14 días",
            technical_name="deferral_cost_delta_14d_eur",
            definition="Incremento de coste aproximado al diferir 14 días respecto a intervención inmediata.",
            formula="costo_total_eur(14) - costo_total_eur(0) en impacto_diferimiento_resumen.",
            valid_grain="escenario_diferimiento",
            unit="eur_aproximado",
            owner="Economía de operaciones",
            consumers="Dirección Operaciones, Planificación Taller",
            aggregation_rule="Comparación puntual por escenario, no sumable transversalmente.",
            mandatory_tests="monotonicidad_costo_vs_dias, monotonicidad_downtime_vs_dias",
            anti_interpretation="No asumir linealidad fuera del rango simulado.",
            source_of_truth="data/processed/impacto_diferimiento_resumen.csv",
            maturity="media",
        ),
        MetricContract(
            business_name="Saturación de taller",
            technical_name="mean_depot_saturation_pct",
            definition="Uso relativo de capacidad de talleres en el corte más reciente.",
            formula="mean(saturation_ratio)*100 en vw_depot_maintenance_pressure (fecha max).",
            valid_grain="deposito_dia",
            unit="pct",
            owner="Planificación de taller",
            consumers="Dirección Mantenimiento, Planificación",
            aggregation_rule="Media simple por depósito en el corte.",
            mandatory_tests="non_negative, coherence_backlog_exposure, cap_overrun_sanity",
            anti_interpretation="No confundir con productividad técnica; mide tensión de capacidad.",
            source_of_truth="data/processed/narrative_metrics_official.csv",
            maturity="alta",
        ),
        MetricContract(
            business_name="Probabilidad calibrada de fallo a 30 días",
            technical_name="calibrated_probability_30d",
            definition="Probabilidad empírica calibrada con cortes temporales anteriores y resultados maduros.",
            formula="Tasa de fallo suavizada por bins de score, estimada sólo con cortes anteriores al holdout.",
            valid_grain="componente_corte_temporal",
            unit="ratio_0_1",
            owner="Gobierno de modelo",
            consumers="Monitorización, piloto en sombra",
            aggregation_rule="Evaluar por corte y familia; no mezclar periodos sin ponderar por cardinalidad.",
            mandatory_tests="temporal_no_leakage, brier, calibration_error, minimum_events",
            anti_interpretation="No usar operacionalmente si model_deployment_gate bloquea el modelo.",
            source_of_truth="data/processed/risk_temporal_validation.csv",
            maturity="baja",
        ),
        MetricContract(
            business_name="Permiso de uso autónomo",
            technical_name="autonomous_use_allowed",
            definition="Puerta compuesta que autoriza o bloquea el uso autónomo del modelo.",
            formula="AND de fuente externa, cortes maduros, eventos, ROC AUC, calibración y deriva PSI.",
            valid_grain="puerta_modelo_ejecucion",
            unit="booleano",
            owner="Gobierno de modelo",
            consumers="Registro de decisión, operaciones",
            aggregation_rule="No agregable; leer el estado de la ejecución vigente.",
            mandatory_tests="synthetic_blocked, thresholds_explicit, failed_checks_traceable",
            anti_interpretation="Un valor verdadero no sustituye aprobación humana ni autorización operacional.",
            source_of_truth="data/processed/model_deployment_gate.csv",
            maturity="alta",
        ),
    ]


def _data_contracts() -> list[DataContract]:
    return [
        DataContract(
            table_name="backlog_mantenimiento",
            layer="bruto_sintetico",
            grain="orden_pendiente_corte",
            primary_key="(fecha, backlog_id)",
            source_of_truth="data/raw/backlog_mantenimiento.csv",
            required_columns="fecha,backlog_id,deposito_id,unidad_id,componente_id,tipo_pendencia,antiguedad_backlog_dias,severidad_pendiente,riesgo_acumulado",
            freshness_expectation="por ejecución del flujo",
            owner="Planificación de mantenimiento",
            consumers="tablas analíticas SQL, estrategia, panel de control, informes",
            quality_rules="pk_unique; foreign_keys_valid; age_non_negative; risk_non_negative",
            notes="Corte semanal de órdenes pendientes identificadas por intervención.",
        ),
        DataContract(
            table_name="scoring_componentes",
            layer="procesado",
            grain="componente_corte",
            primary_key="(unidad_id, componente_id)",
            source_of_truth="data/processed/scoring_componentes.csv",
            required_columns="fecha,unidad_id,componente_id,component_health_score,prob_fallo_30d,confidence_flag,main_risk_driver,recommended_action_initial",
            freshness_expectation="por ejecución del flujo",
            owner="Analítica de fiabilidad",
            consumers="RUL, Recomendación, Panel de control, Informes",
            quality_rules="pk_unique; range_health_0_100; range_prob_0_1; action_taxonomy_valid",
            notes="Tabla de puntuación canónica a nivel componente.",
        ),
        DataContract(
            table_name="component_rul_estimate",
            layer="procesado",
            grain="componente_corte",
            primary_key="(unidad_id, componente_id)",
            source_of_truth="data/processed/component_rul_estimate.csv",
            required_columns="fecha_corte,unidad_id,componente_id,component_family,component_rul_estimate,confidence_rul,confidence_flag,rul_window_bucket",
            freshness_expectation="por ejecución del flujo",
            owner="Analítica de fiabilidad",
            consumers="Alerta temprana, Priorización de taller, Panel de control",
            quality_rules="pk_unique; rul_days_positive; confidence_0_1; bucket_valid",
            notes="RUL aproximado interpretable con perfiles por familia.",
        ),
        DataContract(
            table_name="workshop_priority_table",
            layer="procesado",
            grain="componente_intervencion_corte",
            primary_key="(unidad_id, componente_id)",
            source_of_truth="data/processed/workshop_priority_table.csv",
            required_columns="unidad_id,componente_id,intervention_priority_score,deferral_risk_score,service_impact_score,decision_type,deposito_recomendado,suggested_window_days",
            freshness_expectation="por ejecución del flujo",
            owner="Oficina de decisión de mantenimiento",
            consumers="Planificación, Panel de control, Memorando",
            quality_rules="pk_unique; scores_0_100; decision_taxonomy_valid; sequence_unique",
            notes="Fuente oficial de priorización operativa.",
        ),
        DataContract(
            table_name="comparativo_estrategias",
            layer="procesado",
            grain="estrategia_corte",
            primary_key="estrategia",
            source_of_truth="data/processed/comparativo_estrategias.csv",
            required_columns="estrategia,fleet_availability,coste_operativo_proxy,ahorro_neto_vs_reactiva,rango_plausible_valor_min,rango_plausible_valor_max,prob_ahorro_positivo",
            freshness_expectation="por ejecución del flujo",
            owner="Estrategia de mantenimiento + Analítica financiera",
            consumers="Panel estratégico, README, Memorando",
            quality_rules="pk_unique; strategy_set_complete; savings_semantics_consistent",
            notes="Comparativo con sensibilidad y rango de valor.",
        ),
        DataContract(
            table_name="impacto_diferimiento_resumen",
            layer="procesado",
            grain="escenario_diferimiento_dias",
            primary_key="defer_dias",
            source_of_truth="data/processed/impacto_diferimiento_resumen.csv",
            required_columns="defer_dias,costo_total_eur,downtime_total_h",
            freshness_expectation="por ejecución del flujo",
            owner="Economía de operaciones",
            consumers="Compensaciones del panel, Memorando",
            quality_rules="pk_unique; monotonic_cost_vs_defer; monotonic_downtime_vs_defer",
            notes="Base para compensación de aplazamiento.",
        ),
        DataContract(
            table_name="vw_depot_maintenance_pressure",
            layer="procesado_tabla_analitica_sql",
            grain="deposito_dia",
            primary_key="(fecha, deposito_id)",
            source_of_truth="data/processed/vw_depot_maintenance_pressure.csv",
            required_columns="fecha,deposito_id,saturation_ratio,backlog_physical_items,backlog_exposure_adjusted_score",
            freshness_expectation="por ejecución del flujo",
            owner="Planificación de taller",
            consumers="Panel de taller, Indicadores de informes",
            quality_rules="pk_unique; saturation_non_negative; exposure_0_100",
            notes="Tabla analítica oficial de presión de taller/deposito.",
        ),
        DataContract(
            table_name="narrative_metrics_official",
            layer="procesado_gobernanza",
            grain="metric_id",
            primary_key="metric_id",
            source_of_truth="data/processed/narrative_metrics_official.csv",
            required_columns="metric_id,metric_value,label,unit,source_of_truth,window_definition,aggregation_definition",
            freshness_expectation="por ejecución del flujo",
            owner="Gobernanza analítica",
            consumers="README, Memorando, Panel de control, Informes",
            quality_rules="pk_unique; source_of_truth_exists; metric_value_not_null",
            notes="Registro oficial de métricas narrativas.",
        ),
        DataContract(
            table_name="input_data_manifest",
            layer="procesado_gobernanza",
            grain="archivo_entrada_ejecucion",
            primary_key="file_name",
            source_of_truth="data/processed/input_data_manifest.csv",
            required_columns="source_mode,source_name,file_name,row_count,column_count,bytes,sha256",
            freshness_expectation="por ejecución del flujo",
            owner="Ingeniería analítica",
            consumers="Auditoría, reproducibilidad, operaciones",
            quality_rules="pk_unique; sha256_present; rows_positive",
            notes="Linaje técnico del snapshot efectivo.",
        ),
        DataContract(
            table_name="risk_temporal_validation",
            layer="procesado_modelo",
            grain="componente_corte_temporal",
            primary_key="(fecha, componente_id)",
            source_of_truth="data/processed/risk_temporal_validation.csv",
            required_columns="fecha,unidad_id,componente_id,component_family,risk_score,failure_in_30d,calibrated_probability_30d,calibration_status",
            freshness_expectation="por ejecución del flujo",
            owner="Analítica de fiabilidad",
            consumers="Gobierno de modelo, modo sombra",
            quality_rules="pk_unique; temporal_holdout; outcome_mature; no_future_leakage",
            notes="Validación mensual fuera de muestra con calibración rodante.",
        ),
        DataContract(
            table_name="model_deployment_gate",
            layer="procesado_gobernanza",
            grain="puerta_modelo",
            primary_key="gate_name",
            source_of_truth="data/processed/model_deployment_gate.csv",
            required_columns="gate_name,source_mode,autonomous_use_allowed,operating_mode,failed_checks",
            freshness_expectation="por ejecución del flujo",
            owner="Gobierno de modelo",
            consumers="Registro de decisión, operaciones",
            quality_rules="pk_unique; synthetic_never_autonomous; failed_checks_traceable",
            notes="Bloquea uso autónomo si la evidencia no es suficiente.",
        ),
        DataContract(
            table_name="capacity_optimization_gate",
            layer="procesado_gobernanza",
            grain="puerta_optimizacion_capacidad",
            primary_key="gate_name",
            source_of_truth="data/processed/capacity_optimization_gate.csv",
            required_columns="gate_name,formal_optimization_required,max_daily_utilization,saturated_depot_day_share,pending_capacity_cases,saturation_trigger,trigger_reason",
            freshness_expectation="por ejecución del flujo",
            owner="Planificación de taller",
            consumers="Optimización formal, decisión operativa",
            quality_rules="pk_unique; trigger_0_1; saturation_threshold_explicit",
            notes="Evita ejecutar optimización formal sin saturación material.",
        ),
        DataContract(
            table_name="decision_register",
            layer="procesado_gobernanza",
            grain="decision_componente_corte",
            primary_key="decision_id",
            source_of_truth="data/processed/decision_register.csv",
            required_columns="decision_id,fecha,unidad_id,componente_id,decision_type,decision_rule_id,approval_required,approval_status,operating_mode,auto_execution_allowed,execution_authorized",
            freshness_expectation="por ejecución del flujo",
            owner="Oficina de decisión de mantenimiento",
            consumers="Revisión humana, auditoría, planificación",
            quality_rules="pk_unique; shadow_mode_enforced; automatic_execution_disabled; approval_traceable",
            notes="Registro auditable; no es una orden de trabajo.",
        ),
    ]


def _metric_lineage_table() -> pd.DataFrame:
    rows = [
        (
            "health_score",
            "data/raw/sensores_componentes.csv + inspecciones_automaticas.csv + componentes_criticos.csv",
            "data/processed/component_day_features.csv",
            "data/processed/scoring_componentes.csv",
            "kpi_top_componentes_por_criticidad",
            "panel: Vista Salud",
        ),
        (
            "prob_fallo_30d",
            "data/raw/sensores_componentes.csv + fallas_historicas.csv",
            "data/processed/component_day_features.csv",
            "data/processed/scoring_componentes.csv",
            "kpi_top_unidades_por_riesgo",
            "panel: Vista Salud/Priorización",
        ),
        (
            "component_rul_estimate",
            "data/raw/sensores_componentes.csv + eventos_mantenimiento.csv + fallas_historicas.csv",
            "data/processed/component_day_features.csv",
            "data/processed/component_rul_estimate.csv",
            "rul_distribution_before_after",
            "panel: priorización + decisión ejecutiva",
        ),
        (
            "pendientes_fisicos",
            "data/raw/backlog_mantenimiento.csv",
            "tabla analítica SQL vw_depot_maintenance_pressure",
            "data/processed/narrative_metrics_official.csv",
            "kpi_backlog",
            "panel: indicadores + Vista Taller",
        ),
        (
            "high_deferral_risk_cases_count",
            "data/raw/intervenciones_programadas.csv + señales de puntuación",
            "data/processed/workshop_priority_features.csv",
            "data/processed/workshop_priority_table.csv",
            "kpi_backlog_critico",
            "panel: indicadores + Decisión Ejecutiva",
        ),
        (
            "fleet_availability_pct",
            "data/raw/disponibilidad_servicio.csv",
            "data/processed/fleet_week_features.csv",
            "data/processed/narrative_metrics_official.csv",
            "kpi_fleet_availability",
            "panel: Cabecera + indicadores",
        ),
        (
            "cbm_operational_savings_eur",
            "data/processed/fleet_week_features.csv + workshop_priority_table.csv + inspection_module_value_comparison.csv",
            "data/processed/comparativo_estrategias_sensibilidad.csv",
            "data/processed/comparativo_estrategias.csv",
            "comparativo_estrategias_escenarios",
            "panel: Vista Estratégica",
        ),
        (
            "deferral_cost_delta_14d_eur",
            "data/processed/workshop_priority_table.csv",
            "data/processed/impacto_diferimiento_resumen.csv",
            "data/processed/narrative_metrics_official.csv",
            "impacto_diferimiento_resumen",
            "panel: compensación de diferimiento",
        ),
        (
            "mean_depot_saturation_pct",
            "data/raw/depositos.csv + backlog_mantenimiento.csv + eventos_mantenimiento.csv",
            "sql/vw_depot_maintenance_pressure",
            "data/processed/narrative_metrics_official.csv",
            "kpi_depot_saturation",
            "panel: indicadores + Vista Taller",
        ),
        (
            "calibrated_probability_30d",
            "data/raw/fallas_historicas.csv + data/processed/component_day_features.csv",
            "data/processed/risk_temporal_validation.csv",
            "data/processed/risk_temporal_validation.csv",
            "risk_calibration.csv + risk_temporal_performance.csv",
            "gobierno de modelo + piloto en sombra",
        ),
        (
            "autonomous_use_allowed",
            "input_data_manifest.csv + risk_temporal_performance.csv + feature_drift_report.csv",
            "data/processed/model_readiness_assessment.csv",
            "data/processed/model_deployment_gate.csv",
            "decision_register.csv",
            "gobierno de decisión",
        ),
    ]
    return pd.DataFrame(
        rows,
        columns=[
            "metric_name",
            "raw_inputs",
            "feature_or_mart_layer",
            "source_of_truth",
            "intermediate_outputs",
            "final_consumption",
        ],
    )


def _run_contract_checks(metric_df: pd.DataFrame, data_df: pd.DataFrame) -> pd.DataFrame:
    checks: list[dict[str, object]] = []

    for row in metric_df.itertuples(index=False):
        source_path = row.source_of_truth
        exists = (
            (DATA_PROCESSED_DIR.parent / source_path.replace("data/", "")).exists()
            if source_path.startswith("data/")
            else False
        )
        if source_path.startswith("data/raw/"):
            exists = (DATA_RAW_DIR / source_path.split("data/raw/")[1]).exists()
        elif source_path.startswith("data/processed/"):
            exists = (DATA_PROCESSED_DIR / source_path.split("data/processed/")[1]).exists()
        checks.append(
            {
                "check_id": f"metric_contract_source_exists::{row.technical_name}",
                "contract_type": "metric",
                "severity": "alta",
                "passed": bool(exists),
                "detail": f"source={source_path}",
            }
        )

    for row in data_df.itertuples(index=False):
        source = str(row.source_of_truth)
        if source.startswith("data/raw/"):
            path = DATA_RAW_DIR / source.split("data/raw/")[1]
        elif source.startswith("data/processed/"):
            path = DATA_PROCESSED_DIR / source.split("data/processed/")[1]
        else:
            path = DATA_PROCESSED_DIR / source
        exists = path.exists()
        checks.append(
            {
                "check_id": f"data_contract_table_exists::{row.table_name}",
                "contract_type": "data",
                "severity": "critica",
                "passed": bool(exists),
                "detail": f"path={source}",
            }
        )
        if not exists:
            continue

        df = pd.read_csv(path)
        req_cols = [c.strip() for c in str(row.required_columns).split(",") if c.strip()]
        missing = [c for c in req_cols if c not in df.columns]
        checks.append(
            {
                "check_id": f"data_contract_required_columns::{row.table_name}",
                "contract_type": "data",
                "severity": "alta",
                "passed": len(missing) == 0,
                "detail": f"missing={missing}",
            }
        )

        pk_text = str(row.primary_key).strip()
        if pk_text.startswith("(") and pk_text.endswith(")"):
            pk_cols = [c.strip() for c in pk_text[1:-1].split(",") if c.strip()]
        else:
            pk_cols = [pk_text]
        if all(c in df.columns for c in pk_cols):
            dup = int(df.duplicated(subset=pk_cols).sum())
            checks.append(
                {
                    "check_id": f"data_contract_pk_uniqueness::{row.table_name}",
                    "contract_type": "data",
                    "severity": "alta",
                    "passed": dup == 0,
                    "detail": f"pk={pk_cols}, duplicates={dup}",
                }
            )
        else:
            checks.append(
                {
                    "check_id": f"data_contract_pk_uniqueness::{row.table_name}",
                    "contract_type": "data",
                    "severity": "alta",
                    "passed": False,
                    "detail": f"pk_missing_cols={pk_cols}",
                }
            )
    return pd.DataFrame(checks)


def _apply_blocker_policy(checks_df: pd.DataFrame) -> pd.DataFrame:
    out = checks_df.copy()
    if out.empty:
        out["publish_blocker"] = False
        return out

    blocker_patterns = (
        "metric_contract_source_exists::",
        "data_contract_table_exists::",
        "data_contract_required_columns::",
        "data_contract_pk_uniqueness::",
    )
    out["severity"] = out["severity"].astype(str).str.lower()
    out["publish_blocker"] = (
        (~out["passed"].astype(bool))
        & out["severity"].isin(["critica", "alta"])
        & out["check_id"].astype(str).str.startswith(blocker_patterns)
    )
    return out


def _assert_governance_compliance(checks_df: pd.DataFrame) -> None:
    if checks_df.empty:
        raise RuntimeError("Contratos de gobernanza: no se generaron controles.")
    blockers = checks_df[checks_df["publish_blocker"]].copy()
    if blockers.empty:
        return
    short = blockers[["check_id", "severity", "detail"]].head(10).to_dict(orient="records")
    raise RuntimeError(
        f"Bloqueos de contrato de gobernanza detectados ({len(blockers)}). Principales incidencias: {short}"
    )


def _to_markdown(df: pd.DataFrame) -> str:
    column_labels = {
        "business_name": "nombre_negocio",
        "technical_name": "nombre_tecnico",
        "definition": "definicion",
        "formula": "formula",
        "valid_grain": "grano_valido",
        "unit": "unidad",
        "owner": "responsable",
        "consumers": "consumidores",
        "aggregation_rule": "regla_agregacion",
        "mandatory_tests": "pruebas_obligatorias",
        "anti_interpretation": "anti_interpretacion",
        "source_of_truth": "fuente_de_verdad",
        "maturity": "madurez",
        "table_name": "nombre_tabla",
        "layer": "capa",
        "grain": "grano",
        "primary_key": "clave_primaria",
        "required_columns": "columnas_requeridas",
        "freshness_expectation": "expectativa_actualizacion",
        "quality_rules": "reglas_calidad",
        "notes": "notas",
        "metric_name": "nombre_metrica",
        "raw_inputs": "entradas_brutas",
        "feature_or_mart_layer": "capa_variables_o_tabla_analitica",
        "intermediate_outputs": "salidas_intermedias",
        "final_consumption": "consumo_final",
    }
    return df.rename(columns=column_labels).to_markdown(index=False)


def run_governance_contracts(*, fail_on_blocker: bool = False) -> dict[str, str]:
    """Materializa contratos y bloquea la publicación si se solicita."""
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    metric_df = (
        pd.DataFrame([c.__dict__ for c in _metric_contracts()]).sort_values("technical_name").reset_index(drop=True)
    )
    data_df = pd.DataFrame([c.__dict__ for c in _data_contracts()]).sort_values("table_name").reset_index(drop=True)
    lineage_df = _metric_lineage_table()
    checks_df = _apply_blocker_policy(_run_contract_checks(metric_df, data_df))

    metric_df.to_csv(DATA_PROCESSED_DIR / "metric_contract_registry.csv", index=False)
    data_df.to_csv(DATA_PROCESSED_DIR / "data_contract_registry.csv", index=False)
    lineage_df.to_csv(DATA_PROCESSED_DIR / "metric_lineage_registry.csv", index=False)
    checks_df.to_csv(DATA_PROCESSED_DIR / "governance_contract_checks.csv", index=False)

    governance_doc = "\n".join(
        [
            "# Gobierno de Métricas y Datos",
            "",
            "## Objetivo",
            "Consolidar contratos y trazabilidad en un único documento de referencia.",
            "",
            "## Contratos de Métrica",
            _to_markdown(metric_df),
            "",
            "## Contratos de Datos",
            _to_markdown(data_df),
            "",
            "## Linaje por Métrica",
            _to_markdown(lineage_df),
            "",
            "## Fuentes oficiales críticas",
            "- `health_score`, `prob_fallo_30d`: `data/processed/scoring_componentes.csv`",
            "- `component_rul_estimate`: `data/processed/component_rul_estimate.csv`",
            "- pendientes físicos: `data/raw/backlog_mantenimiento.csv`",
            "- `high_deferral_risk_cases_count`: `data/processed/workshop_priority_table.csv`",
            "- `fleet_availability_pct`: `data/processed/narrative_metrics_official.csv`",
            "- `cbm_operational_savings_eur`: `data/processed/comparativo_estrategias.csv`",
            "- `deferral_cost_delta_14d_eur`: `data/processed/impacto_diferimiento_resumen.csv`",
            "- `mean_depot_saturation_pct`: `data/processed/narrative_metrics_official.csv` + `vw_depot_maintenance_pressure.csv`",
            "- `calibrated_probability_30d`: `data/processed/risk_temporal_validation.csv`",
            "- `autonomous_use_allowed`: `data/processed/model_deployment_gate.csv`",
            "",
            "## Métricas no maduras (declaración explícita)",
            "- `cbm_operational_savings_eur`: madurez media (aproximación económica).",
            "- `deferral_cost_delta_14d_eur`: madurez media (sensible a supuestos de coste).",
            "- `component_rul_estimate`: madurez media (aproximación interpretable, no modelo físico de fabricante).",
            "- `prob_fallo_30d`: madurez media (score relativo pendiente de calibración externa).",
            "- `calibrated_probability_30d`: madurez baja mientras la puerta de modelo permanezca bloqueada.",
            "",
            "## Reglas de gobernanza",
            "- Sin PK única validada, la tabla no puede considerarse fuente oficial.",
            "- No se permite usar tabla derivada en el panel sin contrato activo.",
            "- Si falta columna requerida, el artefacto queda en estado `no conforme`.",
        ]
    )

    (DOCS_DIR / "gobierno_metricas.md").write_text(governance_doc + "\n", encoding="utf-8")

    if fail_on_blocker:
        _assert_governance_compliance(checks_df)

    return {
        "governance_doc": str(DOCS_DIR / "gobierno_metricas.md"),
        "checks": str(DATA_PROCESSED_DIR / "governance_contract_checks.csv"),
    }


if __name__ == "__main__":
    run_governance_contracts()
