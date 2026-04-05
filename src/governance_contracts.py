from __future__ import annotations

from dataclasses import dataclass
import pandas as pd

from src.config import DATA_PROCESSED_DIR, DATA_RAW_DIR, DOCS_DIR, OUTPUTS_REPORTS_DIR


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
            formula="Weighted score de estimated_health_index, deterioration_index inverso, degradation_velocity inversa, defect_score inverso y maintenance_restoration_index.",
            valid_grain="componente_snapshot",
            unit="score_0_100",
            owner="Reliability Analytics",
            consumers="Mantenimiento, Operaciones, Dashboard Ejecutivo",
            aggregation_rule="No promediar sin contexto; para unidad usar media ponderada por criticidad.",
            mandatory_tests="range_0_100, monotonicidad_inversa_con_deterioration, cross_output_consistency",
            anti_interpretation="No usar como probabilidad de fallo; es estado, no riesgo.",
            source_of_truth="data/processed/scoring_componentes.csv",
            maturity="alta",
        ),
        MetricContract(
            business_name="Riesgo de fallo 30 días",
            technical_name="prob_fallo_30d",
            definition="Probabilidad proxy de fallo a corto plazo por componente.",
            formula="Calibrated percentile de signal_risk con ajustes por familia, estrés y restauración.",
            valid_grain="componente_snapshot",
            unit="ratio_0_1",
            owner="Reliability Analytics",
            consumers="Planificación Taller, Early Warning, Dirección Mantenimiento",
            aggregation_rule="Para unidad usar media + p90, no suma.",
            mandatory_tests="range_0_1, entropy_min, class_collapse_check, semantic_sign_consistency",
            anti_interpretation="No interpretar causalidad directa; es score de propensión, no modelo físico causal.",
            source_of_truth="data/processed/scoring_componentes.csv",
            maturity="alta",
        ),
        MetricContract(
            business_name="Vida útil remanente estimada",
            technical_name="component_rul_estimate",
            definition="Días estimados hasta umbral crítico por familia bajo condiciones actuales.",
            formula="health_buffer / (effective_daily_damage * nonlinear_accel), con reset parcial y penalización repetitiva.",
            valid_grain="componente_snapshot",
            unit="days",
            owner="Reliability Analytics",
            consumers="Planificación Taller, Early Warning, Scheduling",
            aggregation_rule="No promediar para cartera sin segmentar por familia/bucket.",
            mandatory_tests="distribution_spread, saturation_reduction_vs_legacy, family_discrimination, failure_linkage",
            anti_interpretation="No usar como fecha exacta de fallo; usar por buckets de intervención.",
            source_of_truth="data/processed/component_rul_estimate.csv",
            maturity="media",
        ),
        MetricContract(
            business_name="Backlog físico",
            technical_name="backlog_physical_items_count",
            definition="Pendientes reales abiertos de mantenimiento.",
            formula="count(*) en backlog_mantenimiento para última fecha disponible.",
            valid_grain="snapshot_deposito_y_global",
            unit="count",
            owner="Maintenance Planning",
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
            valid_grain="componente_intervencion_snapshot",
            unit="count",
            owner="Maintenance Decision Office",
            consumers="Dirección Mantenimiento y Operaciones",
            aggregation_rule="Suma por depósito/unidad/familia.",
            mandatory_tests="range_0_100_deferral_score, decision_consistency, backlog_semantic_separation",
            anti_interpretation="No equivale a backlog crítico físico.",
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
            owner="Operations Analytics",
            consumers="Dirección Operaciones, Dirección Servicio",
            aggregation_rule="Media ponderada por horas planificadas.",
            mandatory_tests="range_0_100, denominator_non_zero, temporal_coverage_min",
            anti_interpretation="No comparar sin alinear periodo/filtro de servicio.",
            source_of_truth="data/processed/narrative_metrics_official.csv",
            maturity="alta",
        ),
        MetricContract(
            business_name="Ahorro operativo CBM vs reactiva",
            technical_name="cbm_operational_savings_eur",
            definition="Diferencia de coste operativo proxy entre estrategia reactiva y CBM.",
            formula="coste_operativo_proxy(reactiva) - coste_operativo_proxy(CBM).",
            valid_grain="estrategia_snapshot",
            unit="eur_proxy",
            owner="Maintenance Strategy + Finance Analytics",
            consumers="Comité de inversión, Dirección Mantenimiento",
            aggregation_rule="No agregar con otras métricas sin escenario explícito.",
            mandatory_tests="strategy_semantic_consistency, uncertainty_non_degenerate, value_range_monotonicity",
            anti_interpretation="No tratar como P&L real; es proxy económico dependiente de supuestos.",
            source_of_truth="data/processed/comparativo_estrategias.csv",
            maturity="media",
        ),
        MetricContract(
            business_name="Impacto de diferimiento a 14 días",
            technical_name="deferral_cost_delta_14d_eur",
            definition="Incremento de coste proxy al diferir 14 días respecto a intervención inmediata.",
            formula="costo_total_eur(14) - costo_total_eur(0) en impacto_diferimiento_resumen.",
            valid_grain="escenario_diferimiento",
            unit="eur_proxy",
            owner="Operations Economics",
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
            definition="Uso relativo de capacidad de talleres en snapshot más reciente.",
            formula="mean(saturation_ratio)*100 en vw_depot_maintenance_pressure (fecha max).",
            valid_grain="deposito_dia",
            unit="pct",
            owner="Workshop Planning",
            consumers="Dirección Mantenimiento, Scheduling",
            aggregation_rule="Media por depósito; global ponderada por capacidad.",
            mandatory_tests="non_negative, coherence_backlog_exposure, cap_overrun_sanity",
            anti_interpretation="No confundir con productividad técnica; mide tensión de capacidad.",
            source_of_truth="data/processed/narrative_metrics_official.csv",
            maturity="alta",
        ),
    ]


def _data_contracts() -> list[DataContract]:
    return [
        DataContract(
            table_name="scoring_componentes",
            layer="processed",
            grain="componente_snapshot",
            primary_key="(unidad_id, componente_id)",
            source_of_truth="data/processed/scoring_componentes.csv",
            required_columns="fecha,unidad_id,componente_id,component_health_score,prob_fallo_30d,confidence_flag,main_risk_driver,recommended_action_initial",
            freshness_expectation="por corrida pipeline",
            owner="Reliability Analytics",
            consumers="RUL, Recomendación, Dashboard, Reporting",
            quality_rules="pk_unique; range_health_0_100; range_prob_0_1; action_taxonomy_valid",
            notes="Tabla de score canónica a nivel componente.",
        ),
        DataContract(
            table_name="component_rul_estimate",
            layer="processed",
            grain="componente_snapshot",
            primary_key="(unidad_id, componente_id)",
            source_of_truth="data/processed/component_rul_estimate.csv",
            required_columns="fecha_corte,unidad_id,componente_id,component_family,component_rul_estimate,confidence_rul,confidence_flag,rul_window_bucket",
            freshness_expectation="por corrida pipeline",
            owner="Reliability Analytics",
            consumers="Early Warning, Workshop Prioritization, Dashboard",
            quality_rules="pk_unique; rul_days_positive; confidence_0_1; bucket_valid",
            notes="RUL proxy interpretable con perfiles por familia.",
        ),
        DataContract(
            table_name="workshop_priority_table",
            layer="processed",
            grain="componente_intervencion_snapshot",
            primary_key="(unidad_id, componente_id)",
            source_of_truth="data/processed/workshop_priority_table.csv",
            required_columns="unidad_id,componente_id,intervention_priority_score,deferral_risk_score,service_impact_score,decision_type,deposito_recomendado,suggested_window_days",
            freshness_expectation="por corrida pipeline",
            owner="Maintenance Decision Office",
            consumers="Scheduling, Dashboard, Memo",
            quality_rules="pk_unique; scores_0_100; decision_taxonomy_valid; sequence_unique",
            notes="Fuente oficial de priorización operativa.",
        ),
        DataContract(
            table_name="comparativo_estrategias",
            layer="processed",
            grain="estrategia_snapshot",
            primary_key="estrategia",
            source_of_truth="data/processed/comparativo_estrategias.csv",
            required_columns="estrategia,fleet_availability,coste_operativo_proxy,ahorro_neto_vs_reactiva,rango_plausible_valor_min,rango_plausible_valor_max,prob_ahorro_positivo",
            freshness_expectation="por corrida pipeline",
            owner="Maintenance Strategy + Finance Analytics",
            consumers="Dashboard estratégico, README, Memo",
            quality_rules="pk_unique; strategy_set_complete; savings_semantics_consistent",
            notes="Comparativo robusto con sensibilidad y rango de valor.",
        ),
        DataContract(
            table_name="impacto_diferimiento_resumen",
            layer="processed",
            grain="escenario_diferimiento_dias",
            primary_key="defer_dias",
            source_of_truth="data/processed/impacto_diferimiento_resumen.csv",
            required_columns="defer_dias,costo_total_eur,downtime_total_h",
            freshness_expectation="por corrida pipeline",
            owner="Operations Economics",
            consumers="Dashboard trade-offs, Memo",
            quality_rules="pk_unique; monotonic_cost_vs_defer; monotonic_downtime_vs_defer",
            notes="Base para trade-off de aplazamiento.",
        ),
        DataContract(
            table_name="vw_depot_maintenance_pressure",
            layer="processed_sql_mart",
            grain="deposito_dia",
            primary_key="(fecha, deposito_id)",
            source_of_truth="data/processed/vw_depot_maintenance_pressure.csv",
            required_columns="fecha,deposito_id,saturation_ratio,backlog_physical_items,backlog_exposure_adjusted_score",
            freshness_expectation="por corrida pipeline",
            owner="Workshop Planning",
            consumers="Dashboard taller, Reporting KPIs",
            quality_rules="pk_unique; saturation_non_negative; exposure_0_100",
            notes="Mart oficial de presión de taller/deposito.",
        ),
        DataContract(
            table_name="narrative_metrics_official",
            layer="processed_governance",
            grain="metric_id",
            primary_key="metric_id",
            source_of_truth="data/processed/narrative_metrics_official.csv",
            required_columns="metric_id,metric_value,label,unit,source_of_truth,window_definition,aggregation_definition",
            freshness_expectation="por corrida pipeline",
            owner="Analytics Governance",
            consumers="README, Memo, Dashboard, Reports",
            quality_rules="pk_unique; source_of_truth_exists; metric_value_not_null",
            notes="Single Source of Truth narrativo.",
        ),
    ]


def _metric_lineage_table() -> pd.DataFrame:
    rows = [
        ("health_score", "data/raw/sensores_componentes.csv + inspecciones_automaticas.csv + componentes_criticos.csv", "data/processed/component_day_features.csv", "data/processed/scoring_componentes.csv", "kpi_top_componentes_por_criticidad", "dashboard: Vista Salud"),
        ("prob_fallo_30d", "data/raw/sensores_componentes.csv + fallas_historicas.csv", "data/processed/component_day_features.csv", "data/processed/scoring_componentes.csv", "kpi_top_unidades_por_riesgo", "dashboard: Vista Salud/Priorización"),
        ("component_rul_estimate", "data/raw/sensores_componentes.csv + eventos_mantenimiento.csv + fallas_historicas.csv", "data/processed/component_day_features.csv", "data/processed/component_rul_estimate.csv", "rul_distribution_before_after", "dashboard: priorización + decisión ejecutiva"),
        ("backlog_fisico", "data/raw/backlog_mantenimiento.csv", "sql/mart vw_depot_maintenance_pressure", "data/processed/narrative_metrics_official.csv", "kpi_backlog", "dashboard: KPI cards + Vista Taller"),
        ("high_deferral_risk_cases_count", "data/raw/intervenciones_programadas.csv + señales score", "data/processed/workshop_priority_features.csv", "data/processed/workshop_priority_table.csv", "kpi_backlog_critico", "dashboard: KPI cards + Decisión Ejecutiva"),
        ("fleet_availability_pct", "data/raw/disponibilidad_servicio.csv", "data/processed/fleet_week_features.csv", "data/processed/narrative_metrics_official.csv", "kpi_fleet_availability", "dashboard: Header + KPI cards"),
        ("cbm_operational_savings_eur", "data/processed/fleet_week_features.csv + workshop_priority_table.csv + inspection_module_value_comparison.csv", "data/processed/comparativo_estrategias_sensibilidad.csv", "data/processed/comparativo_estrategias.csv", "comparativo_estrategias_escenarios", "dashboard: Vista Estratégica"),
        ("deferral_cost_delta_14d_eur", "data/processed/workshop_priority_table.csv", "data/processed/impacto_diferimiento_resumen.csv", "data/processed/narrative_metrics_official.csv", "impacto_diferimiento_resumen", "dashboard: Trade-off diferimiento"),
        ("mean_depot_saturation_pct", "data/raw/depositos.csv + backlog_mantenimiento.csv + eventos_mantenimiento.csv", "sql/vw_depot_maintenance_pressure", "data/processed/narrative_metrics_official.csv", "kpi_depot_saturation", "dashboard: KPI cards + Vista Taller"),
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
        exists = (DATA_PROCESSED_DIR.parent / source_path.replace("data/", "")).exists() if source_path.startswith("data/") else False
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
                "detail": f"path={path}",
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
        raise RuntimeError("Governance contracts: no checks produced.")
    blockers = checks_df[checks_df["publish_blocker"]].copy()
    if blockers.empty:
        return
    short = blockers[["check_id", "severity", "detail"]].head(10).to_dict(orient="records")
    raise RuntimeError(f"Governance contract blockers detected ({len(blockers)}). Top issues: {short}")


def _to_markdown(df: pd.DataFrame) -> str:
    return df.to_markdown(index=False)


def run_governance_contracts(*, fail_on_blocker: bool = False) -> dict[str, str]:
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUTS_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    metric_df = pd.DataFrame([c.__dict__ for c in _metric_contracts()]).sort_values("technical_name").reset_index(drop=True)
    data_df = pd.DataFrame([c.__dict__ for c in _data_contracts()]).sort_values("table_name").reset_index(drop=True)
    lineage_df = _metric_lineage_table()
    checks_df = _apply_blocker_policy(_run_contract_checks(metric_df, data_df))

    metric_df.to_csv(OUTPUTS_REPORTS_DIR / "metric_contract_registry.csv", index=False)
    data_df.to_csv(OUTPUTS_REPORTS_DIR / "data_contract_registry.csv", index=False)
    lineage_df.to_csv(OUTPUTS_REPORTS_DIR / "metric_lineage_registry.csv", index=False)
    checks_df.to_csv(OUTPUTS_REPORTS_DIR / "governance_contract_checks.csv", index=False)
    checks_df.to_csv(DATA_PROCESSED_DIR / "governance_contract_checks.csv", index=False)
    blocker_df = checks_df[checks_df["publish_blocker"]].copy()
    blocker_df.to_csv(OUTPUTS_REPORTS_DIR / "governance_contract_blockers.csv", index=False)

    metric_doc = "\n".join(
        [
            "# Metric Contracts",
            "",
            "## Objetivo",
            "Contratos formales para evitar drift semántico entre SQL, Python, dashboard y narrativa.",
            "",
            "## Contratos de Métrica",
            _to_markdown(metric_df),
            "",
            "## Source of Truth Crítico",
            "- `health_score`, `prob_fallo_30d`: `data/processed/scoring_componentes.csv`",
            "- `component_rul_estimate`: `data/processed/component_rul_estimate.csv`",
            "- backlog físico: `data/raw/backlog_mantenimiento.csv`",
            "- `high_deferral_risk_cases_count`: `data/processed/workshop_priority_table.csv`",
            "- `fleet_availability_pct`: `data/processed/narrative_metrics_official.csv`",
            "- `cbm_operational_savings_eur`: `data/processed/comparativo_estrategias.csv`",
            "- `deferral_cost_delta_14d_eur`: `data/processed/impacto_diferimiento_resumen.csv`",
            "- `mean_depot_saturation_pct`: `data/processed/narrative_metrics_official.csv` + `vw_depot_maintenance_pressure.csv`",
            "",
            "## Métricas no maduras (declaración explícita)",
            "- `cbm_operational_savings_eur`: madurez media (proxy económico).",
            "- `deferral_cost_delta_14d_eur`: madurez media (sensible a supuestos de coste).",
            "- `component_rul_estimate`: madurez media (proxy interpretable, no modelo físico de fabricante).",
        ]
    )
    data_doc = "\n".join(
        [
            "# Data Contracts",
            "",
            "## Objetivo",
            "Definir contratos de estructura, grain, PK y reglas de calidad para tablas críticas.",
            "",
            "## Contratos de Tabla",
            _to_markdown(data_df),
            "",
            "## Reglas de Governanza",
            "- Sin PK única validada, la tabla no puede ser source-of-truth.",
            "- No se permite usar tabla derivada en dashboard sin contrato activo.",
            "- Si falta columna requerida, el artefacto queda en estado `non-compliant`.",
        ]
    )
    lineage_doc = "\n".join(
        [
            "# Metric Lineage",
            "",
            "## Objetivo",
            "Trazabilidad lógica desde raw hasta decisión ejecutiva.",
            "",
            "## Lineage por Métrica",
            _to_markdown(lineage_df),
            "",
            "## Ownership por Output",
            "- Reliability Analytics: scoring, RUL, risk drivers.",
            "- Workshop Planning: priorización/scheduling, saturación, backlog operativo.",
            "- Operations Analytics: disponibilidad e impacto en servicio.",
            "- Finance Analytics (proxy): comparativo CBM y diferimiento.",
            "- Analytics Governance: SSOT narrativo y contratos.",
        ]
    )

    (DOCS_DIR / "metric_contracts.md").write_text(metric_doc, encoding="utf-8")
    (DOCS_DIR / "data_contracts.md").write_text(data_doc, encoding="utf-8")
    (DOCS_DIR / "metric_lineage.md").write_text(lineage_doc, encoding="utf-8")

    summary = [
        "# Governance Contract Report",
        "",
        f"- checks_total: {len(checks_df)}",
        f"- checks_failed: {int((~checks_df['passed']).sum())}",
        f"- blockers: {int(checks_df['publish_blocker'].sum())}",
        "",
        "## Failed checks (top 20)",
    ]
    failed = checks_df[~checks_df["passed"]][["check_id", "severity", "detail", "publish_blocker"]].head(20)
    if failed.empty:
        summary.append("- none")
    else:
        summary.append(failed.to_markdown(index=False))
    (OUTPUTS_REPORTS_DIR / "governance_contract_report.md").write_text("\n".join(summary), encoding="utf-8")

    if fail_on_blocker:
        _assert_governance_compliance(checks_df)

    return {
        "metric_contracts": str(DOCS_DIR / "metric_contracts.md"),
        "data_contracts": str(DOCS_DIR / "data_contracts.md"),
        "metric_lineage": str(DOCS_DIR / "metric_lineage.md"),
        "checks": str(DATA_PROCESSED_DIR / "governance_contract_checks.csv"),
    }


if __name__ == "__main__":
    run_governance_contracts()
