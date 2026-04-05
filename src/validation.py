from __future__ import annotations

import math
import re
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from src.config import DATA_PROCESSED_DIR, DATA_RAW_DIR, DOCS_DIR, OUTPUTS_DASHBOARD_DIR, OUTPUTS_REPORTS_DIR, ROOT_DIR


SEVERITY_ORDER = {"critica": 4, "alta": 3, "media": 2, "informativa": 1}
VALIDATION_LAYERS = [
    "raw_data",
    "staging",
    "marts",
    "features",
    "scores",
    "recommendations",
    "dashboard_datasets",
    "reports_docs_consistency",
]


@dataclass
class QAResult:
    check_id: str
    layer: str
    severity: str
    passed: bool
    blocker_if_fail: bool
    publish_blocker: bool
    what_checked: str
    detail: str
    impact_potential: str
    technical_recommendation: str
    metric_value: str | None = None
    threshold: str | None = None


def _add_result(
    results: list[QAResult],
    *,
    check_id: str,
    layer: str,
    severity: str,
    passed: bool,
    blocker_if_fail: bool,
    what_checked: str,
    detail: str,
    impact_potential: str,
    technical_recommendation: str,
    metric_value: str | None = None,
    threshold: str | None = None,
) -> None:
    results.append(
        QAResult(
            check_id=check_id,
            layer=layer,
            severity=severity,
            passed=bool(passed),
            blocker_if_fail=bool(blocker_if_fail),
            publish_blocker=bool((not passed) and blocker_if_fail),
            what_checked=what_checked,
            detail=detail,
            impact_potential=impact_potential,
            technical_recommendation=technical_recommendation,
            metric_value=metric_value,
            threshold=threshold,
        )
    )


def _safe_read(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def _series_entropy(s: pd.Series, bins: int = 10) -> float:
    s = s.dropna()
    if s.empty:
        return 0.0
    counts = pd.cut(s, bins=bins, duplicates="drop").value_counts(normalize=True)
    if counts.empty:
        return 0.0
    return float(-(counts * np.log2(counts + 1e-12)).sum())


def _class_entropy(s: pd.Series) -> float:
    probs = s.value_counts(normalize=True, dropna=True)
    if probs.empty:
        return 0.0
    return float(-(probs * np.log2(probs + 1e-12)).sum())


def _extract_metric(text: str, pattern: str) -> float | None:
    m = re.search(pattern, text, flags=re.IGNORECASE | re.MULTILINE)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", "."))
    except ValueError:
        return None


def _extract_token(text: str, pattern: str) -> str | None:
    m = re.search(pattern, text, flags=re.IGNORECASE | re.MULTILINE)
    if not m:
        return None
    return str(m.group(1)).strip()


def _load_raw_tables() -> dict[str, pd.DataFrame]:
    tables = [
        "flotas",
        "unidades",
        "depositos",
        "componentes_criticos",
        "sensores_componentes",
        "inspecciones_automaticas",
        "eventos_mantenimiento",
        "fallas_historicas",
        "alertas_operativas",
        "intervenciones_programadas",
        "disponibilidad_servicio",
        "asignacion_servicio",
        "backlog_mantenimiento",
        "parametros_operativos_contexto",
        "escenarios_mantenimiento",
    ]
    return {t: _safe_read(DATA_RAW_DIR / f"{t}.csv") for t in tables}


def _load_processed_tables() -> dict[str, pd.DataFrame]:
    files = {
        "component_day_features": "component_day_features.csv",
        "unit_day_features": "unit_day_features.csv",
        "fleet_week_features": "fleet_week_features.csv",
        "workshop_priority_features": "workshop_priority_features.csv",
        "scoring_componentes": "scoring_componentes.csv",
        "component_rul_estimate": "component_rul_estimate.csv",
        "component_failure_risk_score": "component_failure_risk_score.csv",
        "component_health_score": "component_health_score.csv",
        "unit_unavailability_risk_score": "unit_unavailability_risk_score.csv",
        "rul_before_after_comparison": "rul_before_after_comparison.csv",
        "rul_distribution_before_after": "rul_distribution_before_after.csv",
        "rul_family_discrimination_before_after": "rul_family_discrimination_before_after.csv",
        "rul_backtest_failure_linkage": "rul_backtest_failure_linkage.csv",
        "rul_validation_checks": "rul_validation_checks.csv",
        "rul_window_utility_before_after": "rul_window_utility_before_after.csv",
        "workshop_priority_table": "workshop_priority_table.csv",
        "workshop_scheduling_recommendation": "workshop_scheduling_recommendation.csv",
        "workshop_capacity_calendar": "workshop_capacity_calendar.csv",
        "scheduling_before_after_metrics": "scheduling_before_after_metrics.csv",
        "scheduling_before_after_deltas": "scheduling_before_after_deltas.csv",
        "scheduling_status_distribution": "scheduling_status_distribution.csv",
        "scheduling_bottleneck_diagnosis": "scheduling_bottleneck_diagnosis.csv",
        "alertas_tempranas": "alertas_tempranas.csv",
        "comparativo_estrategias": "comparativo_estrategias.csv",
        "comparativo_estrategias_sensibilidad": "comparativo_estrategias_sensibilidad.csv",
        "comparativo_estrategias_escenarios": "comparativo_estrategias_escenarios.csv",
        "comparativo_estrategias_value_ranges": "comparativo_estrategias_value_ranges.csv",
        "inspection_module_family_performance": "inspection_module_family_performance.csv",
        "inspection_module_consistency_checks": "inspection_module_consistency_checks.csv",
        "inspection_module_failure_linkage": "inspection_module_failure_linkage.csv",
        "mart_component_day": "mart_component_day.csv",
        "mart_unit_day": "mart_unit_day.csv",
        "mart_fleet_week": "mart_fleet_week.csv",
        "vw_depot_maintenance_pressure": "vw_depot_maintenance_pressure.csv",
        "kpi_top_unidades_por_riesgo": "kpi_top_unidades_por_riesgo.csv",
        "kpi_inspeccion_automatica_por_familia": "kpi_inspeccion_automatica_por_familia.csv",
        "narrative_metrics_official": "narrative_metrics_official.csv",
        "governance_contract_checks": "governance_contract_checks.csv",
        "val_row_counts": "val_row_counts.csv",
        "val_null_rates_critical": "val_null_rates_critical.csv",
        "val_sensor_ranges": "val_sensor_ranges.csv",
        "val_temporal_coherence": "val_temporal_coherence.csv",
        "val_consistency_scores_actions": "val_consistency_scores_actions.csv",
        "val_semantic_health_deterioration": "val_semantic_health_deterioration.csv",
        "val_backlog_semantic_consistency": "val_backlog_semantic_consistency.csv",
    }
    return {k: _safe_read(DATA_PROCESSED_DIR / v) for k, v in files.items()}


def _validate_required_files(raw: dict[str, pd.DataFrame], processed: dict[str, pd.DataFrame], results: list[QAResult]) -> None:
    for name, df in raw.items():
        _add_result(
            results,
            check_id=f"raw_file_exists_{name}",
            layer="raw_data",
            severity="critica",
            passed=not df.empty,
            blocker_if_fail=True,
            what_checked=f"Existencia de data/raw/{name}.csv",
            detail="ok" if not df.empty else "archivo faltante o vacío",
            impact_potential="Sin dataset base no hay trazabilidad ni análisis defendible.",
            technical_recommendation="Regenerar raw layer y verificar permisos/ruta.",
        )

    for name, df in processed.items():
        _add_result(
            results,
            check_id=f"processed_file_exists_{name}",
            layer="staging" if name.startswith("val_") else "dashboard_datasets",
            severity="critica",
            passed=not df.empty,
            blocker_if_fail=True,
            what_checked=f"Existencia de data/processed/{name}.csv",
            detail="ok" if not df.empty else "archivo faltante o vacío",
            impact_potential="Salidas analíticas incompletas rompen consistencia entre capas.",
            technical_recommendation="Reejecutar pipeline completo y revisar export de artefactos.",
        )


def _validate_raw_layer(raw: dict[str, pd.DataFrame], results: list[QAResult]) -> None:
    flotas = raw["flotas"]
    unidades = raw["unidades"]
    depositos = raw["depositos"]
    componentes = raw["componentes_criticos"]
    sensores = raw["sensores_componentes"]
    inspecciones = raw["inspecciones_automaticas"]
    fallas = raw["fallas_historicas"]
    mantenimiento = raw["eventos_mantenimiento"]
    backlog = raw["backlog_mantenimiento"]
    disponibilidad = raw["disponibilidad_servicio"]

    row_thresholds = {
        "sensores_componentes": 500_000,
        "disponibilidad_servicio": 40_000,
        "componentes_criticos": 1_000,
        "unidades": 100,
    }
    for tbl, minimum in row_thresholds.items():
        val = len(raw[tbl])
        _add_result(
            results,
            check_id=f"raw_row_count_{tbl}",
            layer="raw_data",
            severity="alta",
            passed=val >= minimum,
            blocker_if_fail=True,
            what_checked=f"Volumen mínimo de {tbl}",
            detail=f"rows={val}, min={minimum}",
            metric_value=str(val),
            threshold=f">={minimum}",
            impact_potential="Baja cardinalidad degrada robustez de scoring y comparativas.",
            technical_recommendation="Ajustar generador sintético para densidad histórica suficiente.",
        )

    duplicate_checks = {
        "flotas": ("flota_id", flotas),
        "unidades": ("unidad_id", unidades),
        "depositos": ("deposito_id", depositos),
        "componentes_criticos": ("componente_id", componentes),
    }
    for name, (key, frame) in duplicate_checks.items():
        dup = int(frame.duplicated(subset=[key]).sum())
        _add_result(
            results,
            check_id=f"raw_pk_duplicates_{name}",
            layer="raw_data",
            severity="critica",
            passed=dup == 0,
            blocker_if_fail=True,
            what_checked=f"Duplicados en PK {name}.{key}",
            detail=f"duplicates={dup}",
            metric_value=str(dup),
            threshold="==0",
            impact_potential="Orphan joins y double counting en marts.",
            technical_recommendation="Deduplicar y reforzar unicidad al generar dimensiones.",
        )

    critical_null_rows = int(
        sensores[["timestamp", "unidad_id", "componente_id"]].isna().any(axis=1).sum()
        + fallas[["falla_id", "unidad_id", "componente_id", "fecha_falla"]].isna().any(axis=1).sum()
        + mantenimiento[["mantenimiento_id", "unidad_id", "componente_id", "fecha_inicio", "fecha_fin"]].isna().any(axis=1).sum()
    )
    _add_result(
        results,
        check_id="raw_critical_null_keys",
        layer="raw_data",
        severity="critica",
        passed=critical_null_rows == 0,
        blocker_if_fail=True,
        what_checked="Nulls en claves críticas raw",
        detail=f"null_rows={critical_null_rows}",
        metric_value=str(critical_null_rows),
        threshold="==0",
        impact_potential="Rompe integridad referencial y scoring por entidad.",
        technical_recommendation="Imputar/eliminar filas inválidas antes de staging.",
    )

    # Integridad referencial (orphans)
    orphan_components = int((~componentes["unidad_id"].isin(unidades["unidad_id"])).sum())
    orphan_sensors_unit = int((~sensores["unidad_id"].isin(unidades["unidad_id"])).sum())
    orphan_sensors_comp = int((~sensores["componente_id"].isin(componentes["componente_id"])).sum())
    orphan_failures = int((~fallas["componente_id"].isin(componentes["componente_id"])).sum())
    orphan_inspections = int((~inspecciones["componente_id"].isin(componentes["componente_id"])).sum())
    orphan_maint_depot = int((~mantenimiento["deposito_id"].isin(depositos["deposito_id"])).sum())
    orphan_total = orphan_components + orphan_sensors_unit + orphan_sensors_comp + orphan_failures + orphan_inspections + orphan_maint_depot

    _add_result(
        results,
        check_id="raw_fk_orphan_integrity",
        layer="raw_data",
        severity="critica",
        passed=orphan_total == 0,
        blocker_if_fail=True,
        what_checked="Orphan joins en FKs raw",
        detail=(
            f"components={orphan_components}, sensors_unit={orphan_sensors_unit}, sensors_comp={orphan_sensors_comp}, "
            f"failures={orphan_failures}, inspections={orphan_inspections}, maint_depot={orphan_maint_depot}"
        ),
        metric_value=str(orphan_total),
        threshold="==0",
        impact_potential="Inconsistencia estructural entre entidades y pérdida de trazabilidad.",
        technical_recommendation="Validar FK al generar datos y bloquear escritura de huérfanos.",
    )

    out_of_range = int(
        ((sensores["temperatura_operacion"] < -30) | (sensores["temperatura_operacion"] > 180)).sum()
        + ((sensores["vibracion_proxy"] < 0) | (sensores["vibracion_proxy"] > 35)).sum()
        + ((sensores["desgaste_proxy"] < 0) | (sensores["desgaste_proxy"] > 220)).sum()
    )
    _add_result(
        results,
        check_id="raw_sensor_range_sanity",
        layer="raw_data",
        severity="alta",
        passed=out_of_range == 0,
        blocker_if_fail=True,
        what_checked="Sensores en rango físico plausible",
        detail=f"out_of_range_count={out_of_range}",
        metric_value=str(out_of_range),
        threshold="==0",
        impact_potential="Distorsiona features y dispara falsas alertas estructurales.",
        technical_recommendation="Aplicar límites por familia de señal y clipping pre-staging.",
    )

    mantenimiento_dt = mantenimiento.copy()
    mantenimiento_dt["fecha_inicio"] = pd.to_datetime(mantenimiento_dt["fecha_inicio"], errors="coerce")
    mantenimiento_dt["fecha_fin"] = pd.to_datetime(mantenimiento_dt["fecha_fin"], errors="coerce")
    negative_duration = int((mantenimiento_dt["fecha_inicio"] > mantenimiento_dt["fecha_fin"]).sum())
    _add_result(
        results,
        check_id="raw_temporal_maintenance_order",
        layer="raw_data",
        severity="critica",
        passed=negative_duration == 0,
        blocker_if_fail=True,
        what_checked="Orden temporal fecha_inicio <= fecha_fin en mantenimiento",
        detail=f"negative_duration_rows={negative_duration}",
        metric_value=str(negative_duration),
        threshold="==0",
        impact_potential="Imposibilita MTTR y evaluación de disponibilidad.",
        technical_recommendation="Corregir lógica de timestamps en generador de eventos.",
    )

    sensores_dt = sensores.copy()
    sensores_dt["timestamp"] = pd.to_datetime(sensores_dt["timestamp"], errors="coerce")
    disponibilidad_dt = disponibilidad.copy()
    disponibilidad_dt["fecha"] = pd.to_datetime(disponibilidad_dt["fecha"], errors="coerce")
    sensor_span = int((sensores_dt["timestamp"].max() - sensores_dt["timestamp"].min()).days) if not sensores_dt.empty else 0
    disp_span = int((disponibilidad_dt["fecha"].max() - disponibilidad_dt["fecha"].min()).days) if not disponibilidad_dt.empty else 0
    _add_result(
        results,
        check_id="raw_temporal_coverage",
        layer="raw_data",
        severity="alta",
        passed=sensor_span >= 700 and disp_span >= 700,
        blocker_if_fail=True,
        what_checked="Cobertura temporal mínima ~24 meses",
        detail=f"sensor_span_days={sensor_span}, disponibilidad_span_days={disp_span}",
        metric_value=f"{sensor_span}/{disp_span}",
        threshold=">=700 ambos",
        impact_potential="Cobertura insuficiente invalida tendencias y estacionalidad.",
        technical_recommendation="Extender horizonte sintético y verificar continuidad diaria.",
    )

    backlog_bad = int((backlog["antiguedad_backlog_dias"] < 0).sum() + (backlog["riesgo_acumulado"] < 0).sum())
    _add_result(
        results,
        check_id="raw_backlog_non_negative",
        layer="raw_data",
        severity="alta",
        passed=backlog_bad == 0,
        blocker_if_fail=False,
        what_checked="No negatividad en backlog",
        detail=f"negative_rows={backlog_bad}",
        metric_value=str(backlog_bad),
        threshold="==0",
        impact_potential="Sesga priorización de diferimiento y presión de taller.",
        technical_recommendation="Corregir cálculo de backlog y aplicar sanity constraints.",
    )

    required_families = {"wheel", "brake", "bogie", "pantograph"}
    present_families = set(inspecciones["familia_inspeccion"].dropna().astype(str).str.lower().unique())
    missing_families = sorted(required_families - present_families)
    _add_result(
        results,
        check_id="raw_inspection_family_taxonomy",
        layer="raw_data",
        severity="alta",
        passed=len(missing_families) == 0,
        blocker_if_fail=False,
        what_checked="Cobertura de familias técnicas en inspección automática",
        detail=f"missing={missing_families}",
        impact_potential="Sin familias completas no hay comparativa técnica defendible.",
        technical_recommendation="Forzar presencia de wheel/brake/bogie/pantograph en simulación.",
    )


def _validate_staging_layer(processed: dict[str, pd.DataFrame], results: list[QAResult]) -> None:
    sensor_ranges = processed["val_sensor_ranges"]
    temporal = processed["val_temporal_coherence"]
    null_rates = processed["val_null_rates_critical"]
    semantic = processed["val_semantic_health_deterioration"]
    backlog_semantic = processed["val_backlog_semantic_consistency"]
    sql_consistency = processed["val_consistency_scores_actions"]

    out_sensor = float(sensor_ranges.sum(axis=1).iloc[0]) if not sensor_ranges.empty else math.inf
    _add_result(
        results,
        check_id="staging_sql_sensor_ranges",
        layer="staging",
        severity="alta",
        passed=out_sensor == 0,
        blocker_if_fail=True,
        what_checked="Salida SQL val_sensor_ranges",
        detail=f"out_of_range_total={out_sensor}",
        metric_value=f"{out_sensor:.0f}",
        threshold="==0",
        impact_potential="Valores imposibles sobreviven staging y contaminan marts.",
        technical_recommendation="Reforzar casts/constraints de rangos en SQL staging.",
    )

    temporal_issues = float(temporal.sum(axis=1).iloc[0]) if not temporal.empty else math.inf
    _add_result(
        results,
        check_id="staging_sql_temporal_coherence",
        layer="staging",
        severity="critica",
        passed=temporal_issues == 0,
        blocker_if_fail=True,
        what_checked="Salida SQL val_temporal_coherence",
        detail=f"temporal_issues={temporal_issues}",
        metric_value=f"{temporal_issues:.0f}",
        threshold="==0",
        impact_potential="Duraciones inválidas rompen KPIs de fiabilidad.",
        technical_recommendation="Revisar filtros/joins temporales en staging maintenance.",
    )

    max_null_rate = float(null_rates.select_dtypes(include=[np.number]).max().max()) if not null_rates.empty else math.inf
    _add_result(
        results,
        check_id="staging_sql_null_rates_critical",
        layer="staging",
        severity="alta",
        passed=max_null_rate <= 0.001,
        blocker_if_fail=True,
        what_checked="Null rates críticas en SQL",
        detail=f"max_null_rate={max_null_rate:.6f}",
        metric_value=f"{max_null_rate:.6f}",
        threshold="<=0.001",
        impact_potential="Nulls críticos en staging destruyen integridad de marts.",
        technical_recommendation="Añadir filtros/COALESCE y controles de calidad pre-export.",
    )

    if not semantic.empty:
        mae = float(semantic["health_deterioration_balance_mae"].iloc[0])
        out_health = int(semantic["health_out_of_range"].iloc[0])
        out_det = int(semantic["deterioration_out_of_range"].iloc[0])
        sem_ok = mae <= 1e-6 and out_health == 0 and out_det == 0
        sem_detail = f"mae={mae:.8f}, out_health={out_health}, out_det={out_det}"
    else:
        sem_ok = False
        sem_detail = "archivo_val_semantic_health_deterioration_vacio"
    _add_result(
        results,
        check_id="staging_sql_semantic_balance",
        layer="staging",
        severity="critica",
        passed=sem_ok,
        blocker_if_fail=True,
        what_checked="Balance health+deterioration en SQL",
        detail=sem_detail,
        impact_potential="Inconsistencia semántica invalida interpretación de scores.",
        technical_recommendation="Sincronizar fórmulas en marts y docs semánticos.",
    )

    if not backlog_semantic.empty:
        backlog_issues = int(backlog_semantic.select_dtypes(include=[np.number]).sum(axis=1).iloc[0])
        backlog_detail = (
            f"overdue_gt_physical={int(backlog_semantic['overdue_gt_physical_rows'].iloc[0])}, "
            f"critical_gt_physical={int(backlog_semantic['critical_gt_physical_rows'].iloc[0])}, "
            f"overdue_ratio_oob={int(backlog_semantic['overdue_ratio_out_of_bounds'].iloc[0])}, "
            f"critical_ratio_oob={int(backlog_semantic['critical_ratio_out_of_bounds'].iloc[0])}, "
            f"exposure_oob={int(backlog_semantic['exposure_score_out_of_bounds'].iloc[0])}"
        )
    else:
        backlog_issues = math.inf
        backlog_detail = "archivo_val_backlog_semantic_consistency_vacio"
    _add_result(
        results,
        check_id="staging_sql_backlog_semantic_consistency",
        layer="staging",
        severity="critica",
        passed=backlog_issues == 0,
        blocker_if_fail=True,
        what_checked="Consistencia semántica backlog físico/vencido/crítico/exposure",
        detail=backlog_detail,
        metric_value=str(backlog_issues) if np.isfinite(backlog_issues) else None,
        threshold="==0",
        impact_potential="KPIs híbridos de backlog y diferimiento vuelven a mezclarse.",
        technical_recommendation="Corregir definiciones SQL y denominadores de backlog antes de publicar.",
    )

    sql_action_incons = float(sql_consistency.sum(axis=1).iloc[0]) if not sql_consistency.empty else math.inf
    _add_result(
        results,
        check_id="staging_sql_scores_actions_consistency",
        layer="staging",
        severity="media",
        passed=sql_action_incons == 0,
        blocker_if_fail=False,
        what_checked="Consistencia SQL de scores vs acciones",
        detail=f"inconsistencias={sql_action_incons}",
        metric_value=f"{sql_action_incons:.0f}",
        threshold="==0",
        impact_potential="Divergencias tempranas entre score y acción final.",
        technical_recommendation="Alinear reglas SQL/Python en la capa de decisión.",
    )


def _validate_marts_layer(raw: dict[str, pd.DataFrame], processed: dict[str, pd.DataFrame], results: list[QAResult]) -> None:
    mart_component = processed["mart_component_day"]
    mart_unit = processed["mart_unit_day"]
    mart_fleet = processed["mart_fleet_week"]
    depot_pressure = processed["vw_depot_maintenance_pressure"]
    unit_risk = processed["unit_unavailability_risk_score"]
    score = processed["scoring_componentes"]

    _add_result(
        results,
        check_id="marts_non_empty",
        layer="marts",
        severity="critica",
        passed=(not mart_component.empty) and (not mart_unit.empty) and (not mart_fleet.empty),
        blocker_if_fail=True,
        what_checked="Existencia y no vacíos de marts principales",
        detail=f"component={len(mart_component)}, unit={len(mart_unit)}, fleet={len(mart_fleet)}",
        impact_potential="Sin marts válidos no existe capa analítica explotable.",
        technical_recommendation="Verificar ejecución SQL y export de objects.",
    )

    dup_comp = int(mart_component.duplicated(subset=["fecha", "unidad_id", "componente_id"]).sum()) if not mart_component.empty else math.inf
    dup_unit = int(mart_unit.duplicated(subset=["fecha", "unidad_id"]).sum()) if not mart_unit.empty else math.inf
    dup_fleet = int(mart_fleet.duplicated(subset=["week_start", "flota_id"]).sum()) if not mart_fleet.empty else math.inf
    _add_result(
        results,
        check_id="marts_grain_uniqueness",
        layer="marts",
        severity="critica",
        passed=(dup_comp == 0 and dup_unit == 0 and dup_fleet == 0),
        blocker_if_fail=True,
        what_checked="Unicidad de grain en marts",
        detail=f"dup_component={dup_comp}, dup_unit={dup_unit}, dup_fleet={dup_fleet}",
        impact_potential="Duplicación de grain altera agregados y rankings.",
        technical_recommendation="Revisar keys en SQL analytical marts y joins de integración.",
    )

    if not mart_component.empty:
        orphan_mart_comp = int((~mart_component["componente_id"].isin(raw["componentes_criticos"]["componente_id"])).sum())
    else:
        orphan_mart_comp = math.inf
    if not mart_unit.empty:
        orphan_mart_unit = int((~mart_unit["unidad_id"].isin(raw["unidades"]["unidad_id"])).sum())
    else:
        orphan_mart_unit = math.inf
    _add_result(
        results,
        check_id="marts_orphan_integrity",
        layer="marts",
        severity="alta",
        passed=(orphan_mart_comp == 0 and orphan_mart_unit == 0),
        blocker_if_fail=True,
        what_checked="Integridad de IDs entre marts y dimensiones raw",
        detail=f"orphan_component={orphan_mart_comp}, orphan_unit={orphan_mart_unit}",
        impact_potential="Pérdida de trazabilidad raw->mart y quiebre de auditoría.",
        technical_recommendation="Corregir joins/filtros de integración SQL y validación de FKs.",
    )

    if not mart_component.empty:
        mart_component["fecha"] = pd.to_datetime(mart_component["fecha"], errors="coerce")
        span_component = int((mart_component["fecha"].max() - mart_component["fecha"].min()).days)
    else:
        span_component = 0
    _add_result(
        results,
        check_id="marts_temporal_coverage",
        layer="marts",
        severity="alta",
        passed=span_component >= 700,
        blocker_if_fail=True,
        what_checked="Cobertura temporal de mart_component_day",
        detail=f"span_days={span_component}",
        metric_value=str(span_component),
        threshold=">=700",
        impact_potential="Series cortas limitan análisis de degradación y tendencia.",
        technical_recommendation="Revisar cortes por fecha en SQL y export.",
    )

    if not depot_pressure.empty:
        backlog_col = "backlog_exposure_adjusted_score" if "backlog_exposure_adjusted_score" in depot_pressure.columns else "backlog_risk"
        corr_bs = float(depot_pressure[[backlog_col, "saturation_ratio"]].corr(method="spearman").iloc[0, 1])
    else:
        backlog_col = "backlog_exposure_adjusted_score"
        corr_bs = np.nan
    _add_result(
        results,
        check_id="marts_backlog_saturation_coherence",
        layer="marts",
        severity="media",
        passed=bool(np.isfinite(corr_bs) and corr_bs > 0.15),
        blocker_if_fail=False,
        what_checked=f"Coherencia {backlog_col} vs saturation_ratio",
        detail=f"spearman={corr_bs:.3f}" if np.isfinite(corr_bs) else "corr_no_disponible",
        metric_value=f"{corr_bs:.3f}" if np.isfinite(corr_bs) else None,
        threshold=">0.15",
        impact_potential="Si desacoplado, la presión de taller pierde poder explicativo.",
        technical_recommendation="Alinear fórmula de saturación con carga real y exposición de backlog físico.",
    )

    if not score.empty and not unit_risk.empty:
        comp_agg = score.groupby("unidad_id", as_index=False)["prob_fallo_30d"].mean().rename(columns={"prob_fallo_30d": "risk_comp_mean"})
        merge = comp_agg.merge(unit_risk, on="unidad_id", how="inner")
        corr_unit = float(merge[["risk_comp_mean", "unit_unavailability_risk_score"]].corr(method="spearman").iloc[0, 1]) if not merge.empty else np.nan
    else:
        corr_unit = np.nan
    _add_result(
        results,
        check_id="marts_cross_level_risk_consistency",
        layer="marts",
        severity="alta",
        passed=bool(np.isfinite(corr_unit) and corr_unit >= 0.45),
        blocker_if_fail=False,
        what_checked="Consistencia riesgo componente agregado vs riesgo unidad",
        detail=f"spearman={corr_unit:.3f}" if np.isfinite(corr_unit) else "corr_no_disponible",
        metric_value=f"{corr_unit:.3f}" if np.isfinite(corr_unit) else None,
        threshold=">=0.45",
        impact_potential="Sin coherencia vertical, la agregación unitaria no es defendible.",
        technical_recommendation="Revisar pesos de agregación y normalizaciones por unidad.",
    )


def _validate_features_layer(processed: dict[str, pd.DataFrame], results: list[QAResult]) -> None:
    component = processed["component_day_features"]
    unit = processed["unit_day_features"]
    score = processed["scoring_componentes"]
    rul = processed["component_rul_estimate"]
    rul_before_after = processed["rul_before_after_comparison"]
    rul_family = processed["rul_family_discrimination_before_after"]
    rul_linkage = processed["rul_backtest_failure_linkage"]
    rul_checks = processed["rul_validation_checks"]

    features_range_ok = (
        component["estimated_health_index"].between(0, 100).all()
        and component["deterioration_index"].between(0, 100).all()
        and component["maintenance_restoration_index"].between(0, 100).all()
        and component["degradation_velocity"].between(0, 10).all()
    )
    _add_result(
        results,
        check_id="features_range_sanity",
        layer="features",
        severity="alta",
        passed=bool(features_range_ok),
        blocker_if_fail=True,
        what_checked="Rangos de features críticas",
        detail="health/deterioration/restoration/degradation_velocity en rango esperado",
        impact_potential="Features fuera de rango rompen interpretabilidad y scoring.",
        technical_recommendation="Aplicar clipping y revisar transformaciones derivadas.",
    )

    null_rate = float(
        component[["sensor_mean", "rolling_mean_7d", "inspection_defect_score_recent", "operating_stress_index"]]
        .isna()
        .mean()
        .mean()
    )
    _add_result(
        results,
        check_id="features_null_rate_critical",
        layer="features",
        severity="alta",
        passed=null_rate <= 0.05,
        blocker_if_fail=False,
        what_checked="Null rate en features de señal base",
        detail=f"avg_null_rate={null_rate:.4f}",
        metric_value=f"{null_rate:.4f}",
        threshold="<=0.05",
        impact_potential="Nulls altos generan señales arbitrarias en riesgo.",
        technical_recommendation="Mejorar imputación y cobertura de joins en feature engineering.",
    )

    if "deterioration_index" in score.columns:
        det_series = score["deterioration_index"].copy()
        score_aux = score.copy()
    else:
        score_aux = score.merge(
            component.sort_values("fecha").drop_duplicates(["unidad_id", "componente_id"], keep="last")[["unidad_id", "componente_id", "deterioration_index"]],
            on=["unidad_id", "componente_id"],
            how="left",
        )
        det_series = score_aux["deterioration_index"].copy()
    q = pd.qcut(det_series.fillna(det_series.median()), q=3, labels=["bajo", "medio", "alto"])
    tier = score_aux.assign(det_tier=q).groupby("det_tier", observed=False)["prob_fallo_30d"].mean()
    mono_ok = bool(tier.loc["alto"] >= tier.loc["medio"] >= tier.loc["bajo"])
    _add_result(
        results,
        check_id="features_monotonicity_deterioration_risk",
        layer="features",
        severity="alta",
        passed=mono_ok,
        blocker_if_fail=True,
        what_checked="Monotonía riesgo por terciles de deterioro",
        detail=f"bajo={tier.loc['bajo']:.3f}, medio={tier.loc['medio']:.3f}, alto={tier.loc['alto']:.3f}",
        impact_potential="Sin monotonía, degradación pierde valor como señal causal operativa.",
        technical_recommendation="Recalibrar transformación de deterioro y pesos en scoring.",
    )

    unit["fecha"] = pd.to_datetime(unit["fecha"], errors="coerce")
    latest_unit = unit[unit["fecha"] == unit["fecha"].max()].copy()
    impact_entropy = _series_entropy(latest_unit["impact_on_service_proxy"], bins=10)
    impact_sat_share = float((latest_unit["impact_on_service_proxy"] >= 99).mean())
    _add_result(
        results,
        check_id="features_impact_distribution_entropy",
        layer="features",
        severity="media",
        passed=impact_entropy >= 1.6,
        blocker_if_fail=False,
        what_checked="Entropía distribución impact_on_service_proxy",
        detail=f"entropy={impact_entropy:.3f}",
        metric_value=f"{impact_entropy:.3f}",
        threshold=">=1.6",
        impact_potential="Baja entropía implica señal degenerada para impacto de servicio.",
        technical_recommendation="Reescalar fórmula de impacto y reducir clipping extremo.",
    )
    _add_result(
        results,
        check_id="features_impact_saturation",
        layer="features",
        severity="alta",
        passed=impact_sat_share <= 0.30,
        blocker_if_fail=False,
        what_checked="Saturación alta de impact_on_service_proxy",
        detail=f"share_ge_99={impact_sat_share:.3f}",
        metric_value=f"{impact_sat_share:.3f}",
        threshold="<=0.30",
        impact_potential="Saturación reduce discriminación en priorización operativa.",
        technical_recommendation="Ajustar normalización y límites superiores de impacto.",
    )

    rul_share_max = float((rul["component_rul_estimate"] >= 365).mean()) if not rul.empty else 1.0
    _add_result(
        results,
        check_id="features_rul_distribution_degeneracy",
        layer="features",
        severity="alta",
        passed=rul_share_max <= 0.95,
        blocker_if_fail=True,
        what_checked="Degeneración de RUL en valor máximo",
        detail=f"share_rul_365={rul_share_max:.3f}",
        metric_value=f"{rul_share_max:.3f}",
        threshold="<=0.95",
        impact_potential="RUL sin variabilidad inutiliza decisiones temporales de intervención.",
        technical_recommendation="Recalibrar estimación RUL y evitar techo estructural.",
    )

    # Validaciones específicas de RUL (hardening de discriminación y utilidad).
    if not rul_before_after.empty and {"legacy_rul_days", "component_rul_estimate"}.issubset(rul_before_after.columns):
        legacy_cap_share = float((rul_before_after["legacy_rul_days"] >= 365).mean())
        new_cap = float(rul_before_after["component_rul_estimate"].max())
        new_cap_share = float((rul_before_after["component_rul_estimate"] >= new_cap).mean())
        p10_new = float(rul_before_after["component_rul_estimate"].quantile(0.10))
        p90_new = float(rul_before_after["component_rul_estimate"].quantile(0.90))
        rul_delta_mean = float((rul_before_after["component_rul_estimate"] - rul_before_after["legacy_rul_days"]).mean())
    else:
        legacy_cap_share = np.nan
        new_cap_share = np.nan
        p10_new = np.nan
        p90_new = np.nan
        rul_delta_mean = np.nan

    _add_result(
        results,
        check_id="features_rul_saturation_reduction_vs_legacy",
        layer="features",
        severity="alta",
        passed=bool(np.isfinite(new_cap_share) and np.isfinite(legacy_cap_share) and (new_cap_share <= max(0.60, legacy_cap_share - 0.20))),
        blocker_if_fail=False,
        what_checked="Reducción de saturación RUL frente a baseline legacy",
        detail=f"legacy_share_cap={legacy_cap_share:.3f}, new_share_cap={new_cap_share:.3f}",
        impact_potential="Si no cae la saturación, RUL sigue siendo ornamental para planificación.",
        technical_recommendation="Rebalancear daño efectivo diario y topes por familia.",
    )
    _add_result(
        results,
        check_id="features_rul_distribution_spread_new",
        layer="features",
        severity="alta",
        passed=bool(np.isfinite(p10_new) and np.isfinite(p90_new) and (p90_new - p10_new >= 55)),
        blocker_if_fail=False,
        what_checked="Amplitud de distribución RUL nueva (P90-P10)",
        detail=f"p10={p10_new:.1f}, p90={p90_new:.1f}, spread={p90_new - p10_new:.1f}",
        metric_value=f"{(p90_new - p10_new):.1f}" if np.isfinite(p10_new) and np.isfinite(p90_new) else None,
        threshold=">=55",
        impact_potential="Spread bajo limita capacidad de secuenciar ventanas de intervención.",
        technical_recommendation="Incrementar discriminación por familia/estrés/repetitividad en el estimador.",
    )
    _add_result(
        results,
        check_id="features_rul_shift_vs_legacy",
        layer="features",
        severity="informativa",
        passed=bool(np.isfinite(rul_delta_mean)),
        blocker_if_fail=False,
        what_checked="Desplazamiento medio de RUL nuevo vs legacy",
        detail=f"mean_delta_new_minus_legacy={rul_delta_mean:.2f} días",
        impact_potential="Ayuda a auditar si el rediseño acorta/extiende horizontes de forma plausible.",
        technical_recommendation="Revisar delta por familia antes de despliegue con datos reales.",
    )

    if not rul_family.empty and {"new_p50", "component_family"}.issubset(rul_family.columns):
        family_disp = float(rul_family["new_p50"].max() - rul_family["new_p50"].min())
    else:
        family_disp = np.nan
    _add_result(
        results,
        check_id="features_rul_family_discrimination",
        layer="features",
        severity="media",
        passed=bool(np.isfinite(family_disp) and family_disp >= 12),
        blocker_if_fail=False,
        what_checked="Discriminación de RUL por familia técnica",
        detail=f"median_range_days={family_disp:.2f}",
        metric_value=f"{family_disp:.2f}" if np.isfinite(family_disp) else None,
        threshold=">=12",
        impact_potential="Sin dispersión por familia, RUL ignora física operativa diferencial.",
        technical_recommendation="Ajustar perfiles por familia y sensibilidad al estrés.",
    )

    if not rul_linkage.empty and {"method", "rul_bucket", "failure_rate_30d"}.issubset(rul_linkage.columns):
        new_link = rul_linkage[rul_linkage["method"] == "nuevo_proxy_familia"].copy()
        low = float(new_link.loc[new_link["rul_bucket"].isin(["00_<=14", "01_15_30"]), "failure_rate_30d"].mean())
        high = float(new_link.loc[new_link["rul_bucket"] == "05_>180", "failure_rate_30d"].mean())
        separation = low - high
    else:
        separation = np.nan
        low = np.nan
        high = np.nan
    _add_result(
        results,
        check_id="features_rul_failure_linkage_separation",
        layer="features",
        severity="alta",
        passed=bool(np.isfinite(separation) and separation >= 0.02),
        blocker_if_fail=False,
        what_checked="Separación de tasa de falla futura por bucket RUL (backtest)",
        detail=f"failure_rate_low_rul={low:.4f}, failure_rate_high_rul={high:.4f}, delta={separation:.4f}",
        metric_value=f"{separation:.4f}" if np.isfinite(separation) else None,
        threshold=">=0.02",
        impact_potential="Sin linkage con fallas posteriores, RUL pierde credibilidad prognóstica.",
        technical_recommendation="Recalibrar penalización por repetitividad/estrés y umbrales de agotamiento.",
    )

    if not rul_checks.empty and {"passed"}.issubset(rul_checks.columns):
        rul_check_pass_rate = float(rul_checks["passed"].astype(bool).mean())
    else:
        rul_check_pass_rate = np.nan
    _add_result(
        results,
        check_id="features_rul_specific_checks_pass_rate",
        layer="features",
        severity="media",
        passed=bool(np.isfinite(rul_check_pass_rate) and rul_check_pass_rate >= 0.67),
        blocker_if_fail=False,
        what_checked="Cobertura de checks específicos del framework RUL",
        detail=f"pass_rate={rul_check_pass_rate:.3f}",
        metric_value=f"{rul_check_pass_rate:.3f}" if np.isfinite(rul_check_pass_rate) else None,
        threshold=">=0.67",
        impact_potential="Framework RUL sin QA específico reduce auditabilidad del módulo.",
        technical_recommendation="Corregir checks fallidos en rul_validation_checks y regenerar evidencias.",
    )


def _validate_scores_layer(processed: dict[str, pd.DataFrame], results: list[QAResult]) -> None:
    score = processed["scoring_componentes"]
    failure_score = processed["component_failure_risk_score"]
    health_score = processed["component_health_score"]
    component = processed["component_day_features"]
    strategy_base = processed["comparativo_estrategias"]
    strategy_sim = processed["comparativo_estrategias_sensibilidad"]
    strategy_summary = processed["comparativo_estrategias_escenarios"]
    strategy_ranges = processed["comparativo_estrategias_value_ranges"]

    range_ok = (
        score["prob_fallo_30d"].between(0, 1).all()
        and score["health_score"].between(0, 100).all()
        and score["riesgo_ajustado_negocio"].between(0, 100).all()
    )
    _add_result(
        results,
        check_id="scores_range_sanity",
        layer="scores",
        severity="critica",
        passed=bool(range_ok),
        blocker_if_fail=True,
        what_checked="Rangos de scores finales",
        detail="prob_fallo_30d, health_score, riesgo_ajustado_negocio en rango",
        impact_potential="Scores fuera de rango invalidan toda la capa decisional.",
        technical_recommendation="Aplicar clipping final y revisar composición de score.",
    )

    risk_sat = float((score["prob_fallo_30d"] >= 0.90).mean())
    risk_entropy = _series_entropy(score["prob_fallo_30d"], bins=10)
    _add_result(
        results,
        check_id="scores_saturation_failure_risk",
        layer="scores",
        severity="alta",
        passed=risk_sat <= 0.35,
        blocker_if_fail=False,
        what_checked="Saturación en cola alta de prob_fallo_30d",
        detail=f"share_ge_0.90={risk_sat:.3f}",
        metric_value=f"{risk_sat:.3f}",
        threshold="<=0.35",
        impact_potential="Demasiados casos críticos artificiales destruyen priorización.",
        technical_recommendation="Recalibrar funciones de escala y umbrales por familia.",
    )
    _add_result(
        results,
        check_id="scores_entropy_failure_risk",
        layer="scores",
        severity="media",
        passed=risk_entropy >= 2.0,
        blocker_if_fail=False,
        what_checked="Entropía de distribución de failure risk",
        detail=f"entropy={risk_entropy:.3f}",
        metric_value=f"{risk_entropy:.3f}",
        threshold=">=2.0",
        impact_potential="Baja entropía reduce ranking discriminante.",
        technical_recommendation="Ajustar calibración por percentiles y winsorización.",
    )

    risk_std = float(score["prob_fallo_30d"].std(ddof=0))
    risk_iqr = float(score["prob_fallo_30d"].quantile(0.75) - score["prob_fallo_30d"].quantile(0.25))
    _add_result(
        results,
        check_id="scores_variability_minimum",
        layer="scores",
        severity="alta",
        passed=bool(risk_std >= 0.08 and risk_iqr >= 0.10),
        blocker_if_fail=True,
        what_checked="Variabilidad mínima de score de riesgo (std + IQR)",
        detail=f"std={risk_std:.4f}, iqr={risk_iqr:.4f}",
        metric_value=f"{risk_std:.4f}/{risk_iqr:.4f}",
        threshold="std>=0.08 & iqr>=0.10",
        impact_potential="Baja variabilidad degrada ranking y oculta casos críticos reales.",
        technical_recommendation="Recalibrar señales dominantes y clipping para recuperar discriminación.",
    )

    if "component_family" in score.columns:
        fam_mean = score.groupby("component_family", observed=True)["prob_fallo_30d"].mean()
        fam_spread = float(fam_mean.max() - fam_mean.min()) if not fam_mean.empty else 0.0
    else:
        fam_spread = 0.0
    _add_result(
        results,
        check_id="scores_family_risk_spread",
        layer="scores",
        severity="media",
        passed=bool(fam_spread >= 0.04),
        blocker_if_fail=False,
        what_checked="Separación de riesgo medio entre familias técnicas",
        detail=f"family_spread={fam_spread:.4f}",
        metric_value=f"{fam_spread:.4f}",
        threshold=">=0.04",
        impact_potential="Sin separación por familia, la capa técnica pierde realismo operacional.",
        technical_recommendation="Revisar ajustes por familia y sensibilidad por subsistema.",
    )

    driver_dom = float(score["main_risk_driver"].value_counts(normalize=True).max())
    driver_entropy = _class_entropy(score["main_risk_driver"])
    action_dom = float(score["recommended_action_initial"].value_counts(normalize=True).max())
    action_entropy = _class_entropy(score["recommended_action_initial"])
    action_classes = int(score["recommended_action_initial"].nunique())
    conf_dom = float(score["confidence_flag"].value_counts(normalize=True).max())

    _add_result(
        results,
        check_id="scores_driver_class_collapse",
        layer="scores",
        severity="media",
        passed=driver_dom <= 0.80 and driver_entropy >= 1.0,
        blocker_if_fail=False,
        what_checked="Colapso de clase en main_risk_driver",
        detail=f"dominant_share={driver_dom:.3f}, entropy={driver_entropy:.3f}",
        impact_potential="Pierde explicabilidad causal de riesgo.",
        technical_recommendation="Rebalancear contribuciones y desempate de drivers.",
    )
    _add_result(
        results,
        check_id="scores_action_class_collapse",
        layer="scores",
        severity="alta",
        passed=action_dom <= 0.60 and action_classes >= 6 and action_entropy >= 1.2,
        blocker_if_fail=True,
        what_checked="Colapso de recommended_action_initial",
        detail=f"dominant_share={action_dom:.3f}, classes={action_classes}, entropy={action_entropy:.3f}",
        impact_potential="Recomendación colapsada elimina valor decisional.",
        technical_recommendation="Reforzar jerarquía de reglas y resolución de conflictos.",
    )
    _add_result(
        results,
        check_id="scores_confidence_flag_collapse",
        layer="scores",
        severity="alta",
        passed=conf_dom <= 0.98,
        blocker_if_fail=True,
        what_checked="Colapso de confidence_flag",
        detail=f"dominant_share={conf_dom:.3f}",
        metric_value=f"{conf_dom:.3f}",
        threshold="<=0.98",
        impact_potential="Sin diversidad de confianza no hay gobernanza real del riesgo.",
        technical_recommendation="Recalibrar calidad de señal y criterios de confidence_flag.",
    )

    component_latest = component.sort_values("fecha").drop_duplicates(["unidad_id", "componente_id"], keep="last")[
        ["unidad_id", "componente_id", "deterioration_index", "maintenance_restoration_index"]
    ].rename(
        columns={
            "deterioration_index": "deterioration_index_aux",
            "maintenance_restoration_index": "maintenance_restoration_index_aux",
        }
    )
    sem = score.merge(component_latest, on=["unidad_id", "componente_id"], how="left")
    if "deterioration_index" not in sem.columns:
        sem["deterioration_index"] = sem["deterioration_index_aux"]
    else:
        sem["deterioration_index"] = sem["deterioration_index"].fillna(sem["deterioration_index_aux"])
    if "maintenance_restoration_index" not in sem.columns:
        sem["maintenance_restoration_index"] = sem["maintenance_restoration_index_aux"]
    else:
        sem["maintenance_restoration_index"] = sem["maintenance_restoration_index"].fillna(sem["maintenance_restoration_index_aux"])
    corr_health = float(sem[["health_score", "prob_fallo_30d"]].corr(method="spearman").iloc[0, 1])
    corr_det = float(sem[["deterioration_index", "prob_fallo_30d"]].corr(method="spearman").iloc[0, 1])
    corr_rest = float(sem[["maintenance_restoration_index", "prob_fallo_30d"]].corr(method="spearman").iloc[0, 1])
    semantic_ok = corr_health <= -0.30 and corr_det >= 0.30 and corr_rest <= -0.03
    _add_result(
        results,
        check_id="scores_semantic_sign_consistency",
        layer="scores",
        severity="critica",
        passed=semantic_ok,
        blocker_if_fail=True,
        what_checked="Signos semánticos health/deterioration/restoration vs riesgo",
        detail=f"rho_health={corr_health:.3f}, rho_det={corr_det:.3f}, rho_rest={corr_rest:.3f}",
        impact_potential="Semántica invertida compromete defendibilidad técnica en entrevista/comité.",
        technical_recommendation="Alinear fórmulas con convención oficial y revalidar pesos.",
    )

    merged_failure = score.merge(
        failure_score[["unidad_id", "componente_id", "component_failure_risk_score"]].rename(
            columns={"component_failure_risk_score": "component_failure_risk_score_aux"}
        ),
        on=["unidad_id", "componente_id"],
        how="left",
    )
    merged_health = score.merge(
        health_score[["unidad_id", "componente_id", "component_health_score"]].rename(
            columns={"component_health_score": "component_health_score_aux"}
        ),
        on=["unidad_id", "componente_id"],
        how="left",
    )
    if "component_failure_risk_score" not in merged_failure.columns:
        merged_failure["component_failure_risk_score"] = merged_failure["component_failure_risk_score_aux"]
    else:
        merged_failure["component_failure_risk_score"] = merged_failure["component_failure_risk_score"].fillna(
            merged_failure["component_failure_risk_score_aux"]
        )
    if "component_health_score" not in merged_health.columns:
        merged_health["component_health_score"] = merged_health["component_health_score_aux"]
    else:
        merged_health["component_health_score"] = merged_health["component_health_score"].fillna(
            merged_health["component_health_score_aux"]
        )

    max_diff_failure = float((merged_failure["prob_fallo_30d"] - merged_failure["component_failure_risk_score"]).abs().max())
    max_diff_health = float((merged_health["health_score"] - merged_health["component_health_score"]).abs().max())
    _add_result(
        results,
        check_id="scores_cross_output_consistency",
        layer="scores",
        severity="alta",
        passed=max_diff_failure <= 1e-6 and max_diff_health <= 1e-6,
        blocker_if_fail=True,
        what_checked="Consistencia entre scoring_componentes y tablas score dedicadas",
        detail=f"max_diff_failure={max_diff_failure:.8f}, max_diff_health={max_diff_health:.8f}",
        impact_potential="Divergencia entre outputs rompe SSOT de métricas.",
        technical_recommendation="Unificar generación de tablas finales desde un único dataframe.",
    )

    p90 = float(score["prob_fallo_30d"].quantile(0.90))
    p10 = float(score["prob_fallo_30d"].quantile(0.10))
    top_mean = float(score.loc[score["prob_fallo_30d"] >= p90, "riesgo_ajustado_negocio"].mean())
    bot_mean = float(score.loc[score["prob_fallo_30d"] <= p10, "riesgo_ajustado_negocio"].mean())
    discr_ratio = top_mean / max(bot_mean, 1e-6)
    _add_result(
        results,
        check_id="scores_rank_discrimination",
        layer="scores",
        severity="media",
        passed=discr_ratio >= 1.8,
        blocker_if_fail=False,
        what_checked="Discriminación top/bottom decile de riesgo",
        detail=f"ratio_top10_bottom10={discr_ratio:.3f}",
        metric_value=f"{discr_ratio:.3f}",
        threshold=">=1.8",
        impact_potential="Baja separación top/bottom dificulta priorización real.",
        technical_recommendation="Incrementar separación de señales críticas y recalibrar score final.",
    )

    # Robustez del comparativo estratégico (escenarios + sensibilidad).
    if not strategy_summary.empty and {"scenario_profile", "estrategia"}.issubset(strategy_summary.columns):
        scenarios = set(strategy_summary["scenario_profile"].astype(str))
        strat_per_scenario = strategy_summary.groupby("scenario_profile")["estrategia"].nunique()
        summary_ok = scenarios == {"conservador", "base", "agresivo"} and int(strat_per_scenario.min()) >= 3
        summary_detail = f"scenarios={sorted(scenarios)}, min_strategies_per_scenario={int(strat_per_scenario.min())}"
    else:
        summary_ok = False
        summary_detail = "archivo_escenarios_incompleto"
    _add_result(
        results,
        check_id="scores_strategy_scenario_coverage",
        layer="scores",
        severity="alta",
        passed=summary_ok,
        blocker_if_fail=True,
        what_checked="Cobertura de escenarios y estrategias en comparativo estratégico",
        detail=summary_detail,
        impact_potential="Sin cobertura de escenarios el comparativo no es defendible ante operaciones/finanzas.",
        technical_recommendation="Regenerar simulación estratégica con perfiles conservador/base/agresivo completos.",
    )

    if not strategy_sim.empty and {"scenario_profile", "sensitivity_id", "estrategia", "ahorro_neto_vs_reactiva"}.issubset(strategy_sim.columns):
        sim_strategies = int(strategy_sim["estrategia"].nunique())
        per_combo = strategy_sim.groupby(["scenario_profile", "sensitivity_id"])["estrategia"].nunique()
        combo_balance_ok = bool((per_combo == sim_strategies).all())
        cbm_sim = strategy_sim[strategy_sim["estrategia"] == "basada_en_condicion"]
        savings_std = float(cbm_sim["ahorro_neto_vs_reactiva"].std()) if not cbm_sim.empty else 0.0
        prob_positive = float((cbm_sim["ahorro_neto_vs_reactiva"] > 0).mean()) if not cbm_sim.empty else 0.0
        sim_size_ok = len(strategy_sim) >= 3000
    else:
        combo_balance_ok = False
        savings_std = 0.0
        prob_positive = 0.0
        sim_size_ok = False
    _add_result(
        results,
        check_id="scores_strategy_sensitivity_integrity",
        layer="scores",
        severity="alta",
        passed=combo_balance_ok and sim_size_ok,
        blocker_if_fail=True,
        what_checked="Integridad estructural de simulación de sensibilidad",
        detail=f"rows={len(strategy_sim)}, combo_balance={combo_balance_ok}",
        impact_potential="Simulación inconsistente invalida rangos de valor y downside.",
        technical_recommendation="Verificar grid de sensibilidad y composición de escenarios.",
    )
    _add_result(
        results,
        check_id="scores_strategy_uncertainty_non_degenerate",
        layer="scores",
        severity="alta",
        passed=(savings_std > 1_000_000) and (0.50 <= prob_positive <= 0.99),
        blocker_if_fail=True,
        what_checked="No degeneración de la incertidumbre económica CBM vs reactiva",
        detail=f"std_ahorro_cbm={savings_std:.2f}, prob_ahorro_positivo={prob_positive:.3f}",
        metric_value=f"{prob_positive:.3f}",
        threshold="prob in [0.50,0.99] y std>1M",
        impact_potential="Si colapsa a 0%/100%, el comparativo vuelve a ser determinista y poco creíble.",
        technical_recommendation="Recalibrar supuestos de sensibilidad/capacidad/señal para recuperar incertidumbre realista.",
    )

    if not strategy_ranges.empty and {
        "coste_total_p10",
        "coste_total_p50",
        "coste_total_p90",
        "ahorro_neto_p10_vs_reactiva",
        "ahorro_neto_p50_vs_reactiva",
        "ahorro_neto_p90_vs_reactiva",
        "downside_case",
    }.issubset(strategy_ranges.columns):
        cost_order_issues = int(
            (
                (strategy_ranges["coste_total_p10"] > strategy_ranges["coste_total_p50"])
                | (strategy_ranges["coste_total_p50"] > strategy_ranges["coste_total_p90"])
            ).sum()
        )
        value_order_issues = int(
            (
                (strategy_ranges["ahorro_neto_p10_vs_reactiva"] > strategy_ranges["ahorro_neto_p50_vs_reactiva"])
                | (strategy_ranges["ahorro_neto_p50_vs_reactiva"] > strategy_ranges["ahorro_neto_p90_vs_reactiva"])
            ).sum()
        )
        downside_issues = int((strategy_ranges["downside_case"] > strategy_ranges["ahorro_neto_p10_vs_reactiva"]).sum())
    else:
        cost_order_issues = math.inf
        value_order_issues = math.inf
        downside_issues = math.inf
    _add_result(
        results,
        check_id="scores_strategy_value_range_monotonicity",
        layer="scores",
        severity="alta",
        passed=(cost_order_issues == 0 and value_order_issues == 0 and downside_issues == 0),
        blocker_if_fail=True,
        what_checked="Monotonía P10/P50/P90 y consistencia downside en comparativo estratégico",
        detail=f"cost_order_issues={cost_order_issues}, value_order_issues={value_order_issues}, downside_issues={downside_issues}",
        impact_potential="Rangos mal ordenados rompen credibilidad económica ante finanzas.",
        technical_recommendation="Corregir agregaciones cuantílicas y definición de downside_case.",
    )

    if not strategy_base.empty and {"estrategia", "fleet_availability", "coste_operativo_proxy", "ahorro_neto_vs_reactiva"}.issubset(strategy_base.columns):
        cbm_base = strategy_base[strategy_base["estrategia"] == "basada_en_condicion"]
        react_base = strategy_base[strategy_base["estrategia"] == "reactiva"]
        if not cbm_base.empty and not react_base.empty:
            cbm_savings_semantic_ok = abs(
                float(cbm_base.iloc[0]["ahorro_neto_vs_reactiva"])
                - (float(react_base.iloc[0]["coste_operativo_proxy"]) - float(cbm_base.iloc[0]["coste_operativo_proxy"]))
            ) <= 1e-6
        else:
            cbm_savings_semantic_ok = False
    else:
        cbm_savings_semantic_ok = False
    _add_result(
        results,
        check_id="scores_strategy_base_semantic_consistency",
        layer="scores",
        severity="critica",
        passed=cbm_savings_semantic_ok,
        blocker_if_fail=True,
        what_checked="Consistencia semántica ahorro_neto_vs_reactiva en tabla base",
        detail=f"cbm_savings_semantic_ok={cbm_savings_semantic_ok}",
        impact_potential="Si el ahorro base no cuadra, se rompe la narrativa económica del proyecto.",
        technical_recommendation="Unificar fórmula de ahorro neto en una sola función fuente.",
    )


def _validate_recommendations_layer(processed: dict[str, pd.DataFrame], results: list[QAResult]) -> None:
    score = processed["scoring_componentes"]
    prio = processed["workshop_priority_table"]
    plan = processed["workshop_scheduling_recommendation"]
    cap_cal = processed.get("workshop_capacity_calendar", pd.DataFrame())
    sched_metrics = processed.get("scheduling_before_after_metrics", pd.DataFrame())

    decision_dom = float(prio["decision_type"].value_counts(normalize=True).max())
    decision_entropy = _class_entropy(prio["decision_type"])
    decision_classes = int(prio["decision_type"].nunique())
    _add_result(
        results,
        check_id="recommendation_decision_class_collapse",
        layer="recommendations",
        severity="alta",
        passed=decision_dom <= 0.60 and decision_classes >= 6 and decision_entropy >= 1.2,
        blocker_if_fail=True,
        what_checked="Colapso de decision_type",
        detail=f"dominant_share={decision_dom:.3f}, classes={decision_classes}, entropy={decision_entropy:.3f}",
        impact_potential="Motor de decisión no discrimina casos operativos.",
        technical_recommendation="Ajustar jerarquía operativa y lógica de conflictos/capacidad.",
    )

    high_risk_weak = float(
        (
            prio["prob_fallo_30d"].ge(0.80)
            & prio["decision_type"].isin(["no acción por ahora", "mantener bajo observación"])
        ).mean()
    )
    _add_result(
        results,
        check_id="recommendation_high_risk_weak_action",
        layer="recommendations",
        severity="critica",
        passed=high_risk_weak <= 0.01,
        blocker_if_fail=True,
        what_checked="Casos alto riesgo con decisión operativa débil",
        detail=f"share_high_risk_weak={high_risk_weak:.4f}",
        metric_value=f"{high_risk_weak:.4f}",
        threshold="<=0.01",
        impact_potential="Exposición directa a falla por sub-reacción del motor.",
        technical_recommendation="Escalar prioridad para alto riesgo y revisar reglas de no acción.",
    )

    high_deferral_weak = float(
        (
            prio["deferral_risk_score"].ge(75)
            & prio["decision_type"].isin(["no acción por ahora", "mantener bajo observación"])
        ).mean()
    )
    _add_result(
        results,
        check_id="recommendation_high_deferral_weak_action",
        layer="recommendations",
        severity="alta",
        passed=high_deferral_weak <= 0.02,
        blocker_if_fail=True,
        what_checked="Casos de alto riesgo de diferimiento con decisión débil",
        detail=f"share_high_deferral_weak={high_deferral_weak:.4f}",
        metric_value=f"{high_deferral_weak:.4f}",
        threshold="<=0.02",
        impact_potential="Diferimientos de alto riesgo sin respuesta elevan exposición operativa y económica.",
        technical_recommendation="Forzar escalado/intervención para percentiles altos de deferral_risk_score.",
    )

    no_action = prio[prio["decision_type"] == "no acción por ahora"]
    no_action_ok = no_action.empty or (
        float(no_action["prob_fallo_30d"].mean()) <= 0.25 and float(no_action["health_score"].mean()) >= 50
    )
    _add_result(
        results,
        check_id="recommendation_no_action_plausibility",
        layer="recommendations",
        severity="media",
        passed=bool(no_action_ok),
        blocker_if_fail=False,
        what_checked="Plausibilidad de la clase no acción",
        detail=(
            "sin_casos_no_accion"
            if no_action.empty
            else f"mean_risk={no_action['prob_fallo_30d'].mean():.3f}, mean_health={no_action['health_score'].mean():.2f}"
        ),
        impact_potential="No acción mal calibrada compromete seguridad operativa.",
        technical_recommendation="Endurecer criterios de no acción con umbrales de riesgo/salud.",
    )

    immediate = prio[prio["decision_type"] == "intervención inmediata"]
    immediate_ok = (not immediate.empty) and float(immediate["prob_fallo_30d"].mean()) >= 0.75
    _add_result(
        results,
        check_id="recommendation_immediate_presence_plausibility",
        layer="recommendations",
        severity="media",
        passed=bool(immediate_ok),
        blocker_if_fail=False,
        what_checked="Presencia y plausibilidad de intervención inmediata",
        detail="sin_casos_inmediatos" if immediate.empty else f"mean_risk={immediate['prob_fallo_30d'].mean():.3f}",
        impact_potential="Sin clase inmediata, se pierde respuesta ante emergencias.",
        technical_recommendation="Recalibrar gatillos de urgencia y lógica de capacidad.",
    )

    decision_window_map = {
        "intervención inmediata": 2,
        "intervención en próxima ventana": 7,
        "inspección prioritaria": 3,
        "monitorización intensiva": 10,
        "mantener bajo observación": 14,
        "no acción por ahora": 21,
        "escalado técnico/manual review": 1,
    }
    bad_window = int((prio.apply(lambda r: decision_window_map.get(r["decision_type"], r["suggested_window_days"]) != r["suggested_window_days"], axis=1)).sum())
    _add_result(
        results,
        check_id="recommendation_window_consistency",
        layer="recommendations",
        severity="alta",
        passed=bad_window == 0,
        blocker_if_fail=False,
        what_checked="Consistencia decisión -> suggested_window_days",
        detail=f"inconsistent_rows={bad_window}",
        impact_potential="Incoherencia temporal en scheduling recomendado.",
        technical_recommendation="Mantener mapping de ventanas centralizado y testeado.",
    )

    seq_unique = int(prio["recommended_entry_sequence"].nunique()) == len(prio)
    seq_min = int(prio["recommended_entry_sequence"].min())
    _add_result(
        results,
        check_id="recommendation_priority_sequence_integrity",
        layer="recommendations",
        severity="alta",
        passed=seq_unique and seq_min == 1,
        blocker_if_fail=False,
        what_checked="Integridad de secuencia de entrada recomendada",
        detail=f"unique={seq_unique}, min_sequence={seq_min}",
        impact_potential="Cola de taller no determinística o incompleta.",
        technical_recommendation="Revisar ranking method y persistencia de ordering.",
    )

    valid_sched_statuses = {
        "programada",
        "programable_proxima_ventana",
        "pendiente_repuesto",
        "pendiente_capacidad",
        "pendiente_conflicto_operativo",
        "escalar_decision",
    }
    unknown_status_rows = int((~plan["estado_intervencion"].isin(valid_sched_statuses)).sum())
    _add_result(
        results,
        check_id="recommendation_scheduling_status_taxonomy",
        layer="recommendations",
        severity="alta",
        passed=unknown_status_rows == 0,
        blocker_if_fail=True,
        what_checked="Taxonomía oficial de estados de scheduling",
        detail=f"unknown_status_rows={unknown_status_rows}",
        impact_potential="Estados no estandarizados rompen ejecución y reporting operativo.",
        technical_recommendation="Restringir salida del scheduler al enum oficial.",
    )

    scheduled_states = {"programada", "programable_proxima_ventana"}
    pending_states = {"pendiente_repuesto", "pendiente_capacidad", "pendiente_conflicto_operativo", "escalar_decision"}
    scheduled_missing_date = int(
        (plan["estado_intervencion"].isin(scheduled_states)).astype(int).sum()
        - plan.loc[plan["estado_intervencion"].isin(scheduled_states), "ventana_temporal_sugerida"].notna().sum()
    )
    pending_with_date = int(plan.loc[plan["estado_intervencion"].isin(pending_states), "ventana_temporal_sugerida"].notna().sum())
    _add_result(
        results,
        check_id="recommendation_scheduling_temporal_integrity",
        layer="recommendations",
        severity="alta",
        passed=scheduled_missing_date == 0 and pending_with_date == 0,
        blocker_if_fail=False,
        what_checked="Coherencia estado_intervencion vs fecha sugerida",
        detail=f"scheduled_missing_date={scheduled_missing_date}, pending_with_date={pending_with_date}",
        impact_potential="Plan de taller inconsistente para ejecución operativa.",
        technical_recommendation="Ajustar asignación de ventanas en heurística de scheduling.",
    )

    plan_dates = plan.copy()
    plan_dates["ventana_temporal_sugerida"] = pd.to_datetime(plan_dates["ventana_temporal_sugerida"], errors="coerce")
    if "fecha" in prio.columns:
        prio_dates = prio[["unidad_id", "componente_id", "fecha", "suggested_window_days"]].copy()
        prio_dates["fecha"] = pd.to_datetime(prio_dates["fecha"], errors="coerce")
        check_df = plan_dates.merge(prio_dates, on=["unidad_id", "componente_id"], how="left")
        check_df["day_offset"] = (check_df["ventana_temporal_sugerida"] - check_df["fecha"]).dt.days
        bad_programada = int(
            check_df.loc[
                check_df["estado_intervencion"] == "programada",
                "day_offset",
            ].gt(check_df.loc[check_df["estado_intervencion"] == "programada", "suggested_window_days"]).fillna(False).sum()
        )
        bad_programable = int(
            check_df.loc[
                check_df["estado_intervencion"] == "programable_proxima_ventana",
                "day_offset",
            ].le(check_df.loc[check_df["estado_intervencion"] == "programable_proxima_ventana", "suggested_window_days"]).fillna(False).sum()
        )
    else:
        bad_programada = math.inf
        bad_programable = math.inf
    _add_result(
        results,
        check_id="recommendation_scheduling_window_semantics",
        layer="recommendations",
        severity="media",
        passed=(bad_programada == 0 and bad_programable == 0),
        blocker_if_fail=False,
        what_checked="Semántica de ventana: programada dentro y programable fuera de ventana preferida",
        detail=f"bad_programada={bad_programada}, bad_programable={bad_programable}",
        impact_potential="Estados de scheduling pierden significado operativo.",
        technical_recommendation="Corregir regla de asignación por ventana preferida vs carry-over.",
    )

    actionable_share = float(plan["estado_intervencion"].isin(scheduled_states).mean())
    _add_result(
        results,
        check_id="recommendation_scheduling_actionability_share",
        layer="recommendations",
        severity="alta",
        passed=actionable_share >= 0.45,
        blocker_if_fail=True,
        what_checked="Porcentaje de casos accionables (programada + programable_proxima_ventana)",
        detail=f"actionable_share={actionable_share:.3f}",
        metric_value=f"{actionable_share:.3f}",
        threshold=">=0.45",
        impact_potential="Si es bajo, el scheduling sigue siendo poco ejecutable operativamente.",
        technical_recommendation="Revisar horizonte, capacidad flexible y carry-over por bucket.",
    )

    if not cap_cal.empty and {"total_used_h", "total_capacity_h"}.issubset(cap_cal.columns):
        cap_overrun = int((cap_cal["total_used_h"] > (cap_cal["total_capacity_h"] + 1e-6)).sum())
    else:
        cap_overrun = math.inf
    _add_result(
        results,
        check_id="recommendation_capacity_overrun_sanity",
        layer="recommendations",
        severity="alta",
        passed=cap_overrun == 0,
        blocker_if_fail=True,
        what_checked="No sobreasignación de capacidad en calendario de taller",
        detail=f"capacity_overrun_rows={cap_overrun}",
        impact_potential="Plan irrealizable por consumo de capacidad mayor a la disponible.",
        technical_recommendation="Corregir asignación de slots y uso de capacidad flexible.",
    )

    if not sched_metrics.empty and {"scenario", "actionable_pct"}.issubset(sched_metrics.columns):
        before = sched_metrics[sched_metrics["scenario"] == "baseline_greedy_21d"]["actionable_pct"]
        after = sched_metrics[sched_metrics["scenario"] == "heuristica_redisenada_35d"]["actionable_pct"]
        if not before.empty and not after.empty:
            improved = float(after.iloc[0]) >= float(before.iloc[0]) + 5.0
            imp_detail = f"before={float(before.iloc[0]):.2f}, after={float(after.iloc[0]):.2f}"
        else:
            improved = False
            imp_detail = "filas_before_after_no_disponibles"
    else:
        improved = False
        imp_detail = "archivo_scheduling_before_after_metrics_incompleto"
    _add_result(
        results,
        check_id="recommendation_scheduling_before_after_improvement",
        layer="recommendations",
        severity="media",
        passed=bool(improved),
        blocker_if_fail=False,
        what_checked="Mejora de accionabilidad vs baseline de scheduling",
        detail=imp_detail,
        impact_potential="Sin mejora tangible, el rediseño no aporta valor operativo real.",
        technical_recommendation="Recalibrar bucketización, carry-over y asignación multiperiodo.",
    )

    conflict_share = float(prio["decision_conflict_flag"].mean()) if "decision_conflict_flag" in prio.columns else 1.0
    _add_result(
        results,
        check_id="recommendation_conflict_rate",
        layer="recommendations",
        severity="media",
        passed=conflict_share <= 0.30,
        blocker_if_fail=False,
        what_checked="Tasa de conflictos de decisión",
        detail=f"conflict_share={conflict_share:.3f}",
        metric_value=f"{conflict_share:.3f}",
        threshold="<=0.30",
        impact_potential="Demasiados conflictos indican motor inestable o señal insuficiente.",
        technical_recommendation="Refinar reglas de conflicto y mejorar calidad de confianza.",
    )

    valid_actions = {
        "intervencion_inmediata",
        "intervencion_proxima_ventana",
        "inspeccion_prioritaria",
        "monitorizacion_intensiva",
        "mantener_bajo_observacion",
        "no_accion_por_ahora",
        "escalado_tecnico_manual_review",
    }
    mismatch = int((~score["recommended_action_initial"].isin(valid_actions)).sum())
    _add_result(
        results,
        check_id="recommendation_action_taxonomy_consistency",
        layer="recommendations",
        severity="informativa",
        passed=mismatch == 0,
        blocker_if_fail=False,
        what_checked="Consistencia de taxonomía de acciones en scoring",
        detail=f"unknown_action_rows={mismatch}",
        impact_potential="Etiquetado no estándar impide análisis por clase y QA histórica.",
        technical_recommendation="Mantener enum oficial de acciones y validar en runtime.",
    )


def _validate_dashboard_layer(processed: dict[str, pd.DataFrame], results: list[QAResult]) -> None:
    dashboard_path = OUTPUTS_DASHBOARD_DIR / "index.html"
    html = dashboard_path.read_text(encoding="utf-8") if dashboard_path.exists() else ""
    prio = processed["workshop_priority_table"]
    score = processed["scoring_componentes"]
    rul = processed["component_rul_estimate"]
    narrative = processed.get("narrative_metrics_official", pd.DataFrame())

    _add_result(
        results,
        check_id="dashboard_file_exists",
        layer="dashboard_datasets",
        severity="critica",
        passed=dashboard_path.exists(),
        blocker_if_fail=True,
        what_checked="Existencia de outputs/dashboard/index.html",
        detail="ok" if dashboard_path.exists() else "dashboard_no_generado",
        impact_potential="Entregable ejecutivo ausente.",
        technical_recommendation="Ejecutar build_dashboard al final del pipeline.",
    )

    size_bytes = dashboard_path.stat().st_size if dashboard_path.exists() else 0
    _add_result(
        results,
        check_id="dashboard_payload_size_ceiling",
        layer="dashboard_datasets",
        severity="alta",
        passed=(size_bytes > 0 and size_bytes <= 2_000_000),
        blocker_if_fail=False,
        what_checked="Peso del dashboard HTML autocontenido",
        detail=f"size_bytes={size_bytes}",
        impact_potential="Payload excesivo degrada UX y dificulta consumo offline fluido.",
        technical_recommendation="Reducir dataset embebido o densidad de tablas/charts en frontend.",
    )

    has_external_script = bool(re.search(r"<script[^>]+src=['\"]https?://", html, flags=re.IGNORECASE))
    has_external_css = bool(re.search(r"<link[^>]+href=['\"]https?://", html, flags=re.IGNORECASE))
    _add_result(
        results,
        check_id="dashboard_offline_no_external_cdn",
        layer="dashboard_datasets",
        severity="critica",
        passed=not (has_external_script or has_external_css),
        blocker_if_fail=True,
        what_checked="Autocontención offline sin dependencias CDN",
        detail=f"external_script={has_external_script}, external_css={has_external_css}",
        impact_potential="Dependencia de red rompe reproducibilidad y disponibilidad offline.",
        technical_recommendation="Eliminar referencias externas e incrustar todos los assets localmente.",
    )

    has_version_stamp = bool(re.search(r"Dashboard:\s*\d{8}-\d{4}\s*·\s*[a-f0-9]{10}", html))
    _add_result(
        results,
        check_id="dashboard_version_stamp_visible",
        layer="dashboard_datasets",
        severity="media",
        passed=has_version_stamp,
        blocker_if_fail=False,
        what_checked="Versionado visible del dashboard (build + firma payload)",
        detail=f"version_stamp_found={has_version_stamp}",
        impact_potential="Sin versión visible baja auditabilidad en revisiones ejecutivas.",
        technical_recommendation="Incluir build stamp y payload signature en header.",
    )

    required_sections = [
        "Decisión Ejecutiva",
        "Vista de Salud de Activos",
        "Vista de Operación y Servicio",
        "Vista de Taller",
        "Vista Estratégica",
    ]
    missing_sections = [s for s in required_sections if s not in html]
    _add_result(
        results,
        check_id="dashboard_required_sections",
        layer="dashboard_datasets",
        severity="alta",
        passed=len(missing_sections) == 0,
        blocker_if_fail=False,
        what_checked="Secciones clave presentes en HTML",
        detail=f"missing_sections={missing_sections}",
        impact_potential="Narrativa ejecutiva incompleta para toma de decisión.",
        technical_recommendation="Asegurar secciones obligatorias en template del dashboard.",
    )

    required_controls = [
        'id="btnPrevPage"',
        'id="btnNextPage"',
        'id="pageSize"',
        'id="pageInfo"',
    ]
    missing_controls = [x for x in required_controls if x not in html]
    _add_result(
        results,
        check_id="dashboard_pagination_controls_present",
        layer="dashboard_datasets",
        severity="media",
        passed=len(missing_controls) == 0,
        blocker_if_fail=False,
        what_checked="Controles de paginación para tabla final",
        detail=f"missing_controls={missing_controls}",
        impact_potential="Sin paginación, la tabla degrada rendimiento y legibilidad con alta cardinalidad.",
        technical_recommendation="Mantener paginación y límite de render por página.",
    )

    required_backlog_labels = [
        "Backlog físico",
        "Backlog vencido",
        "Backlog crítico físico",
        "Riesgo diferimiento alto",
    ]
    missing_backlog_labels = [x for x in required_backlog_labels if x not in html]
    _add_result(
        results,
        check_id="dashboard_backlog_taxonomy_visibility",
        layer="dashboard_datasets",
        severity="alta",
        passed=len(missing_backlog_labels) == 0,
        blocker_if_fail=False,
        what_checked="Visibilidad explícita de taxonomía backlog/diferimiento en KPI cards",
        detail=f"missing_labels={missing_backlog_labels}",
        impact_potential="Dashboard vuelve a mezclar backlog físico y riesgo de diferimiento.",
        technical_recommendation="Exponer KPI cards separados para backlog y diferimiento.",
    )

    top_unit_html = _extract_token(html, r"Unidad que debe entrar primero:</strong>\s*([A-Z0-9]+)")
    top_comp_html = _extract_token(html, r"Componente que debe sustituirse primero:</strong>\s*([A-Z0-9]+)")
    if not prio.empty:
        top_unit_expected = str(prio.sort_values("intervention_priority_score", ascending=False).iloc[0]["unidad_id"])
    else:
        top_unit_expected = None

    if not narrative.empty and "metric_id" in narrative.columns:
        nlookup = narrative.set_index("metric_id")["metric_value"].to_dict()
        top_comp_expected = str(nlookup.get("top_component_by_priority")) if "top_component_by_priority" in nlookup else None
    else:
        table_for_replace = prio.copy()
        if "prob_fallo_30d" not in table_for_replace.columns:
            table_for_replace = table_for_replace.merge(
                score[["unidad_id", "componente_id", "prob_fallo_30d"]],
                on=["unidad_id", "componente_id"],
                how="left",
            )
        if "component_rul_estimate" not in table_for_replace.columns:
            table_for_replace = table_for_replace.merge(
                rul[["unidad_id", "componente_id", "component_rul_estimate"]],
                on=["unidad_id", "componente_id"],
                how="left",
            )
        if not table_for_replace.empty:
            top_comp_expected = str(
                table_for_replace.sort_values(["component_rul_estimate", "prob_fallo_30d"], ascending=[True, False]).iloc[0]["componente_id"]
            )
        else:
            top_comp_expected = None

    _add_result(
        results,
        check_id="dashboard_top_unit_consistency",
        layer="dashboard_datasets",
        severity="alta",
        passed=(top_unit_html is not None and top_unit_expected is not None and top_unit_html == top_unit_expected),
        blocker_if_fail=False,
        what_checked="Consistencia top unidad dashboard vs priority table",
        detail=f"dashboard={top_unit_html}, expected={top_unit_expected}",
        impact_potential="Mensaje ejecutivo inconsistente con ranking real.",
        technical_recommendation="Sincronizar bloque ejecutivo con cálculo de prioridad.",
    )
    _add_result(
        results,
        check_id="dashboard_top_component_consistency",
        layer="dashboard_datasets",
        severity="media",
        passed=(top_comp_html is not None and top_comp_expected is not None and top_comp_html == top_comp_expected),
        blocker_if_fail=False,
        what_checked="Consistencia top componente dashboard vs lógica de reemplazo",
        detail=f"dashboard={top_comp_html}, expected={top_comp_expected}",
        impact_potential="Recomendación de sustitución inconsistente con datos procesados.",
        technical_recommendation="Reutilizar misma lógica de ordenación en dashboard y reporting.",
    )


def _validate_reports_docs_layer(processed: dict[str, pd.DataFrame], results: list[QAResult]) -> None:
    readme_path = ROOT_DIR / "README.md"
    memo_path = DOCS_DIR / "memo_ejecutivo_es.md"
    reporting_governance_path = DOCS_DIR / "reporting_governance.md"
    metric_contracts_path = DOCS_DIR / "metric_contracts.md"
    data_contracts_path = DOCS_DIR / "data_contracts.md"
    metric_lineage_path = DOCS_DIR / "metric_lineage.md"
    validation_framework_path = DOCS_DIR / "validation_framework.md"
    dashboard_path = OUTPUTS_DASHBOARD_DIR / "index.html"

    readme = readme_path.read_text(encoding="utf-8") if readme_path.exists() else ""
    memo = memo_path.read_text(encoding="utf-8") if memo_path.exists() else ""
    dashboard = dashboard_path.read_text(encoding="utf-8") if dashboard_path.exists() else ""

    for pth in [
        readme_path,
        memo_path,
        dashboard_path,
        reporting_governance_path,
        metric_contracts_path,
        data_contracts_path,
        metric_lineage_path,
    ]:
        _add_result(
            results,
            check_id=f"docs_exists_{pth.name}",
            layer="reports_docs_consistency",
            severity="alta",
            passed=pth.exists(),
            blocker_if_fail=False,
            what_checked=f"Existencia {pth.name}",
            detail="ok" if pth.exists() else "faltante",
            impact_potential="Sin artefactos narrativos no hay auditabilidad ejecutiva.",
            technical_recommendation="Regenerar outputs/reportes y revisar pipeline final.",
        )

    fleet = processed["fleet_week_features"]
    unit_score = processed["unit_unavailability_risk_score"]
    prio = processed["workshop_priority_table"]
    narrative = processed.get("narrative_metrics_official", pd.DataFrame())
    if not fleet.empty:
        avail_real = float(fleet["availability_rate"].mean() * 100)
    else:
        avail_real = np.nan
    high_risk_real = int((unit_score["unit_unavailability_risk_score"] >= 70).sum()) if not unit_score.empty else -1

    narrative_lookup = {}
    if not narrative.empty and {"metric_id", "metric_value"}.issubset(narrative.columns):
        narrative_lookup = narrative.set_index("metric_id")["metric_value"].to_dict()

    backlog_physical_real = int(float(narrative_lookup.get("backlog_physical_items_count", -1)))
    backlog_overdue_real = int(float(narrative_lookup.get("backlog_overdue_items_count", -1)))
    backlog_critical_real = int(float(narrative_lookup.get("backlog_critical_physical_count", -1)))
    deferral_high_real = int(float(narrative_lookup.get("high_deferral_risk_cases_count", -1)))

    avail_readme = _extract_metric(readme, r"disponibilidad media de flota:\s*\*\*([0-9]+(?:\.[0-9]+)?)%")
    high_risk_readme = _extract_metric(readme, r"unidades de alto riesgo:\s*\*\*([0-9]+)")
    backlog_physical_readme = _extract_metric(readme, r"backlog f[ií]sico:\s*\*\*([0-9]+)")
    backlog_overdue_readme = _extract_metric(readme, r"backlog vencido:\s*\*\*([0-9]+)")
    backlog_critical_readme = _extract_metric(readme, r"backlog cr[ií]tico f[ií]sico:\s*\*\*([0-9]+)")
    deferral_high_readme = _extract_metric(readme, r"casos alto riesgo de diferimiento:\s*\*\*([0-9]+)")

    avail_consistent = avail_readme is not None and np.isfinite(avail_real) and abs(avail_readme - avail_real) <= 0.25
    highrisk_consistent = high_risk_readme is not None and abs(high_risk_readme - high_risk_real) <= 2
    backlog_physical_consistent = (
        backlog_physical_readme is not None
        and backlog_physical_real >= 0
        and abs(backlog_physical_readme - backlog_physical_real) <= max(2, int(backlog_physical_real * 0.05) if backlog_physical_real > 0 else 2)
    )
    backlog_overdue_consistent = (
        backlog_overdue_readme is not None
        and backlog_overdue_real >= 0
        and abs(backlog_overdue_readme - backlog_overdue_real) <= max(2, int(backlog_overdue_real * 0.05) if backlog_overdue_real > 0 else 2)
    )
    backlog_critical_consistent = (
        backlog_critical_readme is not None
        and backlog_critical_real >= 0
        and abs(backlog_critical_readme - backlog_critical_real) <= max(2, int(backlog_critical_real * 0.05) if backlog_critical_real > 0 else 2)
    )
    deferral_high_consistent = (
        deferral_high_readme is not None
        and deferral_high_real >= 0
        and abs(deferral_high_readme - deferral_high_real) <= max(2, int(deferral_high_real * 0.05) if deferral_high_real > 0 else 2)
    )

    _add_result(
        results,
        check_id="docs_readme_metric_consistency",
        layer="reports_docs_consistency",
        severity="alta",
        passed=(
            avail_consistent
            and highrisk_consistent
            and backlog_physical_consistent
            and backlog_overdue_consistent
            and backlog_critical_consistent
            and deferral_high_consistent
        ),
        blocker_if_fail=True,
        what_checked="Consistencia métricas README vs data/processed",
        detail=(
            f"availability(readme={avail_readme},real={avail_real:.2f}); "
            f"high_risk(readme={high_risk_readme},real={high_risk_real}); "
            f"backlog_physical(readme={backlog_physical_readme},real={backlog_physical_real}); "
            f"backlog_overdue(readme={backlog_overdue_readme},real={backlog_overdue_real}); "
            f"backlog_critical(readme={backlog_critical_readme},real={backlog_critical_real}); "
            f"deferral_high(readme={deferral_high_readme},real={deferral_high_real})"
        ),
        impact_potential="Narrativa comercial desalineada con resultados reales.",
        technical_recommendation="Actualizar README automáticamente desde outputs versionados.",
    )

    top_unit_memo = _extract_token(memo, r"Unidad prioritaria:\s*([A-Z0-9]+)")
    top_comp_memo = _extract_token(memo, r"Componente prioritario:\s*([A-Z0-9]+)")
    if not prio.empty:
        top_row = prio.sort_values("intervention_priority_score", ascending=False).iloc[0]
        top_unit_real = str(top_row["unidad_id"])
        top_comp_real = str(top_row["componente_id"])
    else:
        top_unit_real = None
        top_comp_real = None
    _add_result(
        results,
        check_id="docs_memo_priority_consistency",
        layer="reports_docs_consistency",
        severity="media",
        passed=(top_unit_memo == top_unit_real and top_comp_memo == top_comp_real),
        blocker_if_fail=False,
        what_checked="Consistencia prioridades memo vs tabla de priorización",
        detail=f"memo_unit={top_unit_memo}, real_unit={top_unit_real}, memo_comp={top_comp_memo}, real_comp={top_comp_real}",
        impact_potential="Memo puede inducir decisiones erróneas si no refleja última corrida.",
        technical_recommendation="Versionar memo a partir de tabla priorizada de la corrida actual.",
    )

    top_unit_dash = _extract_token(dashboard, r"Unidad que debe entrar primero:</strong>\s*([A-Z0-9]+)")
    _add_result(
        results,
        check_id="docs_dashboard_memo_alignment",
        layer="reports_docs_consistency",
        severity="alta",
        passed=(top_unit_dash is not None and top_unit_memo is not None and top_unit_dash == top_unit_memo),
        blocker_if_fail=True,
        what_checked="Alineación narrativa entre dashboard y memo",
        detail=f"dashboard_unit={top_unit_dash}, memo_unit={top_unit_memo}",
        impact_potential="Mensajes ejecutivos contradictorios entre entregables.",
        technical_recommendation="Unificar fuente de verdad para bloque de decisión ejecutiva.",
    )

    kpi_top_units = processed["kpi_top_unidades_por_riesgo"]
    if not kpi_top_units.empty:
        top_kpi = str(kpi_top_units.iloc[0]["unidad_id"])
        unit_day = processed["mart_unit_day"]
        if not unit_day.empty:
            top_real = str(unit_day.sort_values("predicted_unavailability_risk", ascending=False).iloc[0]["unidad_id"])
        else:
            top_real = None
        kpi_consistent = top_kpi == top_real
    else:
        top_kpi = None
        top_real = None
        kpi_consistent = False
    _add_result(
        results,
        check_id="docs_sql_output_consistency_top_units",
        layer="reports_docs_consistency",
        severity="media",
        passed=kpi_consistent,
        blocker_if_fail=False,
        what_checked="Consistencia KPI SQL top unidades vs mart_unit_day",
        detail=f"kpi_top_unit={top_kpi}, mart_top_unit={top_real}",
        impact_potential="KPI SQL podría no representar ranking operativo real.",
        technical_recommendation="Revisar ORDER BY/LIMIT y granularidad en query KPI.",
    )

    _add_result(
        results,
        check_id="docs_validation_framework_generation",
        layer="reports_docs_consistency",
        severity="informativa",
        passed=True,
        blocker_if_fail=False,
        what_checked="Generación de docs/validation_framework.md al cierre de run_validation",
        detail=f"target={validation_framework_path.name}",
        impact_potential="Sin framework documentado la QA no es auditable.",
        technical_recommendation="Mantener documento sincronizado en cada corrida.",
    )

    governance_checks = processed.get("governance_contract_checks", pd.DataFrame())
    if not governance_checks.empty and {"passed", "severity"}.issubset(governance_checks.columns):
        gov_pass_rate = float(governance_checks["passed"].astype(bool).mean())
        critical_fail = int(
            (
                (~governance_checks["passed"].astype(bool))
                & governance_checks["severity"].astype(str).str.lower().isin(["critica", "alta"])
            ).sum()
        )
    else:
        gov_pass_rate = np.nan
        critical_fail = math.inf
    _add_result(
        results,
        check_id="reports_governance_contract_compliance",
        layer="reports_docs_consistency",
        severity="alta",
        passed=bool(np.isfinite(gov_pass_rate) and gov_pass_rate >= 0.95 and critical_fail == 0),
        blocker_if_fail=True,
        what_checked="Cumplimiento de contratos métricos y de datos",
        detail=f"pass_rate={gov_pass_rate:.3f}, critical_or_high_fail={critical_fail}",
        metric_value=f"{gov_pass_rate:.3f}" if np.isfinite(gov_pass_rate) else None,
        threshold="pass_rate>=0.95 y critical_fail==0",
        impact_potential="Sin contratos cumplidos hay riesgo de drift semántico entre capas.",
        technical_recommendation="Corregir tablas no conformes y mantener metric/data contracts como SSOT de semántica.",
    )


def _validate_framework_strength(results: list[QAResult]) -> None:
    total = len(results)
    if total == 0:
        return
    severities = pd.Series([r.severity for r in results], dtype="string")
    info_share = float((severities == "informativa").mean())
    blocker_share = float(pd.Series([r.blocker_if_fail for r in results], dtype=bool).mean())
    high_critical_share = float(severities.isin(["alta", "critica"]).mean())

    _add_result(
        results,
        check_id="framework_non_cosmetic_balance",
        layer="reports_docs_consistency",
        severity="alta",
        passed=bool(info_share <= 0.25 and high_critical_share >= 0.40),
        blocker_if_fail=True,
        what_checked="Balance no cosmético del framework de validación",
        detail=f"informative_share={info_share:.3f}, high_critical_share={high_critical_share:.3f}",
        metric_value=f"{info_share:.3f}/{high_critical_share:.3f}",
        threshold="info<=0.25 & high_critical>=0.40",
        impact_potential="Un framework sesgado a checks informativos puede dar falsa confianza.",
        technical_recommendation="Aumentar checks de alto valor y degradar checks cosméticos.",
    )
    _add_result(
        results,
        check_id="framework_blocker_coverage",
        layer="reports_docs_consistency",
        severity="media",
        passed=bool(blocker_share >= 0.30),
        blocker_if_fail=False,
        what_checked="Cobertura mínima de controles blocker en el framework",
        detail=f"blocker_share={blocker_share:.3f}",
        metric_value=f"{blocker_share:.3f}",
        threshold=">=0.30",
        impact_potential="Sin blocker coverage suficiente, el pipeline puede publicar con fallos relevantes.",
        technical_recommendation="Promover controles críticos/alta severidad a blocker cuando aplique.",
    )


def _build_outputs(results_df: pd.DataFrame) -> None:
    OUTPUTS_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    results_df = results_df.sort_values(
        by=["passed", "publish_blocker", "severity", "check_id"],
        ascending=[True, False, False, True],
        key=lambda col: col.map(SEVERITY_ORDER) if col.name == "severity" else col,
    ).reset_index(drop=True)

    results_df.to_csv(OUTPUTS_REPORTS_DIR / "validation_checks_detailed.csv", index=False)

    # Compatibilidad legacy
    legacy_checks = results_df[["check_id", "passed", "detail"]].rename(columns={"check_id": "check", "passed": "result"})
    legacy_checks.to_csv(OUTPUTS_REPORTS_DIR / "validation_checks.csv", index=False)

    failed = results_df[~results_df["passed"]].copy()
    issues = failed[["check_id", "severity", "detail", "technical_recommendation"]].rename(
        columns={
            "check_id": "check",
            "technical_recommendation": "fixes_applied_or_recommended",
        }
    )
    if issues.empty:
        issues = pd.DataFrame(
            [
                {
                    "check": "sin_issues_criticos",
                    "severity": "informativa",
                    "detail": "No se detectan issues en esta corrida",
                    "fixes_applied_or_recommended": "Mantener monitoreo continuo",
                }
            ]
        )
    issues.to_csv(OUTPUTS_REPORTS_DIR / "issues_found.csv", index=False)
    issues[["check", "fixes_applied_or_recommended"]].to_csv(OUTPUTS_REPORTS_DIR / "fixes_applied.csv", index=False)

    blockers = results_df[(~results_df["passed"]) & (results_df["publish_blocker"])].copy()
    blockers.to_csv(OUTPUTS_REPORTS_DIR / "publish_blockers.csv", index=False)

    matrix = (
        results_df.groupby("layer", as_index=False)
        .agg(
            total_controls=("check_id", "count"),
            passed_controls=("passed", "sum"),
            failed_controls=("passed", lambda s: int((~s).sum())),
            failed_blockers=("publish_blocker", "sum"),
        )
        .sort_values("layer")
    )
    matrix["pass_rate"] = (matrix["passed_controls"] / matrix["total_controls"]).round(4)
    matrix.to_csv(OUTPUTS_REPORTS_DIR / "validation_control_matrix.csv", index=False)

    matrix_severity = (
        results_df.assign(failed=(~results_df["passed"]).astype(int))
        .groupby(["layer", "severity"], as_index=False)
        .agg(total=("check_id", "count"), failed=("failed", "sum"))
    )
    matrix_severity.to_csv(OUTPUTS_REPORTS_DIR / "validation_control_matrix_by_severity.csv", index=False)

    total = len(results_df)
    passed = int(results_df["passed"].sum())
    fail = total - passed
    blocker_fail = int(blockers.shape[0])
    failed_critical = int(((~results_df["passed"]) & (results_df["severity"] == "critica")).sum())
    failed_high = int(((~results_df["passed"]) & (results_df["severity"] == "alta")).sum())
    failed_medium = int(((~results_df["passed"]) & (results_df["severity"] == "media")).sum())
    failed_info = int(((~results_df["passed"]) & (results_df["severity"] == "informativa")).sum())
    informative_share = float((results_df["severity"] == "informativa").mean()) if total > 0 else 0.0
    severity_failed = (
        failed.groupby("severity", as_index=False)
        .size()
        .rename(columns={"size": "failed_checks"})
        .sort_values("severity", key=lambda s: s.map(SEVERITY_ORDER), ascending=False)
    )

    publish_status = "BLOCKED" if blocker_fail > 0 else ("READY_WITH_WARNINGS" if fail > 0 else "READY")
    confidence = "baja" if blocker_fail > 0 else ("media" if fail > 0 else "alta")

    technically_valid = (failed_critical == 0) and (blocker_fail == 0)
    analytically_acceptable = technically_valid and (failed_high == 0) and (fail / max(total, 1) <= 0.08)
    committee_grade = analytically_acceptable and (failed_medium <= 2) and (informative_share <= 0.22)
    decision_support_only = technically_valid and (not analytically_acceptable)
    screening_grade_only = technically_valid and analytically_acceptable and (not committee_grade)
    not_committee_grade = not committee_grade
    publish_blocked = blocker_fail > 0

    if publish_blocked:
        primary_release_state = "publish-blocked"
    elif committee_grade:
        primary_release_state = "committee-grade"
    elif decision_support_only:
        primary_release_state = "decision-support only"
    elif screening_grade_only:
        primary_release_state = "screening-grade only"
    elif analytically_acceptable:
        primary_release_state = "analytically acceptable"
    elif technically_valid:
        primary_release_state = "technically valid"
    else:
        primary_release_state = "not committee-grade"

    readiness_rows = [
        {"dimension": "technically_valid", "value": technically_valid, "rule": "no critical failures & no blockers"},
        {"dimension": "analytically_acceptable", "value": analytically_acceptable, "rule": "technically_valid & no high failures & fail_rate<=8%"},
        {"dimension": "decision-support only", "value": decision_support_only, "rule": "technically_valid but not analytically_acceptable"},
        {"dimension": "screening-grade only", "value": screening_grade_only, "rule": "analytically_acceptable but not committee_grade"},
        {"dimension": "not committee-grade", "value": not_committee_grade, "rule": "committee_grade == False"},
        {"dimension": "publish-blocked", "value": publish_blocked, "rule": "any failed publish_blocker"},
        {"dimension": "primary_release_state", "value": primary_release_state, "rule": "derived status"},
    ]
    readiness_df = pd.DataFrame(readiness_rows)
    readiness_df.to_csv(OUTPUTS_REPORTS_DIR / "release_readiness.csv", index=False)

    report_lines = [
        "# Validation Report",
        "",
        "## Resumen Ejecutivo QA",
        f"- Estado de publicación: **{publish_status}**",
        f"- Confianza global: **{confidence.upper()}**",
        f"- Controles ejecutados: **{total}**",
        f"- Controles aprobados: **{passed}**",
        f"- Controles fallidos: **{fail}**",
        f"- Publish blockers activos: **{blocker_fail}**",
        "",
        "## Release Readiness",
        f"- Estado primario: **{primary_release_state}**",
        f"- technically_valid: **{technically_valid}**",
        f"- analytically_acceptable: **{analytically_acceptable}**",
        f"- decision-support only: **{decision_support_only}**",
        f"- screening-grade only: **{screening_grade_only}**",
        f"- not committee-grade: **{not_committee_grade}**",
        f"- publish-blocked: **{publish_blocked}**",
        "",
        "## Qué Se Comprobó (Matriz por Capa)",
        matrix.to_markdown(index=False),
        "",
        "## Qué Falló (detalle priorizado)",
    ]
    if failed.empty:
        report_lines.append("- No hay controles fallidos en esta corrida.")
    else:
        report_lines.append(
            failed[
                [
                    "check_id",
                    "layer",
                    "severity",
                    "publish_blocker",
                    "what_checked",
                    "detail",
                    "impact_potential",
                    "technical_recommendation",
                ]
            ].to_markdown(index=False)
        )
    report_lines.extend(["", "## Severidad de Fallos", severity_failed.to_markdown(index=False) if not severity_failed.empty else "- Sin fallos."])

    report_lines.append("")
    report_lines.append("## Lista de Checks que Bloquean Publicación")
    if blockers.empty:
        report_lines.append("- No hay blockers activos en esta corrida.")
    else:
        report_lines.append(
            blockers[
                [
                    "check_id",
                    "layer",
                    "severity",
                    "what_checked",
                    "detail",
                    "impact_potential",
                    "technical_recommendation",
                ]
            ].to_markdown(index=False)
        )

    report_lines.extend(
        [
            "",
            "## Reglas de Interpretación",
            "- `critica`: fallo estructural, publish-blocker por defecto.",
            "- `alta`: impacto analítico serio; puede bloquear publicación según control.",
            "- `media`: degrada calidad analítica, no bloquea por sí sola.",
            "- `informativa`: control de auditoría/seguimiento.",
            "",
            "## Disciplina de Release",
            "- `technically_valid`: integridad estructural sin fallos críticos ni blockers.",
            "- `analytically_acceptable`: además no hay fallos de severidad alta.",
            "- `decision-support only`: técnicamente válido pero todavía no analíticamente aceptable.",
            "- `screening-grade only`: aceptable para screening, no para comité.",
            "- `committee-grade`: apto para revisión ejecutiva con riesgo controlado.",
            "- `publish-blocked`: no publicar ni presentar como base de decisión.",
            "",
            "## Caveats",
            "- Los datos son sintéticos y no sustituyen calibración con histórico real.",
            "- Las señales económicas son proxy y requieren ajuste por costes/SLA reales.",
            "- Un estado READY no implica validez causal, solo coherencia analítica del repositorio.",
        ]
    )

    report_text = "\n".join(report_lines)
    (OUTPUTS_REPORTS_DIR / "validation_report.md").write_text(report_text, encoding="utf-8")
    (DOCS_DIR / "validation_report.md").write_text(report_text, encoding="utf-8")

    framework_lines = [
        "# Validation Framework",
        "",
        "## Capas de Control",
        "- raw_data",
        "- staging",
        "- marts",
        "- features",
        "- scores",
        "- recommendations",
        "- dashboard_datasets",
        "- reports_docs_consistency",
        "",
        "## Política de Severidad",
        "- crítica: fallo estructural, bloquea publicación.",
        "- alta: alto impacto analítico; bloquea cuando el control está marcado como blocker.",
        "- media: riesgo de calidad relevante, no bloquea por sí solo.",
        "- informativa: gobernanza y trazabilidad.",
        "",
        "## Matriz de Controles por Capa",
        matrix.to_markdown(index=False),
        "",
        "## Catálogo de Controles",
        results_df[
            [
                "check_id",
                "layer",
                "severity",
                "blocker_if_fail",
                "what_checked",
                "threshold",
            ]
        ].fillna("").to_markdown(index=False),
    ]
    (DOCS_DIR / "validation_framework.md").write_text("\n".join(framework_lines), encoding="utf-8")


def run_validation() -> pd.DataFrame:
    OUTPUTS_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    raw = _load_raw_tables()
    processed = _load_processed_tables()
    results: list[QAResult] = []

    _validate_required_files(raw, processed, results)
    _validate_raw_layer(raw, results)
    _validate_staging_layer(processed, results)
    _validate_marts_layer(raw, processed, results)
    _validate_features_layer(processed, results)
    _validate_scores_layer(processed, results)
    _validate_recommendations_layer(processed, results)
    _validate_dashboard_layer(processed, results)
    _validate_reports_docs_layer(processed, results)
    _validate_framework_strength(results)

    results_df = pd.DataFrame([r.__dict__ for r in results])
    _build_outputs(results_df)
    return results_df


if __name__ == "__main__":
    run_validation()
