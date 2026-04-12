from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.config import DATA_PROCESSED_DIR, DATA_RAW_DIR, DOCS_DIR, OUTPUTS_REPORTS_DIR, ROOT_DIR


@dataclass(frozen=True)
class MetricSpec:
    metric_id: str
    label: str
    unit: str
    source_of_truth: str
    window_definition: str
    filter_definition: str
    aggregation_definition: str
    definition: str


NARRATIVE_METRIC_SPECS = [
    MetricSpec(
        metric_id="fleet_availability_pct",
        label="Disponibilidad media de flota",
        unit="pct",
        source_of_truth="data/processed/fleet_week_features.csv",
        window_definition="histórico completo semanal disponible",
        filter_definition="todos los registros válidos",
        aggregation_definition="mean(availability_rate) * 100",
        definition="Disponibilidad promedio de flota para narrativa ejecutiva.",
    ),
    MetricSpec(
        metric_id="mtbf_proxy_hours",
        label="MTBF proxy",
        unit="hours",
        source_of_truth="data/processed/fleet_week_features.csv",
        window_definition="histórico completo semanal disponible",
        filter_definition="todos los registros válidos",
        aggregation_definition="mean(mtbf_proxy)",
        definition="Tiempo medio entre fallas proxy consolidado.",
    ),
    MetricSpec(
        metric_id="mttr_proxy_hours",
        label="MTTR proxy",
        unit="hours",
        source_of_truth="data/processed/fleet_week_features.csv",
        window_definition="histórico completo semanal disponible",
        filter_definition="todos los registros válidos",
        aggregation_definition="mean(mttr_proxy)",
        definition="Tiempo medio de reparación proxy consolidado.",
    ),
    MetricSpec(
        metric_id="high_risk_units_count",
        label="Unidades de alto riesgo",
        unit="count",
        source_of_truth="data/processed/unit_unavailability_risk_score.csv",
        window_definition="snapshot actual por unidad",
        filter_definition="unit_unavailability_risk_score >= 70",
        aggregation_definition="count(*)",
        definition="Número de unidades que superan umbral de riesgo alto.",
    ),
    MetricSpec(
        metric_id="backlog_physical_items_count",
        label="Backlog físico",
        unit="count",
        source_of_truth="data/raw/backlog_mantenimiento.csv",
        window_definition="última fecha disponible de backlog",
        filter_definition="todos los pendientes abiertos del día",
        aggregation_definition="count(*)",
        definition="Pendientes reales de mantenimiento aún no resueltos.",
    ),
    MetricSpec(
        metric_id="backlog_overdue_items_count",
        label="Backlog vencido",
        unit="count",
        source_of_truth="data/raw/backlog_mantenimiento.csv",
        window_definition="última fecha disponible de backlog",
        filter_definition="antiguedad_backlog_dias >= 14",
        aggregation_definition="count(*)",
        definition="Pendientes físicos fuera de ventana operativa objetivo.",
    ),
    MetricSpec(
        metric_id="backlog_critical_physical_count",
        label="Backlog crítico físico",
        unit="count",
        source_of_truth="data/raw/backlog_mantenimiento.csv",
        window_definition="última fecha disponible de backlog",
        filter_definition="antiguedad>=21 con severidad alta/crítica o riesgo_acumulado>=70",
        aggregation_definition="count(*)",
        definition="Pendientes físicos con criticidad estructural por edad/severidad.",
    ),
    MetricSpec(
        metric_id="high_deferral_risk_cases_count",
        label="Casos de alto riesgo de diferimiento",
        unit="count",
        source_of_truth="data/processed/workshop_priority_table.csv",
        window_definition="snapshot actual de priorización",
        filter_definition="deferral_risk_score >= 70",
        aggregation_definition="count(*)",
        definition="Casos con alta probabilidad de daño operativo si se difieren.",
    ),
    MetricSpec(
        metric_id="cbm_vs_reactiva_availability_pp",
        label="CBM vs reactiva: mejora de disponibilidad",
        unit="pp",
        source_of_truth="data/processed/comparativo_estrategias.csv",
        window_definition="escenario comparativo de estrategia",
        filter_definition="estrategia in (reactiva, basada_en_condicion)",
        aggregation_definition="fleet_availability(CBM) - fleet_availability(reactiva)",
        definition="Diferencia de disponibilidad entre CBM y estrategia reactiva.",
    ),
    MetricSpec(
        metric_id="cbm_operational_savings_eur",
        label="Ahorro operativo CBM vs reactiva",
        unit="eur",
        source_of_truth="data/processed/comparativo_estrategias.csv",
        window_definition="escenario comparativo de estrategia",
        filter_definition="estrategia in (reactiva, basada_en_condicion)",
        aggregation_definition="coste_operativo_proxy(reactiva) - coste_operativo_proxy(CBM)",
        definition="Ahorro operativo proxy atribuible a estrategia CBM.",
    ),
    MetricSpec(
        metric_id="cbm_value_range_min_eur",
        label="CBM vs reactiva: ahorro neto mínimo plausible (P10)",
        unit="eur",
        source_of_truth="data/processed/comparativo_estrategias.csv",
        window_definition="escenario comparativo de estrategia con sensibilidad",
        filter_definition="estrategia = basada_en_condicion",
        aggregation_definition="rango_plausible_valor_min(CBM)",
        definition="Límite inferior plausible de ahorro CBM frente a reactiva bajo sensibilidad.",
    ),
    MetricSpec(
        metric_id="cbm_value_range_max_eur",
        label="CBM vs reactiva: ahorro neto máximo plausible (P90)",
        unit="eur",
        source_of_truth="data/processed/comparativo_estrategias.csv",
        window_definition="escenario comparativo de estrategia con sensibilidad",
        filter_definition="estrategia = basada_en_condicion",
        aggregation_definition="rango_plausible_valor_max(CBM)",
        definition="Límite superior plausible de ahorro CBM frente a reactiva bajo sensibilidad.",
    ),
    MetricSpec(
        metric_id="cbm_prob_positive_savings",
        label="CBM vs reactiva: probabilidad de ahorro positivo",
        unit="ratio_0_1",
        source_of_truth="data/processed/comparativo_estrategias.csv",
        window_definition="escenario comparativo de estrategia con sensibilidad",
        filter_definition="estrategia = basada_en_condicion",
        aggregation_definition="prob_ahorro_positivo(CBM)",
        definition="Robustez del caso CBM: share de simulaciones con ahorro positivo frente a reactiva.",
    ),
    MetricSpec(
        metric_id="avoidable_downtime_hours_inspection",
        label="Horas de indisponibilidad evitables por inspección automática",
        unit="hours",
        source_of_truth="data/processed/inspection_module_value_comparison.csv",
        window_definition="escenario comparativo inspección automática",
        filter_definition="scenario in (sin_inspeccion_automatica, con_inspeccion_automatica)",
        aggregation_definition="horas_indisponibilidad(sin) - horas_indisponibilidad(con)",
        definition="Horas potencialmente evitadas por módulo de inspección automática.",
    ),
    MetricSpec(
        metric_id="avoidable_correctives_inspection",
        label="Correctivas evitables por inspección automática",
        unit="count_proxy",
        source_of_truth="data/processed/inspection_module_value_comparison.csv",
        window_definition="escenario comparativo inspección automática",
        filter_definition="scenario in (sin_inspeccion_automatica, con_inspeccion_automatica)",
        aggregation_definition="correctivas(sin) - correctivas(con)",
        definition="Correctivas proxy que se evitarían con inspección automática.",
    ),
    MetricSpec(
        metric_id="mean_depot_saturation_pct",
        label="Saturación media de depósitos",
        unit="pct",
        source_of_truth="data/processed/vw_depot_maintenance_pressure.csv",
        window_definition="última fecha disponible",
        filter_definition="fecha = max(fecha)",
        aggregation_definition="mean(saturation_ratio) * 100",
        definition="Saturación media de taller en snapshot más reciente.",
    ),
    MetricSpec(
        metric_id="backlog_exposure_adjusted_mean",
        label="Exposure backlog-adjusted medio",
        unit="score_0_100",
        source_of_truth="data/processed/vw_depot_maintenance_pressure.csv",
        window_definition="última fecha con backlog físico disponible",
        filter_definition="fecha = max(fecha) alineada con snapshot de backlog",
        aggregation_definition="mean(backlog_exposure_adjusted_score)",
        definition="Exposición compuesta de backlog físico (cantidad+edad+criticidad).",
    ),
    MetricSpec(
        metric_id="top_unit_by_priority",
        label="Unidad prioritaria",
        unit="id",
        source_of_truth="data/processed/workshop_priority_table.csv",
        window_definition="snapshot actual de priorización",
        filter_definition="top 1 ordenado por intervention_priority_score desc, deferral_risk_score desc",
        aggregation_definition="first(unidad_id)",
        definition="Unidad con mayor prioridad de entrada a taller.",
    ),
    MetricSpec(
        metric_id="top_component_by_priority",
        label="Componente prioritario",
        unit="id",
        source_of_truth="data/processed/workshop_priority_table.csv",
        window_definition="snapshot actual de priorización",
        filter_definition="misma fila top_unit_by_priority",
        aggregation_definition="first(componente_id)",
        definition="Componente que debe intervenirse primero según score consolidado.",
    ),
    MetricSpec(
        metric_id="top_component_family_by_priority",
        label="Familia de componente prioritario",
        unit="label",
        source_of_truth="data/processed/scoring_componentes.csv + data/processed/workshop_priority_table.csv",
        window_definition="snapshot actual",
        filter_definition="misma fila top_unit_by_priority",
        aggregation_definition="lookup component_family",
        definition="Familia del componente prioritario para narrativa ejecutiva.",
    ),
    MetricSpec(
        metric_id="top_depot_by_saturation",
        label="Depósito más saturado",
        unit="id",
        source_of_truth="data/processed/vw_depot_maintenance_pressure.csv",
        window_definition="última fecha disponible",
        filter_definition="fecha = max(fecha)",
        aggregation_definition="argmax(saturation_ratio)",
        definition="Depósito con mayor saturación de taller.",
    ),
    MetricSpec(
        metric_id="top_depot_saturation_pct",
        label="Saturación del depósito más exigido",
        unit="pct",
        source_of_truth="data/processed/vw_depot_maintenance_pressure.csv",
        window_definition="última fecha disponible",
        filter_definition="fecha = max(fecha)",
        aggregation_definition="max(saturation_ratio) * 100",
        definition="Porcentaje de saturación del depósito más exigido.",
    ),
    MetricSpec(
        metric_id="deferral_cost_delta_14d_eur",
        label="Impacto de diferimiento a 14 días (coste)",
        unit="eur",
        source_of_truth="data/processed/impacto_diferimiento_resumen.csv",
        window_definition="escenarios de diferimiento",
        filter_definition="comparación defer_dias = 14 vs defer_dias = 0",
        aggregation_definition="coste_total_eur(14) - coste_total_eur(0)",
        definition="Incremento de coste proxy por diferir 14 días.",
    ),
    MetricSpec(
        metric_id="deferral_downtime_delta_14d_h",
        label="Impacto de diferimiento a 14 días (downtime)",
        unit="hours",
        source_of_truth="data/processed/impacto_diferimiento_resumen.csv",
        window_definition="escenarios de diferimiento",
        filter_definition="comparación defer_dias = 14 vs defer_dias = 0",
        aggregation_definition="downtime_total_h(14) - downtime_total_h(0)",
        definition="Incremento de horas de indisponibilidad por diferir 14 días.",
    ),
    MetricSpec(
        metric_id="top_priority_score",
        label="Score de prioridad de intervención (top)",
        unit="score_0_100",
        source_of_truth="data/processed/workshop_priority_table.csv",
        window_definition="snapshot actual de priorización",
        filter_definition="fila top por prioridad",
        aggregation_definition="first(intervention_priority_score)",
        definition="Score de la intervención prioritaria.",
    ),
    MetricSpec(
        metric_id="top_deferral_risk_score",
        label="Score de riesgo por diferimiento (top)",
        unit="score_0_100",
        source_of_truth="data/processed/workshop_priority_table.csv",
        window_definition="snapshot actual de priorización",
        filter_definition="fila top por prioridad",
        aggregation_definition="first(deferral_risk_score)",
        definition="Riesgo de diferir asociado al caso prioritario.",
    ),
]


def _format_float(value: float, digits: int = 2) -> str:
    return f"{value:.{digits}f}"


def _format_int(value: float) -> str:
    return str(int(round(value)))


def _format_million_eur(value: float, digits: int = 2) -> str:
    return f"{value / 1_000_000:.{digits}f}"


def _load_inputs() -> dict[str, pd.DataFrame]:
    return {
        "fleet_week": pd.read_csv(DATA_PROCESSED_DIR / "fleet_week_features.csv"),
        "unit_risk": pd.read_csv(DATA_PROCESSED_DIR / "unit_unavailability_risk_score.csv"),
        "priorities": pd.read_csv(DATA_PROCESSED_DIR / "workshop_priority_table.csv"),
        "scoring": pd.read_csv(DATA_PROCESSED_DIR / "scoring_componentes.csv"),
        "strategy": pd.read_csv(DATA_PROCESSED_DIR / "comparativo_estrategias.csv"),
        "inspection_value": pd.read_csv(DATA_PROCESSED_DIR / "inspection_module_value_comparison.csv"),
        "deferral": pd.read_csv(DATA_PROCESSED_DIR / "impacto_diferimiento_resumen.csv"),
        "depot_pressure": pd.read_csv(DATA_PROCESSED_DIR / "vw_depot_maintenance_pressure.csv"),
        "backlog_raw": pd.read_csv(DATA_RAW_DIR / "backlog_mantenimiento.csv"),
        "flotas": pd.read_csv(DATA_RAW_DIR / "flotas.csv"),
        "unidades": pd.read_csv(DATA_RAW_DIR / "unidades.csv"),
        "depositos": pd.read_csv(DATA_RAW_DIR / "depositos.csv"),
        "componentes": pd.read_csv(DATA_RAW_DIR / "componentes_criticos.csv"),
    }


def _safe_strategy_row(strategy: pd.DataFrame, name: str) -> pd.Series:
    row = strategy[strategy["estrategia"] == name]
    if row.empty:
        raise ValueError(f"No se encuentra estrategia requerida: {name}")
    return row.iloc[0]


def _compute_metrics_values(inputs: dict[str, pd.DataFrame]) -> dict[str, Any]:
    fleet_week = inputs["fleet_week"].copy()
    unit_risk = inputs["unit_risk"].copy()
    priorities = inputs["priorities"].copy()
    scoring = inputs["scoring"].copy()
    strategy = inputs["strategy"].copy()
    inspection_value = inputs["inspection_value"].copy()
    deferral = inputs["deferral"].copy()
    depot_pressure = inputs["depot_pressure"].copy()
    backlog_raw = inputs["backlog_raw"].copy()

    fleet_week["week_start"] = pd.to_datetime(fleet_week["week_start"], errors="coerce")
    if "fecha" in priorities.columns:
        priorities["fecha"] = pd.to_datetime(priorities["fecha"], errors="coerce")
    depot_pressure["fecha"] = pd.to_datetime(depot_pressure["fecha"], errors="coerce")
    backlog_raw["fecha"] = pd.to_datetime(backlog_raw["fecha"], errors="coerce")

    react = _safe_strategy_row(strategy, "reactiva")
    cbm = _safe_strategy_row(strategy, "basada_en_condicion")

    sin_insp = inspection_value[inspection_value["scenario"] == "sin_inspeccion_automatica"].iloc[0]
    con_insp = inspection_value[inspection_value["scenario"] == "con_inspeccion_automatica"].iloc[0]

    latest_backlog_day = backlog_raw["fecha"].max()
    latest_depot_date = depot_pressure["fecha"].max()
    latest_depot = depot_pressure[depot_pressure["fecha"] == latest_depot_date].copy()
    if "backlog_critical_items" not in latest_depot.columns:
        latest_depot["backlog_critical_items"] = 0
    latest_depot = latest_depot.sort_values(["saturation_ratio", "backlog_critical_items"], ascending=[False, False])

    backlog_depot = depot_pressure[depot_pressure["fecha"] == latest_backlog_day].copy()
    if backlog_depot.empty:
        backlog_depot = depot_pressure[depot_pressure["fecha"] <= latest_backlog_day].copy()
        if not backlog_depot.empty:
            backlog_depot = backlog_depot[backlog_depot["fecha"] == backlog_depot["fecha"].max()].copy()
        else:
            backlog_depot = latest_depot.copy()
    if "backlog_exposure_adjusted_score" not in backlog_depot.columns:
        backlog_depot["backlog_exposure_adjusted_score"] = backlog_depot.get("backlog_risk", 0).fillna(0).clip(0, 100)

    backlog_latest = backlog_raw[backlog_raw["fecha"] == latest_backlog_day].copy()
    backlog_latest["severidad_pendiente"] = backlog_latest["severidad_pendiente"].fillna("baja").str.lower()
    backlog_physical_items_count = int(len(backlog_latest))
    backlog_overdue_items_count = int((backlog_latest["antiguedad_backlog_dias"].fillna(0) >= 14).sum())
    backlog_critical_mask = (
        (
            backlog_latest["antiguedad_backlog_dias"].fillna(0) >= 21
        )
        & backlog_latest["severidad_pendiente"].isin(["alta", "critica"])
    ) | (backlog_latest["riesgo_acumulado"].fillna(0) >= 70)
    backlog_critical_physical_count = int(backlog_critical_mask.sum())
    high_deferral_risk_cases_count = int((priorities["deferral_risk_score"].fillna(0) >= 70).sum())

    top_priority = priorities.sort_values(
        ["intervention_priority_score", "deferral_risk_score", "service_impact_score", "unidad_id", "componente_id"],
        ascending=[False, False, False, True, True],
    ).iloc[0]

    top_family_row = scoring[
        (scoring["unidad_id"] == top_priority["unidad_id"]) & (scoring["componente_id"] == top_priority["componente_id"])
    ]
    top_component_family = top_family_row.iloc[0]["component_family"] if not top_family_row.empty else "unknown"

    d0 = deferral.loc[deferral["defer_dias"] == 0].iloc[0]
    d14 = deferral.loc[deferral["defer_dias"] == 14].iloc[0]

    values = {
        "as_of_ts": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "coverage_start": fleet_week["week_start"].min().date().isoformat(),
        "coverage_end": fleet_week["week_start"].max().date().isoformat(),
        "fleet_availability_pct": float(fleet_week["availability_rate"].mean() * 100),
        "mtbf_proxy_hours": float(fleet_week["mtbf_proxy"].mean()),
        "mttr_proxy_hours": float(fleet_week["mttr_proxy"].mean()),
        "high_risk_units_count": int((unit_risk["unit_unavailability_risk_score"] >= 70).sum()),
        "backlog_physical_items_count": backlog_physical_items_count,
        "backlog_overdue_items_count": backlog_overdue_items_count,
        "backlog_critical_physical_count": backlog_critical_physical_count,
        "high_deferral_risk_cases_count": high_deferral_risk_cases_count,
        "cbm_vs_reactiva_availability_pp": float(cbm["fleet_availability"] - react["fleet_availability"]),
        "cbm_operational_savings_eur": float(react["coste_operativo_proxy"] - cbm["coste_operativo_proxy"]),
        "cbm_value_range_min_eur": float(cbm.get("rango_plausible_valor_min", np.nan)),
        "cbm_value_range_max_eur": float(cbm.get("rango_plausible_valor_max", np.nan)),
        "cbm_prob_positive_savings": float(cbm.get("prob_ahorro_positivo", np.nan)),
        "avoidable_downtime_hours_inspection": float(
            sin_insp["horas_indisponibilidad_estimadas"] - con_insp["horas_indisponibilidad_estimadas"]
        ),
        "avoidable_correctives_inspection": float(sin_insp["correctivas_estimadas"] - con_insp["correctivas_estimadas"]),
        "mean_depot_saturation_pct": float(latest_depot["saturation_ratio"].mean() * 100),
        "backlog_exposure_adjusted_mean": float(backlog_depot["backlog_exposure_adjusted_score"].mean()),
        "top_unit_by_priority": str(top_priority["unidad_id"]),
        "top_component_by_priority": str(top_priority["componente_id"]),
        "top_component_family_by_priority": str(top_component_family),
        "top_depot_by_saturation": str(latest_depot.iloc[0]["deposito_id"]),
        "top_depot_saturation_pct": float(latest_depot.iloc[0]["saturation_ratio"] * 100),
        "deferral_cost_delta_14d_eur": float(d14["costo_total_eur"] - d0["costo_total_eur"]),
        "deferral_downtime_delta_14d_h": float(d14["downtime_total_h"] - d0["downtime_total_h"]),
        "top_priority_score": float(top_priority["intervention_priority_score"]),
        "top_deferral_risk_score": float(top_priority["deferral_risk_score"]),
        "n_flotas": int(inputs["flotas"]["flota_id"].nunique()),
        "n_unidades": int(inputs["unidades"]["unidad_id"].nunique()),
        "n_depositos": int(inputs["depositos"]["deposito_id"].nunique()),
        "n_componentes": int(inputs["componentes"]["componente_id"].nunique()),
    }
    return values


def _metrics_to_dataframe(values: dict[str, Any]) -> pd.DataFrame:
    specs_by_id = {x.metric_id: x for x in NARRATIVE_METRIC_SPECS}
    rows: list[dict[str, Any]] = []
    for metric_id, value in values.items():
        if metric_id in {"as_of_ts", "coverage_start", "coverage_end", "n_flotas", "n_unidades", "n_depositos", "n_componentes"}:
            rows.append(
                {
                    "metric_id": metric_id,
                    "metric_value": value,
                    "label": metric_id,
                    "unit": "meta",
                    "source_of_truth": "pipeline_runtime",
                    "window_definition": "n/a",
                    "filter_definition": "n/a",
                    "aggregation_definition": "n/a",
                    "definition": "Metadato de corrida.",
                }
            )
            continue

        spec = specs_by_id[metric_id]
        rows.append(
            {
                "metric_id": metric_id,
                "metric_value": value,
                "label": spec.label,
                "unit": spec.unit,
                "source_of_truth": spec.source_of_truth,
                "window_definition": spec.window_definition,
                "filter_definition": spec.filter_definition,
                "aggregation_definition": spec.aggregation_definition,
                "definition": spec.definition,
            }
        )
    return pd.DataFrame(rows)


def _metric_lookup(df: pd.DataFrame) -> dict[str, Any]:
    lookup: dict[str, Any] = {}
    for _, row in df.iterrows():
        lookup[str(row["metric_id"])] = row["metric_value"]
    return lookup


def load_or_compute_narrative_metrics(force_recompute: bool = False) -> dict[str, Any]:
    metrics_path = DATA_PROCESSED_DIR / "narrative_metrics_official.csv"
    if metrics_path.exists() and not force_recompute:
        return _metric_lookup(pd.read_csv(metrics_path))

    inputs = _load_inputs()
    values = _compute_metrics_values(inputs)
    df = _metrics_to_dataframe(values)

    DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUTS_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(metrics_path, index=False)
    df.to_csv(OUTPUTS_REPORTS_DIR / "narrative_metrics_official.csv", index=False)
    return values


def _build_kpis_ejecutivos(metrics: dict[str, Any]) -> pd.DataFrame:
    kpi_order = [
        "fleet_availability_pct",
        "mtbf_proxy_hours",
        "mttr_proxy_hours",
        "high_risk_units_count",
        "backlog_physical_items_count",
        "backlog_overdue_items_count",
        "backlog_critical_physical_count",
        "high_deferral_risk_cases_count",
        "backlog_exposure_adjusted_mean",
        "avoidable_downtime_hours_inspection",
        "avoidable_correctives_inspection",
        "cbm_operational_savings_eur",
        "cbm_value_range_min_eur",
        "cbm_value_range_max_eur",
        "cbm_prob_positive_savings",
        "mean_depot_saturation_pct",
        "deferral_cost_delta_14d_eur",
        "deferral_downtime_delta_14d_h",
    ]
    rows = []
    for k in kpi_order:
        rows.append({"kpi": k, "valor": metrics[k]})
    return pd.DataFrame(rows)


def _build_memo(metrics: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Memo Ejecutivo",
            "",
            "## 1. Contexto",
            "La red ferroviaria analizada opera con alta exigencia de disponibilidad y presión de taller en depósitos críticos.",
            "",
            "## 2. Problema",
            "Persisten fallas repetitivas y backlog técnico que elevan el riesgo de indisponibilidad y afectan servicio.",
            "",
            "## 3. Enfoque metodológico",
            "La narrativa se alimenta automáticamente desde métricas oficiales versionadas (SSOT),",
            "evitando divergencias entre README, dashboard y reportes de resultados.",
            "",
            "## 4. Hallazgos principales",
            f"- Disponibilidad media de flota: {_format_float(float(metrics['fleet_availability_pct']), 2)}%.",
            f"- Unidades de alto riesgo: {_format_int(float(metrics['high_risk_units_count']))}.",
            f"- Backlog físico: {_format_int(float(metrics['backlog_physical_items_count']))} pendientes.",
            f"- Backlog vencido: {_format_int(float(metrics['backlog_overdue_items_count']))} pendientes.",
            f"- Backlog crítico físico: {_format_int(float(metrics['backlog_critical_physical_count']))} pendientes.",
            f"- Casos de alto riesgo de diferimiento: {_format_int(float(metrics['high_deferral_risk_cases_count']))}.",
            f"- CBM vs reactiva: mejora de disponibilidad {_format_float(float(metrics['cbm_vs_reactiva_availability_pp']), 2)} p.p.",
            "",
            "## 5. Implicaciones operativas",
            "Intervenciones anticipadas en componentes de alto riesgo reducen indisponibilidad y sustituciones no planificadas.",
            "",
            "## 6. Implicaciones para taller",
            f"El depósito más saturado es {metrics['top_depot_by_saturation']} con {_format_float(float(metrics['top_depot_saturation_pct']), 1)}% de ocupación.",
            "",
            "## 7. Implicaciones económicas",
            f"Ahorro operativo proxy estimado CBM vs reactiva: {_format_int(float(metrics['cbm_operational_savings_eur']))} EUR.",
            (
                "Rango plausible de ahorro CBM vs reactiva: "
                f"{_format_int(float(metrics['cbm_value_range_min_eur']))} a "
                f"{_format_int(float(metrics['cbm_value_range_max_eur']))} EUR."
            ),
            (
                "Robustez del ahorro CBM (escenarios+sensitivity): "
                f"{_format_float(float(metrics['cbm_prob_positive_savings']) * 100, 1)}% de casos con ahorro positivo."
            ),
            "",
            "## 8. Trade-offs principales",
            (
                "Diferir 14 días incrementa coste en "
                f"{_format_int(float(metrics['deferral_cost_delta_14d_eur']))} EUR y downtime en "
                f"{_format_float(float(metrics['deferral_downtime_delta_14d_h']), 1)} h."
            ),
            (
                "Separación conceptual aplicada: backlog físico (cantidad/edad/severidad) "
                "y riesgo de diferimiento (score de decisión) se reportan por separado."
            ),
            "",
            "## 9. Prioridades de intervención",
            f"- Unidad prioritaria: {metrics['top_unit_by_priority']}.",
            f"- Componente prioritario: {metrics['top_component_by_priority']}.",
            f"- Familia técnica asociada: {metrics['top_component_family_by_priority']}.",
            "",
            "## 10. Limitaciones",
            "Datos sintéticos y costes proxy; los resultados no sustituyen calibración con datos reales de operación.",
            "",
            "## 11. Próximos pasos",
            "Validar umbrales con histórico real, incorporar optimización matemática de scheduling y cerrar loop con órdenes ejecutadas.",
        ]
    )


def _build_readme(metrics: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Sistema de Inteligencia de Mantenimiento Basado en Condición para Flota Ferroviaria",
            "",
            "Plataforma de decisión para mantenimiento ferroviario: identifica riesgo de indisponibilidad, prioriza entrada a taller y cuantifica el valor operativo del CBM con scoring interpretable.",
            "",
            "## El reto real",
            "La flota pierde disponibilidad por degradación, fallas repetitivas y backlog. La decisión crítica es **qué intervenir primero, qué pasa si se difiere y dónde el CBM aporta valor real**.",
            "",
            "## Lo que entrega",
            "Integra operación, sensores, inspección automática y mantenimiento para construir señales de salud, riesgo y RUL operativo. Con esas señales, ordena la cola de taller, estima el impacto de diferir y compara estrategias con sensibilidad.",
            "",
            "## Decisiones que habilita",
            "- Orden de entrada a taller por unidad y componente.",
            "- Intervenir ahora vs diferir con riesgo controlado.",
            "- Dónde escalar CBM y dónde el impacto es marginal.",
            "- Qué depósitos están saturados y requieren rebalanceo.",
            "",
            "## Arquitectura (en seis capas)",
            "1) Datos sintéticos realistas y modelo ferroviario.",
            "2) SQL por capas (staging → marts → KPIs).",
            "3) Feature engineering + scoring interpretable.",
            "4) Priorización y scheduling heurístico.",
            "5) Comparativa estratégica y análisis de diferimiento.",
            "6) Dashboard ejecutivo autocontenido.",
            "",
            "## Estructura del repositorio",
            "- `src/` lógica de datos, scoring y dashboard",
            "- `sql/` capa SQL",
            "- `data/` (raw/processed ignorado en GitHub)",
            "- `outputs/` dashboard y reportes ejecutivos",
            "- `docs/` documentación clave",
            "- `tests/` validación y QA",
            "",
            "## Outputs clave",
            "- `outputs/dashboard/index.html`",
            "- `outputs/reports/informe_analitico_avanzado.md`",
            "- `outputs/reports/memo_ejecutivo_es.md`",
            "- `outputs/reports/validation_report.md`",
            "- `docs/gobierno_metricas.md`",
            "",
            "## Por qué este proyecto es más sólido que un portfolio típico",
            "- Trazabilidad real desde señal técnica → score → decisión.",
            "- Gobernanza de métricas con contratos y checks publish‑blocker.",
            "- Enfoque operativo: priorización y secuenciación, no solo reporting.",
            "",
            "## Resultados clave (SSOT)",
            f"- disponibilidad media de flota: **{_format_float(float(metrics['fleet_availability_pct']), 2)}%**",
            f"- unidades de alto riesgo: **{_format_int(float(metrics['high_risk_units_count']))}**",
            f"- backlog físico: **{_format_int(float(metrics['backlog_physical_items_count']))} pendientes**",
            f"- backlog vencido: **{_format_int(float(metrics['backlog_overdue_items_count']))} pendientes**",
            f"- backlog crítico físico: **{_format_int(float(metrics['backlog_critical_physical_count']))} pendientes**",
            f"- casos alto riesgo de diferimiento: **{_format_int(float(metrics['high_deferral_risk_cases_count']))}**",
            "",
            "## Decisión actual (SSOT)",
            f"- **Unidad que debe entrar primero:** `{metrics['top_unit_by_priority']}`",
            f"- **Componente que debe sustituirse primero:** `{metrics['top_component_by_priority']}`",
            "",
            "## Cómo ejecutar",
            "```bash",
            "python -m src.run_pipeline",
            "python -m src.build_dashboard",
            "```",
            "",
            "## Limitaciones",
            "- Datos sintéticos; requieren calibración real.",
            "- Costes económicos en proxy.",
            "- Scheduling heurístico, no optimizador global.",
            "",
            "## Herramientas",
            "Python, SQL, DuckDB, pandas, Chart.js.",
        ]
    )


def _build_artifact_mapping() -> pd.DataFrame:
    rows = [
        ("README.md", "Key Findings", "fleet_availability_pct"),
        ("README.md", "Key Findings", "high_risk_units_count"),
        ("README.md", "Key Findings", "backlog_physical_items_count"),
        ("README.md", "Key Findings", "backlog_overdue_items_count"),
        ("README.md", "Key Findings", "backlog_critical_physical_count"),
        ("README.md", "Key Findings", "high_deferral_risk_cases_count"),
        ("README.md", "Key Findings", "cbm_vs_reactiva_availability_pp"),
        ("README.md", "Key Findings", "cbm_operational_savings_eur"),
        ("README.md", "Key Findings", "cbm_value_range_min_eur"),
        ("README.md", "Key Findings", "cbm_value_range_max_eur"),
        ("README.md", "Key Findings", "cbm_prob_positive_savings"),
        ("README.md", "Key Findings", "avoidable_downtime_hours_inspection"),
        ("README.md", "Decisión Final", "top_unit_by_priority"),
        ("README.md", "Decisión Final", "top_component_by_priority"),
        ("README.md", "Decisión Final", "top_component_family_by_priority"),
        ("README.md", "Decisión Final", "deferral_cost_delta_14d_eur"),
        ("README.md", "Decisión Final", "deferral_downtime_delta_14d_h"),
        ("docs/memo_ejecutivo_es.md", "Hallazgos", "fleet_availability_pct"),
        ("docs/memo_ejecutivo_es.md", "Hallazgos", "high_risk_units_count"),
        ("docs/memo_ejecutivo_es.md", "Hallazgos", "backlog_physical_items_count"),
        ("docs/memo_ejecutivo_es.md", "Hallazgos", "backlog_overdue_items_count"),
        ("docs/memo_ejecutivo_es.md", "Hallazgos", "backlog_critical_physical_count"),
        ("docs/memo_ejecutivo_es.md", "Hallazgos", "high_deferral_risk_cases_count"),
        ("docs/memo_ejecutivo_es.md", "Implicaciones económicas", "cbm_operational_savings_eur"),
        ("docs/memo_ejecutivo_es.md", "Implicaciones económicas", "cbm_value_range_min_eur"),
        ("docs/memo_ejecutivo_es.md", "Implicaciones económicas", "cbm_value_range_max_eur"),
        ("docs/memo_ejecutivo_es.md", "Implicaciones económicas", "cbm_prob_positive_savings"),
        ("docs/memo_ejecutivo_es.md", "Trade-offs", "deferral_cost_delta_14d_eur"),
        ("docs/memo_ejecutivo_es.md", "Trade-offs", "deferral_downtime_delta_14d_h"),
        ("docs/memo_ejecutivo_es.md", "Prioridades", "top_unit_by_priority"),
        ("docs/memo_ejecutivo_es.md", "Prioridades", "top_component_by_priority"),
        ("outputs/dashboard/index.html", "KPI Cards", "fleet_availability_pct"),
        ("outputs/dashboard/index.html", "KPI Cards", "mtbf_proxy_hours"),
        ("outputs/dashboard/index.html", "KPI Cards", "mttr_proxy_hours"),
        ("outputs/dashboard/index.html", "KPI Cards", "high_risk_units_count"),
        ("outputs/dashboard/index.html", "KPI Cards", "backlog_physical_items_count"),
        ("outputs/dashboard/index.html", "KPI Cards", "backlog_overdue_items_count"),
        ("outputs/dashboard/index.html", "KPI Cards", "backlog_critical_physical_count"),
        ("outputs/dashboard/index.html", "KPI Cards", "high_deferral_risk_cases_count"),
        ("outputs/dashboard/index.html", "KPI Cards", "cbm_operational_savings_eur"),
        ("outputs/dashboard/index.html", "KPI Cards", "cbm_value_range_min_eur"),
        ("outputs/dashboard/index.html", "KPI Cards", "cbm_value_range_max_eur"),
        ("outputs/dashboard/index.html", "KPI Cards", "cbm_prob_positive_savings"),
        ("outputs/dashboard/index.html", "KPI Cards", "mean_depot_saturation_pct"),
        ("outputs/dashboard/index.html", "Decisión Ejecutiva", "top_unit_by_priority"),
        ("outputs/dashboard/index.html", "Decisión Ejecutiva", "top_component_by_priority"),
        ("outputs/dashboard/index.html", "Decisión Ejecutiva", "top_deferral_risk_score"),
    ]
    return pd.DataFrame(rows, columns=["artifact", "block", "metric_id"])


def _build_hardcoded_audit() -> pd.DataFrame:
    rows = [
        {
            "artifact": "README.md",
            "previous_status": "hardcoded_metrics_and_decision",
            "current_status": "rendered_from_ssot_metrics",
            "risk_level": "mitigated",
        },
        {
            "artifact": "docs/memo_ejecutivo_es.md",
            "previous_status": "partial_dynamic_non_ssot",
            "current_status": "rendered_from_ssot_metrics",
            "risk_level": "mitigated",
        },
        {
            "artifact": "outputs/reports/informe_analitico_avanzado.md",
            "previous_status": "dynamic_but_independent_calcs",
            "current_status": "summary_block_synced_from_ssot",
            "risk_level": "controlled",
        },
        {
            "artifact": "outputs/dashboard/index.html",
            "previous_status": "dynamic_with_local_formulas",
            "current_status": "kpis_and_decision_from_ssot_metrics",
            "risk_level": "mitigated",
        },
    ]
    return pd.DataFrame(rows)


def _build_backlog_kpi_before_after(metrics: dict[str, Any]) -> pd.DataFrame:
    before_def = {
        "backlog_critico_reportado": {
            "definition_before": "conteo de componentes con deferral_risk_score >= 70 (mezclado con diferimiento)",
            "value_before": int(float(metrics["high_deferral_risk_cases_count"])),
            "source_before": "data/processed/workshop_priority_table.csv",
        }
    }
    rows = [
        {
            "kpi_name": "backlog_fisico",
            "definition_before": "n/a",
            "value_before": None,
            "source_before": None,
            "definition_after": "pendientes reales abiertos de mantenimiento",
            "value_after": int(float(metrics["backlog_physical_items_count"])),
            "source_after": "data/raw/backlog_mantenimiento.csv",
            "decision_supported": "dimensionar carga real de taller",
        },
        {
            "kpi_name": "backlog_vencido",
            "definition_before": "n/a",
            "value_before": None,
            "source_before": None,
            "definition_after": "pendientes físicos con antigüedad >= 14 días",
            "value_after": int(float(metrics["backlog_overdue_items_count"])),
            "source_after": "data/raw/backlog_mantenimiento.csv",
            "decision_supported": "acelerar cola vencida y proteger SLA",
        },
        {
            "kpi_name": "backlog_critico_fisico",
            "definition_before": before_def["backlog_critico_reportado"]["definition_before"],
            "value_before": before_def["backlog_critico_reportado"]["value_before"],
            "source_before": before_def["backlog_critico_reportado"]["source_before"],
            "definition_after": "pendientes físicos críticos por edad/severidad o riesgo acumulado alto",
            "value_after": int(float(metrics["backlog_critical_physical_count"])),
            "source_after": "data/raw/backlog_mantenimiento.csv",
            "decision_supported": "qué pendientes físicos deben intervenirse antes",
        },
        {
            "kpi_name": "riesgo_diferimiento_alto",
            "definition_before": "implícitamente mezclado con backlog crítico",
            "value_before": int(float(metrics["high_deferral_risk_cases_count"])),
            "source_before": "data/processed/workshop_priority_table.csv",
            "definition_after": "casos con deferral_risk_score >= 70 (riesgo de aplazar, no backlog físico)",
            "value_after": int(float(metrics["high_deferral_risk_cases_count"])),
            "source_after": "data/processed/workshop_priority_table.csv",
            "decision_supported": "qué no debe diferirse por impacto operacional",
        },
        {
            "kpi_name": "exposure_backlog_adjusted",
            "definition_before": "n/a",
            "value_before": None,
            "source_before": None,
            "definition_after": "score compuesto 0-100 de exposición de backlog físico",
            "value_after": round(float(metrics["backlog_exposure_adjusted_mean"]), 2),
            "source_after": "data/processed/vw_depot_maintenance_pressure.csv",
            "decision_supported": "priorizar depósitos por exposición estructural de backlog",
        },
    ]
    return pd.DataFrame(rows)


def _build_backlog_metric_taxonomy(metrics: dict[str, Any]) -> pd.DataFrame:
    rows = [
        {
            "metric_id": "backlog_physical_items_count",
            "category": "backlog_fisico",
            "definition": "pendientes reales abiertos",
            "unit": "count",
            "value": int(float(metrics["backlog_physical_items_count"])),
            "decision_use": "dimensionamiento de cola real de taller",
        },
        {
            "metric_id": "backlog_overdue_items_count",
            "category": "backlog_vencido",
            "definition": "pendientes con antigüedad >=14 días",
            "unit": "count",
            "value": int(float(metrics["backlog_overdue_items_count"])),
            "decision_use": "escalado táctico para recuperar cumplimiento",
        },
        {
            "metric_id": "backlog_critical_physical_count",
            "category": "backlog_critico_por_edad_severidad",
            "definition": "pendientes críticos por edad/severidad o riesgo acumulado",
            "unit": "count",
            "value": int(float(metrics["backlog_critical_physical_count"])),
            "decision_use": "secuenciación de intervención física prioritaria",
        },
        {
            "metric_id": "high_deferral_risk_cases_count",
            "category": "riesgo_diferimiento",
            "definition": "casos con score de diferimiento >=70",
            "unit": "count",
            "value": int(float(metrics["high_deferral_risk_cases_count"])),
            "decision_use": "límite de aplazamiento y ventana de entrada",
        },
        {
            "metric_id": "backlog_exposure_adjusted_mean",
            "category": "exposure_backlog_adjusted",
            "definition": "exposición compuesta 0-100 (cantidad+edad+criticidad backlog físico)",
            "unit": "score_0_100",
            "value": round(float(metrics["backlog_exposure_adjusted_mean"]), 2),
            "decision_use": "priorización de depósitos y rebalanceo de capacidad",
        },
    ]
    return pd.DataFrame(rows)


def write_backlog_metric_governance_doc(before_after_df: pd.DataFrame, taxonomy_df: pd.DataFrame) -> Path:
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Backlog Metric Governance",
        "",
        "## Objetivo",
        "Separar formalmente backlog físico y riesgo de diferimiento para evitar KPIs híbridos mal rotulados.",
        "",
        "## Taxonomía oficial",
        "1. backlog físico: pendientes reales abiertos.",
        "2. backlog vencido: backlog físico fuera de ventana operativa (>=14 días).",
        "3. backlog crítico por edad/severidad: vencido severo o riesgo acumulado alto.",
        "4. riesgo de diferimiento: probabilidad de daño al aplazar una intervención (score de decisión).",
        "5. exposure backlog-adjusted: exposición compuesta 0-100 del backlog físico.",
        "",
        "## Regla de gobierno obligatoria",
        "- Nunca usar `deferral_risk_score` para reportar backlog físico.",
        "- Nunca usar backlog físico para inferir automáticamente riesgo de diferimiento sin score explícito.",
        "",
        "## Tabla Before/After",
        before_after_df.to_markdown(index=False),
        "",
        "## KPI oficial por decisión",
        taxonomy_df.to_markdown(index=False),
        "",
        "## Uso ejecutivo",
        "- Dirección de taller: backlog físico/vencido/crítico por depósito.",
        "- Dirección de operaciones: riesgo de diferimiento y exposición backlog-adjusted.",
        "- Dirección de mantenimiento: combinación de ambos para secuencia de intervención.",
    ]
    out = DOCS_DIR / "backlog_metric_governance.md"
    out.write_text("\n".join(lines), encoding="utf-8")
    return out


def write_reporting_governance_doc(metrics_df: pd.DataFrame, mapping_df: pd.DataFrame) -> Path:
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Reporting Governance",
        "",
        "## Objetivo",
        "Evitar desincronización entre narrativa y resultados: todas las cifras ejecutivas se generan desde métricas oficiales (SSOT).",
        "",
        "## Artefactos narrativos bajo gobierno",
        "- README.md",
        "- docs/memo_ejecutivo_es.md",
        "- outputs/reports/memo_ejecutivo_es.md",
        "- outputs/dashboard/index.html (KPIs + bloque de decisión)",
        "",
        "## Single Source of Truth",
        "Archivo oficial: `data/processed/narrative_metrics_official.csv`",
        "",
        "## Métricas Narrativas Oficializadas",
        metrics_df[
            [
                "metric_id",
                "label",
                "unit",
                "source_of_truth",
                "window_definition",
                "filter_definition",
                "aggregation_definition",
            ]
        ].to_markdown(index=False),
        "",
        "## Mapeo métrica -> artefacto narrativo",
        mapping_df.to_markdown(index=False),
        "",
        "## Reglas de consistencia",
        "- Misma definición de métrica para README, memo, dashboard y summaries.",
        "- Misma ventana temporal para narrativa ejecutiva (`histórico completo` o `última fecha`, según métrica).",
        "- Misma unidad de medida y formato de presentación.",
        "- Si el output de datos cambia, la narrativa se regenera automáticamente en el pipeline.",
        "",
        "## Publicación",
        "- Bloquear publicación cuando los tests de consistencia interartefactos fallen.",
        "- No editar manualmente cifras en README/memo/dashboard.",
    ]
    out = DOCS_DIR / "reporting_governance.md"
    out.write_text("\n".join(lines), encoding="utf-8")
    return out


def sync_narrative_artifacts(force_recompute: bool = True) -> dict[str, Path]:
    metrics = load_or_compute_narrative_metrics(force_recompute=force_recompute)
    metrics_df = pd.read_csv(DATA_PROCESSED_DIR / "narrative_metrics_official.csv")
    mapping_df = _build_artifact_mapping()
    hardcoded_df = _build_hardcoded_audit()
    backlog_before_after_df = _build_backlog_kpi_before_after(metrics)
    backlog_taxonomy_df = _build_backlog_metric_taxonomy(metrics)

    OUTPUTS_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    metrics_df.to_csv(OUTPUTS_REPORTS_DIR / "narrative_metrics_official.csv", index=False)
    mapping_df.to_csv(OUTPUTS_REPORTS_DIR / "narrative_artifact_mapping.csv", index=False)
    hardcoded_df.to_csv(OUTPUTS_REPORTS_DIR / "narrative_hardcoded_audit.csv", index=False)
    backlog_before_after_df.to_csv(OUTPUTS_REPORTS_DIR / "backlog_kpi_before_after.csv", index=False)
    backlog_taxonomy_df.to_csv(OUTPUTS_REPORTS_DIR / "backlog_metric_taxonomy.csv", index=False)

    kpis_df = _build_kpis_ejecutivos(metrics)
    kpis_df.to_csv(OUTPUTS_REPORTS_DIR / "kpis_ejecutivos.csv", index=False)

    memo = _build_memo(metrics)
    (DOCS_DIR / "memo_ejecutivo_es.md").write_text(memo, encoding="utf-8")
    (OUTPUTS_REPORTS_DIR / "memo_ejecutivo_es.md").write_text(memo, encoding="utf-8")

    readme = _build_readme(metrics)
    (ROOT_DIR / "README.md").write_text(readme, encoding="utf-8")

    backlog_governance_doc = write_backlog_metric_governance_doc(
        before_after_df=backlog_before_after_df,
        taxonomy_df=backlog_taxonomy_df,
    )
    return {
        "metrics": DATA_PROCESSED_DIR / "narrative_metrics_official.csv",
        "kpis": OUTPUTS_REPORTS_DIR / "kpis_ejecutivos.csv",
        "memo": DOCS_DIR / "memo_ejecutivo_es.md",
        "readme": ROOT_DIR / "README.md",
        "backlog_governance_doc": backlog_governance_doc,
        "mapping": OUTPUTS_REPORTS_DIR / "narrative_artifact_mapping.csv",
        "hardcoded_audit": OUTPUTS_REPORTS_DIR / "narrative_hardcoded_audit.csv",
        "backlog_before_after": OUTPUTS_REPORTS_DIR / "backlog_kpi_before_after.csv",
        "backlog_taxonomy": OUTPUTS_REPORTS_DIR / "backlog_metric_taxonomy.csv",
    }


if __name__ == "__main__":
    sync_narrative_artifacts(force_recompute=True)
