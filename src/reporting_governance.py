from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.config import DATA_PROCESSED_DIR, DATA_RAW_DIR, DOCS_DIR, ROOT_DIR


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
        definition="Disponibilidad promedio de flota para texto ejecutivo.",
    ),
    MetricSpec(
        metric_id="mtbf_proxy_hours",
        label="MTBF aproximado",
        unit="horas",
        source_of_truth="data/processed/fleet_week_features.csv",
        window_definition="histórico completo semanal disponible",
        filter_definition="todos los registros válidos",
        aggregation_definition="mean(mtbf_proxy)",
        definition="Tiempo medio entre fallas aproximado consolidado.",
    ),
    MetricSpec(
        metric_id="mttr_proxy_hours",
        label="MTTR aproximado",
        unit="horas",
        source_of_truth="data/processed/fleet_week_features.csv",
        window_definition="histórico completo semanal disponible",
        filter_definition="todos los registros válidos",
        aggregation_definition="mean(mttr_proxy)",
        definition="Tiempo medio de reparación aproximado consolidado.",
    ),
    MetricSpec(
        metric_id="high_risk_units_count",
        label="Unidades de alto riesgo",
        unit="conteo",
        source_of_truth="data/processed/unit_unavailability_risk_score.csv",
        window_definition="corte actual por unidad",
        filter_definition="unit_unavailability_risk_score >= media_flota + 1.5·desv_est",
        aggregation_definition="count(*)",
        definition="Unidades en la cola de alto riesgo (valor atípico estadístico >1.5σ sobre la media de flota).",
    ),
    MetricSpec(
        metric_id="n_flotas",
        label="Flotas",
        unit="conteo",
        source_of_truth="data/raw/flotas.csv",
        window_definition="catálogo completo",
        filter_definition="identificadores válidos",
        aggregation_definition="count(distinct flota_id)",
        definition="Número de flotas sintéticas analizadas.",
    ),
    MetricSpec(
        metric_id="n_unidades",
        label="Unidades",
        unit="conteo",
        source_of_truth="data/raw/unidades.csv",
        window_definition="catálogo completo",
        filter_definition="identificadores válidos",
        aggregation_definition="count(distinct unidad_id)",
        definition="Número de unidades sintéticas analizadas.",
    ),
    MetricSpec(
        metric_id="n_depositos",
        label="Depósitos",
        unit="conteo",
        source_of_truth="data/raw/depositos.csv",
        window_definition="catálogo completo",
        filter_definition="identificadores válidos",
        aggregation_definition="count(distinct deposito_id)",
        definition="Número de depósitos sintéticos analizados.",
    ),
    MetricSpec(
        metric_id="n_componentes",
        label="Componentes críticos",
        unit="conteo",
        source_of_truth="data/raw/componentes_criticos.csv",
        window_definition="catálogo completo",
        filter_definition="identificadores válidos",
        aggregation_definition="count(distinct componente_id)",
        definition="Número de componentes críticos sintéticos analizados.",
    ),
    MetricSpec(
        metric_id="backlog_physical_items_count",
        label="Pendientes físicos",
        unit="conteo",
        source_of_truth="data/raw/backlog_mantenimiento.csv",
        window_definition="última fecha disponible de pendientes",
        filter_definition="todos los pendientes abiertos del día",
        aggregation_definition="count(*)",
        definition="Pendientes reales de mantenimiento aún no resueltos.",
    ),
    MetricSpec(
        metric_id="backlog_overdue_items_count",
        label="Pendientes vencidos",
        unit="conteo",
        source_of_truth="data/raw/backlog_mantenimiento.csv",
        window_definition="última fecha disponible de pendientes",
        filter_definition="antiguedad_backlog_dias >= 14",
        aggregation_definition="count(*)",
        definition="Pendientes físicos fuera de ventana operativa objetivo.",
    ),
    MetricSpec(
        metric_id="backlog_critical_physical_count",
        label="Pendientes críticos físicos",
        unit="conteo",
        source_of_truth="data/raw/backlog_mantenimiento.csv",
        window_definition="última fecha disponible de pendientes",
        filter_definition="antiguedad>=21 con severidad alta/crítica o riesgo_acumulado>=70",
        aggregation_definition="count(*)",
        definition="Pendientes físicos con criticidad estructural por edad/severidad.",
    ),
    MetricSpec(
        metric_id="high_deferral_risk_cases_count",
        label="Casos de alto riesgo de diferimiento",
        unit="conteo",
        source_of_truth="data/processed/workshop_priority_table.csv",
        window_definition="corte actual de priorización",
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
        label="Diferencial operativo CBM vs reactiva",
        unit="eur",
        source_of_truth="data/processed/comparativo_estrategias.csv",
        window_definition="escenario comparativo de estrategia",
        filter_definition="estrategia in (reactiva, basada_en_condicion)",
        aggregation_definition="coste_operativo_proxy(reactiva) - coste_operativo_proxy(CBM)",
        definition="Diferencial operativo aproximado firmado: positivo indica ahorro y negativo indica coste incremental.",
    ),
    MetricSpec(
        metric_id="cbm_value_range_min_eur",
        label="CBM vs reactiva: diferencial neto estimado P10",
        unit="eur",
        source_of_truth="data/processed/comparativo_estrategias.csv",
        window_definition="escenario comparativo de estrategia con sensibilidad",
        filter_definition="estrategia = basada_en_condicion",
        aggregation_definition="rango_plausible_valor_min(CBM)",
        definition="Límite inferior estimado del diferencial neto CBM frente a reactiva bajo sensibilidad.",
    ),
    MetricSpec(
        metric_id="cbm_value_range_max_eur",
        label="CBM vs reactiva: diferencial neto estimado P90",
        unit="eur",
        source_of_truth="data/processed/comparativo_estrategias.csv",
        window_definition="escenario comparativo de estrategia con sensibilidad",
        filter_definition="estrategia = basada_en_condicion",
        aggregation_definition="rango_plausible_valor_max(CBM)",
        definition="Límite superior estimado del diferencial neto CBM frente a reactiva bajo sensibilidad.",
    ),
    MetricSpec(
        metric_id="cbm_prob_positive_savings",
        label="CBM vs reactiva: probabilidad de ahorro positivo",
        unit="ratio_0_1",
        source_of_truth="data/processed/comparativo_estrategias.csv",
        window_definition="escenario comparativo de estrategia con sensibilidad",
        filter_definition="estrategia = basada_en_condicion",
        aggregation_definition="prob_ahorro_positivo(CBM)",
        definition="Robustez del caso CBM: proporción de simulaciones con ahorro positivo frente a reactiva.",
    ),
    MetricSpec(
        metric_id="cbm_breakeven_value_per_service_hour_eur",
        label="CBM: valor sombra de equilibrio por hora de servicio",
        unit="eur_por_hora",
        source_of_truth="data/processed/comparativo_estrategias.csv",
        window_definition="escenario comparativo de estrategia (caso base)",
        filter_definition="estrategia in (reactiva, basada_en_condicion)",
        aggregation_definition="abs(cbm_operational_savings_eur) / horas_servicio_preservadas_vs_reactiva(CBM)",
        definition=(
            "Valor mínimo que la organización debería asignar a cada hora de servicio preservada para que el "
            "coste incremental aproximado de CBM quede compensado por el valor de disponibilidad adicional. Es un "
            "umbral de decisión bajo el escenario base, no una estimación de disposición a pagar observada ni "
            "un precio de mercado; no debe usarse para justificar el caso de inversión sin validar el propio "
            "umbral con costes corporativos reales."
        ),
    ),
    MetricSpec(
        metric_id="avoidable_downtime_hours_inspection",
        label="Horas de indisponibilidad evitables por inspección automática",
        unit="horas",
        source_of_truth="data/processed/inspection_module_value_comparison.csv",
        window_definition="escenario comparativo inspección automática",
        filter_definition="scenario in (sin_inspeccion_automatica, con_inspeccion_automatica)",
        aggregation_definition="horas_indisponibilidad(sin) - horas_indisponibilidad(con)",
        definition="Horas potencialmente evitadas por módulo de inspección automática.",
    ),
    MetricSpec(
        metric_id="avoidable_correctives_inspection",
        label="Correctivas evitables por inspección automática",
        unit="conteo_aproximado",
        source_of_truth="data/processed/inspection_module_value_comparison.csv",
        window_definition="escenario comparativo inspección automática",
        filter_definition="scenario in (sin_inspeccion_automatica, con_inspeccion_automatica)",
        aggregation_definition="correctivas(sin) - correctivas(con)",
        definition="Correctivas aproximadas que se evitarían con inspección automática.",
    ),
    MetricSpec(
        metric_id="mean_depot_saturation_pct",
        label="Saturación media de depósitos",
        unit="pct",
        source_of_truth="data/processed/vw_depot_maintenance_pressure.csv",
        window_definition="última fecha disponible",
        filter_definition="fecha = max(fecha)",
        aggregation_definition="mean(saturation_ratio) * 100",
        definition="Saturación media de taller en el corte más reciente.",
    ),
    MetricSpec(
        metric_id="backlog_exposure_adjusted_mean",
        label="Exposición ajustada media de pendientes",
        unit="puntuacion_0_100",
        source_of_truth="data/processed/vw_depot_maintenance_pressure.csv",
        window_definition="última fecha con pendientes físicos disponibles",
        filter_definition="fecha = max(fecha) alineada con corte de pendientes",
        aggregation_definition="mean(backlog_exposure_adjusted_score)",
        definition="Exposición compuesta de pendientes físicos (cantidad+edad+criticidad).",
    ),
    MetricSpec(
        metric_id="top_unit_by_priority",
        label="Unidad prioritaria",
        unit="id",
        source_of_truth="data/processed/workshop_priority_table.csv",
        window_definition="corte actual de priorización",
        filter_definition="top 1 ordenado por intervention_priority_score desc, deferral_risk_score desc",
        aggregation_definition="first(unidad_id)",
        definition="Unidad con mayor prioridad de entrada a taller.",
    ),
    MetricSpec(
        metric_id="top_component_by_priority",
        label="Componente prioritario",
        unit="id",
        source_of_truth="data/processed/workshop_priority_table.csv",
        window_definition="corte actual de priorización",
        filter_definition="misma fila top_unit_by_priority",
        aggregation_definition="first(componente_id)",
        definition="Componente que debe intervenirse primero según puntuación consolidada.",
    ),
    MetricSpec(
        metric_id="top_component_family_by_priority",
        label="Familia de componente prioritario",
        unit="etiqueta",
        source_of_truth="data/processed/scoring_componentes.csv + data/processed/workshop_priority_table.csv",
        window_definition="corte actual",
        filter_definition="misma fila top_unit_by_priority",
        aggregation_definition="lookup component_family",
        definition="Familia del componente prioritario para texto ejecutivo.",
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
        definition="Incremento de coste aproximado por diferir 14 días.",
    ),
    MetricSpec(
        metric_id="deferral_downtime_delta_14d_h",
        label="Impacto de diferimiento a 14 días (indisponibilidad)",
        unit="horas",
        source_of_truth="data/processed/impacto_diferimiento_resumen.csv",
        window_definition="escenarios de diferimiento",
        filter_definition="comparación defer_dias = 14 vs defer_dias = 0",
        aggregation_definition="downtime_total_h(14) - downtime_total_h(0)",
        definition="Incremento de horas de indisponibilidad por diferir 14 días.",
    ),
    MetricSpec(
        metric_id="top_priority_score",
        label="Puntuación de prioridad de intervención (principal)",
        unit="puntuacion_0_100",
        source_of_truth="data/processed/workshop_priority_table.csv",
        window_definition="corte actual de priorización",
        filter_definition="fila top por prioridad",
        aggregation_definition="first(intervention_priority_score)",
        definition="Puntuación de la intervención prioritaria.",
    ),
    MetricSpec(
        metric_id="top_deferral_risk_score",
        label="Puntuación de riesgo por diferimiento (principal)",
        unit="puntuacion_0_100",
        source_of_truth="data/processed/workshop_priority_table.csv",
        window_definition="corte actual de priorización",
        filter_definition="fila top por prioridad",
        aggregation_definition="first(deferral_risk_score)",
        definition="Riesgo de diferir asociado al caso prioritario.",
    ),
]


def _format_float(value: float, digits: int = 2) -> str:
    return f"{value:.{digits}f}".replace(".", ",")


def _format_int(value: float) -> str:
    return f"{int(round(value)):,}".replace(",", ".")


def _format_signed_eur(value: float) -> str:
    sign = "-" if value < 0 else ""
    return f"{sign}€ {_format_int(abs(value))}"


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

    # Cohorte de alto riesgo por control estadístico de proceso: unidades cuyo
    # riesgo de indisponibilidad supera la media de flota en más de 1.5 desviaciones
    # estándar. Es un umbral relativo y auto-calibrado (no un corte arbitrario sobre
    # una escala comprimida) que aísla la cola operativamente ejecutable.
    unit_scores = unit_risk["unit_unavailability_risk_score"].astype(float)
    high_risk_threshold = float(unit_scores.mean() + 1.5 * unit_scores.std())
    high_risk_units_count = int((unit_scores >= high_risk_threshold).sum())

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
    coverage_start = fleet_week["week_start"].min().date().isoformat()
    coverage_end = fleet_week["week_start"].max().date().isoformat()

    values = {
        "coverage_start": coverage_start,
        "coverage_end": coverage_end,
        "fleet_availability_pct": float(fleet_week["availability_rate"].mean() * 100),
        "mtbf_proxy_hours": float(fleet_week["mtbf_proxy"].mean()),
        "mttr_proxy_hours": float(fleet_week["mttr_proxy"].mean()),
        "high_risk_units_count": high_risk_units_count,
        "backlog_physical_items_count": backlog_physical_items_count,
        "backlog_overdue_items_count": backlog_overdue_items_count,
        "backlog_critical_physical_count": backlog_critical_physical_count,
        "high_deferral_risk_cases_count": high_deferral_risk_cases_count,
        "cbm_vs_reactiva_availability_pp": float(cbm["fleet_availability"] - react["fleet_availability"]),
        "cbm_operational_savings_eur": float(react["coste_operativo_proxy"] - cbm["coste_operativo_proxy"]),
        "cbm_value_range_min_eur": float(cbm.get("rango_plausible_valor_min", np.nan)),
        "cbm_value_range_max_eur": float(cbm.get("rango_plausible_valor_max", np.nan)),
        "cbm_prob_positive_savings": float(cbm.get("prob_ahorro_positivo", np.nan)),
        "cbm_breakeven_value_per_service_hour_eur": float(
            abs(react["coste_operativo_proxy"] - cbm["coste_operativo_proxy"])
            / cbm["horas_servicio_preservadas_vs_reactiva"]
        ),
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
        if metric_id in {"coverage_start", "coverage_end"}:
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
    df.to_csv(metrics_path, index=False)
    return values


def _build_memo(metrics: dict[str, Any]) -> str:
    cbm_delta = float(metrics["cbm_operational_savings_eur"])
    economic_result = (
        f"Ahorro operativo aproximado estimado CBM vs reactiva: {_format_signed_eur(cbm_delta)}."
        if cbm_delta >= 0
        else f"Coste incremental aproximado estimado CBM vs reactiva: {_format_signed_eur(abs(cbm_delta))}."
    )
    return "\n".join(
        [
            "# Memorando Ejecutivo",
            "",
            "## 1. Contexto",
            "La red ferroviaria analizada opera con alta exigencia de disponibilidad y presión de taller en depósitos críticos.",
            "",
            "## 2. Problema",
            "Persisten fallas repetitivas y pendientes técnicos que elevan el riesgo de indisponibilidad y afectan servicio.",
            "",
            "## 3. Enfoque metodológico",
            "El resumen toma sus cifras del registro oficial de métricas,",
            "para que README, panel de control e informes comuniquen los mismos valores.",
            "",
            "## 4. Hallazgos principales",
            f"- Disponibilidad media de flota: {_format_float(float(metrics['fleet_availability_pct']), 2)}%.",
            f"- Unidades de alto riesgo: {_format_int(float(metrics['high_risk_units_count']))}.",
            f"- Pendientes físicos: {_format_int(float(metrics['backlog_physical_items_count']))}.",
            f"- Pendientes vencidos: {_format_int(float(metrics['backlog_overdue_items_count']))}.",
            f"- Pendientes críticos físicos: {_format_int(float(metrics['backlog_critical_physical_count']))}.",
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
            economic_result,
            (
                "Rango estimado del diferencial CBM vs reactiva: "
                f"{_format_signed_eur(float(metrics['cbm_value_range_min_eur']))} a "
                f"{_format_signed_eur(float(metrics['cbm_value_range_max_eur']))}."
            ),
            (
                "Robustez del ahorro CBM en escenarios y sensibilidades: "
                f"{_format_float(float(metrics['cbm_prob_positive_savings']) * 100, 1)}% de casos con ahorro positivo."
            ),
            "",
            "## 8. Compensaciones principales",
            (
                "Diferir 14 días incrementa coste en "
                f"{_format_int(float(metrics['deferral_cost_delta_14d_eur']))} EUR y la indisponibilidad en "
                f"{_format_float(float(metrics['deferral_downtime_delta_14d_h']), 1)} h."
            ),
            (
                "Separación conceptual aplicada: pendientes físicos (cantidad/edad/severidad) "
                "y riesgo de diferimiento (puntuación de decisión) se reportan por separado."
            ),
            "",
            "## 9. Prioridades de intervención",
            f"- Unidad prioritaria: {metrics['top_unit_by_priority']}.",
            f"- Componente prioritario: {metrics['top_component_by_priority']}.",
            f"- Familia técnica asociada: {metrics['top_component_family_by_priority']}.",
            "",
            "## 10. Limitaciones",
            "Datos sintéticos y costes aproximados; los resultados no sustituyen calibración con datos reales de operación.",
            "",
            "## 11. Acciones recomendadas",
            "Validar umbrales con histórico real, incorporar optimización matemática de la planificación y retroalimentar el modelo con órdenes ejecutadas.",
        ]
    )


def _build_readme(metrics: dict[str, Any]) -> str:
    cbm_delta = float(metrics["cbm_operational_savings_eur"])
    cbm_delta_label = (
        "Ahorro operativo aproximado CBM vs reactiva"
        if cbm_delta >= 0
        else "Coste incremental aproximado CBM vs reactiva"
    )
    return "\n".join(
        [
            "# Inteligencia de Mantenimiento Ferroviario - CBM",
            "",
            "Sistema de decisión para flotas ferroviarias: prioriza intervenciones de taller, cuantifica el riesgo de diferir cada decisión y mide el valor del mantenimiento basado en condición frente a una estrategia reactiva.",
            "",
            "**[Panel de control en vivo](https://mfidalgomartins.github.io/inteligencia-mantenimiento-ferroviario-cbm/)** · Python · SQL · DuckDB · HTML sin conexión",
            "",
            f"## Resultados - flota sintética de {_format_int(float(metrics['n_unidades']))} unidades",
            "",
            "| Métrica | Valor |",
            "|---------|------:|",
            f"| Disponibilidad media de flota | **{_format_float(float(metrics['fleet_availability_pct']), 2)} %** |",
            f"| Unidades de alto riesgo (≥ media + 1,5σ) | **{_format_int(float(metrics['high_risk_units_count']))}** |",
            f"| Pendientes físicos | **{_format_int(float(metrics['backlog_physical_items_count']))} pendientes** |",
            f"| Pendientes vencidos | **{_format_int(float(metrics['backlog_overdue_items_count']))} pendientes** |",
            f"| Pendientes críticos físicos | **{_format_int(float(metrics['backlog_critical_physical_count']))} pendientes** |",
            f"| Casos de alto riesgo de diferimiento | **{_format_int(float(metrics['high_deferral_risk_cases_count']))}** |",
            f"| {cbm_delta_label} | **{_format_signed_eur(abs(cbm_delta) if cbm_delta < 0 else cbm_delta)}** |",
            f"| Mejora de disponibilidad CBM vs reactiva | **+{_format_float(float(metrics['cbm_vs_reactiva_availability_pp']), 2)} p.p.** |",
            "",
            f"**Decisión actual:** intervenir primero la unidad `{metrics['top_unit_by_priority']}`, componente `{metrics['top_component_by_priority']}`.",
            "",
            "## Qué resuelve",
            "",
            f"- Integra sensores, inspección automática, fallos y mantenimiento para puntuar {_format_int(float(metrics['n_componentes']))} componentes.",
            "- Ordena y secuencia la cola de taller según riesgo técnico, impacto de servicio, capacidad y ventana operativa.",
            "- Compara CBM, preventiva rígida y reactiva con supuestos económicos explícitos y análisis de sensibilidad.",
            "- Mantiene trazabilidad desde los datos hasta las métricas ejecutivas y bloquea el flujo ante validaciones críticas.",
            "",
            "## Análisis",
            "",
            "<table>",
            "<tr>",
            '<td width="50%">',
            "",
            "![Valor estratégico CBM vs reactiva](outputs/graphs/02_valor_estrategias.png)",
            "",
            "</td>",
            '<td width="50%">',
            "",
            "![Distribución de riesgo de flota](outputs/graphs/06_distribucion_riesgo_unidades.png)",
            "",
            "</td>",
            "</tr>",
            "<tr>",
            "<td>",
            "",
            "![Cola de taller por riesgo](outputs/graphs/04_ranking_intervenciones.png)",
            "",
            "</td>",
            "<td>",
            "",
            "![Saturación de depósitos](outputs/graphs/05_saturacion_depositos.png)",
            "",
            "</td>",
            "</tr>",
            "</table>",
            "",
            "## Panel de control",
            "",
            "HTML autocontenido sin dependencias externas. Funciona sin conexión e incluye filtros por flota, depósito, familia, sistema, riesgo e intervención.",
            "",
            "**[Abrir panel de control en vivo](https://mfidalgomartins.github.io/inteligencia-mantenimiento-ferroviario-cbm/)**",
            "",
            "**[Descargar informe analítico (PDF)](outputs/reports/informe_analitico_cbm_ferroviario.pdf)**",
            "",
            "## Arquitectura",
            "",
            "```",
            "datos sintéticos → preparación SQL → tablas analíticas → puntuación → priorización → panel de control",
            "```",
            "",
            "1. Datos sintéticos deterministas de operación, sensores, fallos, inspección y mantenimiento.",
            "2. Capa SQL DuckDB por etapas: preparación, integración, tablas analíticas e indicadores.",
            "3. Ingeniería de variables para salud de componente, RUL operativo y puntuación de prioridad.",
            "4. Priorización y planificación heurística con capacidad de taller.",
            "5. Comparativa estratégica y análisis de diferimiento.",
            "6. Panel de control sin conexión alimentado por el registro oficial de métricas.",
            "",
            "## Reproducir",
            "Requiere Python 3.12 o superior.",
            "",
            "```bash",
            "python3 -m venv .venv",
            "source .venv/bin/activate",
            "python -m pip install -r requirements-lock.txt",
            "./scripts/run_pipeline.sh",
            "./scripts/run_tests.sh",
            "./scripts/run_coverage.sh",
            "```",
            "El flujo usa semilla fija y regenera datos, métricas, documentación derivada y panel de control.",
            "",
            "## Estructura",
            "",
            "```",
            "src/          lógica de datos, puntuación y generador del panel de control",
            "sql/          capa SQL por etapas (preparación → integración → tablas analíticas → indicadores)",
            "notebooks/    análisis exploratorio por fase del flujo",
            "scripts/      ejecución del flujo, pruebas y publicación",
            "outputs/      panel de control, gráficos PNG e informe PDF",
            "tests/        validaciones de calidad, métricas y consistencia",
            "docs/         reproducibilidad, supuestos y contratos de métricas",
            "```",
            "",
            "Documentación técnica: [reproducibilidad](docs/reproducibility.md) · [arquitectura del repositorio](docs/repo_architecture.md) · [preparación productiva](docs/production_readiness.md) · [seguridad y dependencias](docs/security_dependency_hygiene.md) · [marco RUL](docs/rul_framework.md) · [gobierno de métricas](docs/gobierno_metricas.md)",
            "",
            "## Limitaciones",
            "- Todos los datos son sintéticos; los umbrales requieren calibración antes de uso operacional.",
            "- Los costes y ahorros son aproximaciones de escenario, no estimaciones financieras contractuales.",
            "- El RUL sirve como ventana relativa de intervención; su asociación con fallo a 30 días es débil y no representa una fecha de fallo calibrada.",
            "- La planificación es heurística y no garantiza una solución global óptima.",
            "",
            "## Tecnologías",
            "Python · SQL · DuckDB · pandas · matplotlib · pytest · pytest-cov · HTML/CSS/JavaScript",
            "",
            "## Licencia",
            "MIT.",
        ]
    )


def _build_backlog_metric_taxonomy(metrics: dict[str, Any]) -> pd.DataFrame:
    rows = [
        {
            "metric_id": "backlog_physical_items_count",
            "category": "pendientes_fisicos",
            "definition": "pendientes reales abiertos",
            "unit": "conteo",
            "value": int(float(metrics["backlog_physical_items_count"])),
            "decision_use": "dimensionamiento de cola real de taller",
        },
        {
            "metric_id": "backlog_overdue_items_count",
            "category": "pendientes_vencidos",
            "definition": "pendientes con antigüedad >=14 días",
            "unit": "conteo",
            "value": int(float(metrics["backlog_overdue_items_count"])),
            "decision_use": "escalado táctico para recuperar cumplimiento",
        },
        {
            "metric_id": "backlog_critical_physical_count",
            "category": "pendientes_criticos_por_edad_severidad",
            "definition": "pendientes críticos por edad/severidad o riesgo acumulado",
            "unit": "conteo",
            "value": int(float(metrics["backlog_critical_physical_count"])),
            "decision_use": "secuenciación de intervención física prioritaria",
        },
        {
            "metric_id": "high_deferral_risk_cases_count",
            "category": "riesgo_diferimiento",
            "definition": "casos con puntuación de diferimiento >=70",
            "unit": "conteo",
            "value": int(float(metrics["high_deferral_risk_cases_count"])),
            "decision_use": "límite de aplazamiento y ventana de entrada",
        },
        {
            "metric_id": "backlog_exposure_adjusted_mean",
            "category": "exposicion_pendientes_ajustada",
            "definition": "exposición compuesta 0-100 (cantidad+edad+criticidad de pendientes físicos)",
            "unit": "puntuacion_0_100",
            "value": round(float(metrics["backlog_exposure_adjusted_mean"]), 2),
            "decision_use": "priorización de depósitos y rebalanceo de capacidad",
        },
    ]
    return pd.DataFrame(rows)


def write_backlog_metric_governance_doc(taxonomy_df: pd.DataFrame) -> Path:
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Gobierno de Métricas de Pendientes",
        "",
        "## Objetivo",
        "Separar formalmente pendientes físicos y riesgo de diferimiento para evitar indicadores híbridos mal rotulados.",
        "",
        "## Taxonomía oficial",
        "1. pendientes físicos: órdenes reales abiertas.",
        "2. pendientes vencidos: pendientes físicos fuera de ventana operativa (>=14 días).",
        "3. pendientes críticos por edad/severidad: vencimiento severo o riesgo acumulado alto.",
        "4. riesgo de diferimiento: probabilidad de daño al aplazar una intervención (puntuación de decisión).",
        "5. exposición ajustada de pendientes: exposición compuesta 0-100 de los pendientes físicos.",
        "",
        "## Regla de gobierno obligatoria",
        "- Nunca usar `deferral_risk_score` para reportar pendientes físicos.",
        "- Nunca usar pendientes físicos para inferir automáticamente riesgo de diferimiento sin puntuación explícita.",
        "",
        "## Indicador oficial por decisión",
        taxonomy_df.rename(
            columns={
                "category": "categoria",
                "definition": "definicion",
                "unit": "unidad",
                "value": "valor",
                "decision_use": "uso_decision",
            }
        ).to_markdown(index=False),
        "",
        "## Uso ejecutivo",
        "- Dirección de taller: pendientes físicos/vencidos/críticos por depósito.",
        "- Dirección de operaciones: riesgo de diferimiento y exposición ajustada de pendientes.",
        "- Dirección de mantenimiento: combinación de ambos para secuencia de intervención.",
    ]
    out = DOCS_DIR / "backlog_metric_governance.md"
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return out


def sync_narrative_artifacts(force_recompute: bool = True) -> dict[str, Path]:
    metrics = load_or_compute_narrative_metrics(force_recompute=force_recompute)
    backlog_taxonomy_df = _build_backlog_metric_taxonomy(metrics)

    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    memo = _build_memo(metrics)
    (DOCS_DIR / "memo_ejecutivo_es.md").write_text(memo + "\n", encoding="utf-8")

    readme = _build_readme(metrics)
    (ROOT_DIR / "README.md").write_text(readme + "\n", encoding="utf-8")

    backlog_governance_doc = write_backlog_metric_governance_doc(taxonomy_df=backlog_taxonomy_df)
    return {
        "metrics": DATA_PROCESSED_DIR / "narrative_metrics_official.csv",
        "memo": DOCS_DIR / "memo_ejecutivo_es.md",
        "readme": ROOT_DIR / "README.md",
        "backlog_governance_doc": backlog_governance_doc,
    }


if __name__ == "__main__":
    sync_narrative_artifacts(force_recompute=True)
