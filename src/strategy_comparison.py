from __future__ import annotations

from itertools import product

import numpy as np
import pandas as pd

from src.config import DATA_PROCESSED_DIR, DATA_RAW_DIR, DOCS_DIR, OUTPUTS_REPORTS_DIR


STRATEGIES = ["reactiva", "preventiva_rigida", "basada_en_condicion"]
SCENARIOS = ["conservador", "base", "agresivo"]


def _clip(value: float, low: float, high: float) -> float:
    return float(np.clip(value, low, high))


def _build_inputs_tables(
    *,
    fleet_week: pd.DataFrame,
    unit_day: pd.DataFrame,
    priorities: pd.DataFrame,
    backlog_latest: pd.DataFrame,
    fallas: pd.DataFrame,
    mantenimiento: pd.DataFrame,
    disponibilidad: pd.DataFrame,
    early_warning: pd.DataFrame,
    inspection_perf: pd.DataFrame,
) -> tuple[dict[str, float], pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    availability_pct_base = float(fleet_week["availability_rate"].mean() * 100)
    mtbf_base = float(fleet_week["mtbf_proxy"].mean())
    mttr_base = float(fleet_week["mttr_proxy"].mean())
    workshop_util_base = float(fleet_week["capacity_pressure_by_depot"].mean() * 100)
    service_impact_base = float(unit_day["impact_on_service_proxy"].mean())

    planned_h_base = float(disponibilidad["horas_planificadas"].sum())
    downtime_h_base = float(disponibilidad["horas_no_disponibles"].sum())
    service_h_base = max(0.0, planned_h_base - downtime_h_base)

    backlog_latest = backlog_latest.copy()
    backlog_latest["severidad_pendiente"] = backlog_latest["severidad_pendiente"].fillna("baja").str.lower()
    backlog_critical_base = float(
        (
            (
                (backlog_latest["antiguedad_backlog_dias"].fillna(0) >= 21)
                & backlog_latest["severidad_pendiente"].isin(["alta", "critica"])
            )
            | (backlog_latest["riesgo_acumulado"].fillna(0) >= 70)
        ).sum()
    )
    deferral_high_observed = float(priorities["deferral_risk_score"].ge(70).sum())
    # Floor estructural para escenarios (si el observado puntual es 0 en esta corrida sintética).
    deferral_high_reference = max(deferral_high_observed, backlog_critical_base * 0.08)

    failures_total = float(len(fallas))
    repetitive_failures_base = float(fallas["repetitiva_flag"].fillna(0).sum())
    corrective_events_base = float(mantenimiento["correctiva_flag"].fillna(0).sum())
    preventive_events_base = float(mantenimiento["programada_flag"].fillna(0).sum())
    cbm_events_base = float(mantenimiento["basada_en_condicion_flag"].fillna(0).sum())

    if preventive_events_base <= 0:
        preventive_events_base = max(1.0, corrective_events_base * 0.35)

    ew_precision = float(early_warning["precision"].iloc[0]) if not early_warning.empty and "precision" in early_warning.columns else 0.42
    ew_recall = float(early_warning["recall"].iloc[0]) if not early_warning.empty and "recall" in early_warning.columns else 0.30
    if not inspection_perf.empty and "confidence_adjusted_detection_value" in inspection_perf.columns:
        inspection_quality = float(inspection_perf["confidence_adjusted_detection_value"].mean())
    else:
        inspection_quality = 0.32

    observed = {
        "availability_pct_base": availability_pct_base,
        "mtbf_base": mtbf_base,
        "mttr_base": mttr_base,
        "workshop_util_base": workshop_util_base,
        "service_impact_base": service_impact_base,
        "planned_h_base": planned_h_base,
        "downtime_h_base": downtime_h_base,
        "service_h_base": service_h_base,
        "backlog_critical_base": backlog_critical_base,
        "deferral_high_observed": deferral_high_observed,
        "deferral_high_reference": deferral_high_reference,
        "failures_total": failures_total,
        "repetitive_failures_base": repetitive_failures_base,
        "corrective_events_base": corrective_events_base,
        "preventive_events_base": preventive_events_base,
        "cbm_events_base": cbm_events_base,
        "ew_precision": ew_precision,
        "ew_recall": ew_recall,
        "inspection_quality_index": inspection_quality,
        # Proxies económicos explícitos
        "cost_downtime_hour_base": 1300.0,
        "cost_corrective_event_base": 16000.0,
        "cost_preventive_event_base": 8000.0,
        "cost_backlog_critical_case_base": 2500.0,
        "cost_deferral_case_base": 5200.0,
        "cost_service_impact_unit_base": 42000.0,
    }

    observed_table = pd.DataFrame(
        [
            ("availability_pct_base", observed["availability_pct_base"], "observado", "fleet_week_features"),
            ("mtbf_base", observed["mtbf_base"], "observado", "fleet_week_features"),
            ("mttr_base", observed["mttr_base"], "observado", "fleet_week_features"),
            ("downtime_h_base", observed["downtime_h_base"], "observado", "disponibilidad_servicio"),
            ("backlog_critical_base", observed["backlog_critical_base"], "observado", "backlog_mantenimiento"),
            ("deferral_high_observed", observed["deferral_high_observed"], "observado", "workshop_priority_table"),
            ("deferral_high_reference", observed["deferral_high_reference"], "hipotesis_operativa", "backlog+priorities"),
            ("failures_total", observed["failures_total"], "observado", "fallas_historicas"),
            ("corrective_events_base", observed["corrective_events_base"], "observado", "eventos_mantenimiento"),
            ("preventive_events_base", observed["preventive_events_base"], "observado", "eventos_mantenimiento"),
            ("inspection_quality_index", observed["inspection_quality_index"], "observado", "inspection_module_family_performance"),
            ("ew_precision", observed["ew_precision"], "observado", "early_warning_practical_accuracy"),
            ("cost_downtime_hour_base", observed["cost_downtime_hour_base"], "proxy_economico", "assumption"),
            ("cost_corrective_event_base", observed["cost_corrective_event_base"], "proxy_economico", "assumption"),
            ("cost_preventive_event_base", observed["cost_preventive_event_base"], "proxy_economico", "assumption"),
            ("cost_backlog_critical_case_base", observed["cost_backlog_critical_case_base"], "proxy_economico", "assumption"),
            ("cost_deferral_case_base", observed["cost_deferral_case_base"], "proxy_economico", "assumption"),
            ("cost_service_impact_unit_base", observed["cost_service_impact_unit_base"], "proxy_economico", "assumption"),
        ],
        columns=["variable", "valor", "tipo", "fuente"],
    )

    strategy_assumptions = pd.DataFrame(
        [
            {
                "estrategia": "reactiva",
                "proactive_factor": 0.16,
                "predictive_leverage": 0.08,
                "preventive_intensity": 0.22,
                "corrective_dependence": 0.95,
                "enablement_cost_eur": 0.0,
                "hipotesis_operativa": "mínima anticipación, alta dependencia correctiva",
            },
            {
                "estrategia": "preventiva_rigida",
                "proactive_factor": 0.56,
                "predictive_leverage": 0.28,
                "preventive_intensity": 0.95,
                "corrective_dependence": 0.62,
                "enablement_cost_eur": 2400000.0,
                "hipotesis_operativa": "intervención calendarizada alta, menor flexibilidad",
            },
            {
                "estrategia": "basada_en_condicion",
                "proactive_factor": 0.78,
                "predictive_leverage": 0.74,
                "preventive_intensity": 0.66,
                "corrective_dependence": 0.48,
                "enablement_cost_eur": 13000000.0,
                "hipotesis_operativa": "anticipación selectiva, dependencia de señal temprana",
            },
        ]
    )

    scenario_assumptions = pd.DataFrame(
        [
            {
                "scenario_profile": "conservador",
                "failure_stress": 1.12,
                "capacity_factor": 0.90,
                "detection_realization": 0.86,
                "downtime_cost": 1.20,
                "corrective_cost": 1.12,
                "preventive_cost": 1.08,
                "enablement_cost_mult": 1.35,
                "hipotesis": "contexto exigente, menor efectividad de detección y mayor presión de coste",
            },
            {
                "scenario_profile": "base",
                "failure_stress": 1.00,
                "capacity_factor": 1.00,
                "detection_realization": 1.00,
                "downtime_cost": 1.00,
                "corrective_cost": 1.00,
                "preventive_cost": 1.00,
                "enablement_cost_mult": 1.00,
                "hipotesis": "condición operativa esperada",
            },
            {
                "scenario_profile": "agresivo",
                "failure_stress": 0.93,
                "capacity_factor": 1.08,
                "detection_realization": 1.12,
                "downtime_cost": 0.92,
                "corrective_cost": 0.94,
                "preventive_cost": 0.96,
                "enablement_cost_mult": 0.92,
                "hipotesis": "mejor ejecución operativa y mayor calidad de señal",
            },
        ]
    )

    sensitivity_definition = pd.DataFrame(
        [
            ("downtime_cost_mult", "coste indisponibilidad", "sensibilidad económica"),
            ("failure_rate_mult", "tasa de fallo", "sensibilidad técnica"),
            ("capacity_mult", "capacidad taller", "sensibilidad operativa"),
            ("early_detection_mult", "precisión detección temprana", "sensibilidad digital"),
            ("corrective_cost_mult", "coste correctivo", "sensibilidad económica"),
            ("preventive_cost_mult", "coste preventivo", "sensibilidad económica"),
        ],
        columns=["parameter", "interpretacion", "tipo"],
    )
    return observed, observed_table, strategy_assumptions, scenario_assumptions, sensitivity_definition


def _evaluate_strategy(
    *,
    strategy_row: pd.Series,
    scenario_row: pd.Series,
    observed: dict[str, float],
    sens: dict[str, float],
    sensitivity_id: int,
) -> dict[str, float | str | int]:
    strategy = str(strategy_row["estrategia"])

    proactive = float(strategy_row["proactive_factor"])
    predictive = float(strategy_row["predictive_leverage"])
    preventive_intensity = float(strategy_row["preventive_intensity"])
    corrective_dependence = float(strategy_row["corrective_dependence"])
    enablement_cost = float(strategy_row["enablement_cost_eur"])

    failure_rate_mult = float(scenario_row["failure_stress"]) * sens["failure_rate_mult"]
    capacity_mult = float(scenario_row["capacity_factor"]) * sens["capacity_mult"]
    detection_mult = float(scenario_row["detection_realization"]) * sens["early_detection_mult"]

    detection_quality = _clip(observed["inspection_quality_index"] * detection_mult, 0.25, 1.40)
    predictive_effective = predictive * (0.55 + 0.45 * detection_quality) if strategy == "basada_en_condicion" else predictive
    failure_factor = (
        failure_rate_mult
        * (1 - 0.18 * proactive - 0.10 * preventive_intensity)
        * (1 - 0.14 * predictive_effective * (detection_quality - 1))
    )
    failure_factor = _clip(failure_factor, 0.60, 1.70)

    capacity_effective = _clip(capacity_mult, 0.70, 1.20)
    mttr_factor = (1.05 - 0.18 * proactive) / capacity_effective
    mttr_factor = _clip(mttr_factor, 0.72, 1.35)
    downtime_factor = failure_factor * mttr_factor

    corrective_events = observed["corrective_events_base"] * failure_factor * (0.82 + 0.36 * corrective_dependence)
    preventive_events = observed["preventive_events_base"] * (0.35 + preventive_intensity) * (1.03 - 0.10 * capacity_effective)

    backlog_critical = observed["backlog_critical_base"] * (
        0.58 * failure_factor
        + 0.34 * (1 / capacity_effective)
        + 0.20 * corrective_dependence
        - 0.12 * proactive
        - 0.08 * preventive_intensity
    )
    backlog_critical = max(0.0, backlog_critical)

    deferral_high = observed["deferral_high_reference"] * (
        0.48 * failure_factor + 0.30 * (1 / capacity_effective) + 0.22 * (1 - detection_quality)
    )
    deferral_high = max(0.0, deferral_high)

    downtime_h = observed["downtime_h_base"] * downtime_factor
    availability_pct = _clip(100 * (1 - downtime_h / max(observed["planned_h_base"], 1.0)), 78.0, 99.5)
    mtbf = observed["mtbf_base"] * (1 / max(failure_factor, 0.55))
    mttr = observed["mttr_base"] * mttr_factor
    service_hours_preserved = max(0.0, observed["downtime_h_base"] - downtime_h)
    impact_service_proxy = observed["service_impact_base"] * (0.60 + 0.40 * (downtime_h / max(observed["downtime_h_base"], 1.0)))

    repetitive_failures = observed["repetitive_failures_base"] * failure_factor * (
        0.92 + 0.18 * corrective_dependence - 0.14 * predictive
    )
    correctivas_evitables = max(0.0, observed["corrective_events_base"] - corrective_events)

    util_workshop = observed["workshop_util_base"] * (
        0.55 * (corrective_events / max(observed["corrective_events_base"], 1.0))
        + 0.45 * (preventive_events / max(observed["preventive_events_base"], 1.0))
    )
    util_workshop = _clip(util_workshop, 35.0, 145.0)

    interventions_total = max(1.0, corrective_events + preventive_events)
    interv_temprana_ratio = _clip(preventive_events / interventions_total, 0.02, 0.98)
    interv_tardia_ratio = _clip(corrective_events / interventions_total, 0.02, 0.98)

    downtime_cost_unit = observed["cost_downtime_hour_base"] * float(scenario_row["downtime_cost"]) * sens["downtime_cost_mult"]
    corrective_cost_unit = observed["cost_corrective_event_base"] * float(scenario_row["corrective_cost"]) * sens["corrective_cost_mult"]
    preventive_cost_unit = observed["cost_preventive_event_base"] * float(scenario_row["preventive_cost"]) * sens["preventive_cost_mult"]
    backlog_cost_unit = observed["cost_backlog_critical_case_base"] * (1 + 0.15 * (1 / capacity_effective - 1))
    deferral_cost_unit = observed["cost_deferral_case_base"]
    enablement_cost_effective = enablement_cost * float(scenario_row["enablement_cost_mult"])
    if strategy == "basada_en_condicion":
        # Penaliza despliegues con baja calidad de detección, para evitar sobre-claim estructural de CBM.
        enablement_cost_effective *= 1 + 5.5 * max(0.0, 1.0 - detection_quality)
        enablement_cost_effective *= 1 + 1.2 * max(0.0, 1.0 - capacity_effective)

    coste_tecnico_proxy = (
        corrective_events * corrective_cost_unit
        + preventive_events * preventive_cost_unit
        + backlog_critical * backlog_cost_unit
        + deferral_high * deferral_cost_unit
        + enablement_cost_effective
    )
    coste_economico_proxy = downtime_h * downtime_cost_unit + impact_service_proxy * observed["cost_service_impact_unit_base"]
    coste_total_esperado = coste_tecnico_proxy + coste_economico_proxy

    return {
        "scenario_profile": str(scenario_row["scenario_profile"]),
        "sensitivity_id": int(sensitivity_id),
        "estrategia": strategy,
        "availability_pct": round(float(availability_pct), 6),
        "mtbf": round(float(mtbf), 6),
        "mttr": round(float(mttr), 6),
        "downtime_expected_h": round(float(downtime_h), 6),
        "backlog_critical_expected": round(float(backlog_critical), 6),
        "deferral_high_expected": round(float(deferral_high), 6),
        "correctivas_evitables": round(float(correctivas_evitables), 6),
        "service_hours_preserved": round(float(service_hours_preserved), 6),
        "impacto_servicio_proxy": round(float(impact_service_proxy), 6),
        "utilizacion_taller": round(float(util_workshop), 6),
        "intervencion_temprana_ratio": round(float(interv_temprana_ratio), 6),
        "intervencion_tardia_ratio": round(float(interv_tardia_ratio), 6),
        "fallas_repetitivas": round(float(repetitive_failures), 6),
        "coste_tecnico_proxy": round(float(coste_tecnico_proxy), 6),
        "coste_economico_proxy": round(float(coste_economico_proxy), 6),
        "coste_total_esperado": round(float(coste_total_esperado), 6),
        "downtime_cost_mult": sens["downtime_cost_mult"],
        "failure_rate_mult": sens["failure_rate_mult"],
        "capacity_mult": sens["capacity_mult"],
        "early_detection_mult": sens["early_detection_mult"],
        "corrective_cost_mult": sens["corrective_cost_mult"],
        "preventive_cost_mult": sens["preventive_cost_mult"],
        "is_base_point": int(
            all(abs(sens[k] - 1.0) < 1e-9 for k in sens)
            and str(scenario_row["scenario_profile"]) == "base"
        ),
    }


def _run_sensitivity_simulation(
    *,
    observed: dict[str, float],
    strategy_assumptions: pd.DataFrame,
    scenario_assumptions: pd.DataFrame,
) -> pd.DataFrame:
    levels = {
        "downtime_cost_mult": [0.85, 1.00, 1.15],
        "failure_rate_mult": [0.90, 1.00, 1.12],
        "capacity_mult": [0.90, 1.00, 1.10],
        "early_detection_mult": [0.85, 1.00, 1.15],
        "corrective_cost_mult": [0.85, 1.00, 1.20],
        "preventive_cost_mult": [0.90, 1.00, 1.15],
    }

    rows: list[dict[str, float | str | int]] = []
    sens_keys = list(levels.keys())
    grid = list(product(*[levels[k] for k in sens_keys]))
    sensitivity_id = 0
    for scen in scenario_assumptions.itertuples(index=False):
        scenario_row = pd.Series(scen._asdict())
        for combo in grid:
            sensitivity_id += 1
            sens = {k: float(v) for k, v in zip(sens_keys, combo)}
            for _, strat in strategy_assumptions.iterrows():
                rows.append(
                    _evaluate_strategy(
                        strategy_row=strat,
                        scenario_row=scenario_row,
                        observed=observed,
                        sens=sens,
                        sensitivity_id=sensitivity_id,
                    )
                )
    out = pd.DataFrame(rows)

    ref = out[out["estrategia"] == "reactiva"][["scenario_profile", "sensitivity_id", "coste_total_esperado", "downtime_expected_h"]].rename(
        columns={
            "coste_total_esperado": "coste_total_esperado_reactiva",
            "downtime_expected_h": "downtime_expected_h_reactiva",
        }
    )
    out = out.merge(ref, on=["scenario_profile", "sensitivity_id"], how="left")
    out["ahorro_neto_vs_reactiva"] = out["coste_total_esperado_reactiva"] - out["coste_total_esperado"]
    out["horas_servicio_preservadas_vs_reactiva"] = out["downtime_expected_h_reactiva"] - out["downtime_expected_h"]
    return out


def _build_base_comparison(sim: pd.DataFrame) -> pd.DataFrame:
    base = sim[sim["is_base_point"] == 1].copy()
    base = base.sort_values("estrategia").reset_index(drop=True)
    base["fleet_availability"] = base["availability_pct"]
    base["backlog_critico_fisico"] = base["backlog_critical_expected"]
    base["riesgo_diferimiento_alto"] = base["deferral_high_expected"]
    base["horas_indisponibilidad"] = base["downtime_expected_h"]
    base["coste_operativo_proxy"] = base["coste_total_esperado"]
    base["coste_total_esperado"] = base["coste_total_esperado"]
    return base[
        [
            "estrategia",
            "fleet_availability",
            "mtbf",
            "mttr",
            "backlog_critico_fisico",
            "riesgo_diferimiento_alto",
            "correctivas_evitables",
            "fallas_repetitivas",
            "horas_indisponibilidad",
            "impacto_servicio_proxy",
            "utilizacion_taller",
            "intervencion_temprana_ratio",
            "intervencion_tardia_ratio",
            "coste_tecnico_proxy",
            "coste_economico_proxy",
            "coste_operativo_proxy",
            "coste_total_esperado",
            "service_hours_preserved",
            "ahorro_neto_vs_reactiva",
            "horas_servicio_preservadas_vs_reactiva",
        ]
    ].copy()


def _build_sensitivity_outputs(sim: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    # Resumen por estrategia y perfil de escenario.
    summary = (
        sim.groupby(["scenario_profile", "estrategia"], as_index=False)
        .agg(
            coste_total_p10=("coste_total_esperado", lambda s: float(np.quantile(s, 0.10))),
            coste_total_p50=("coste_total_esperado", lambda s: float(np.quantile(s, 0.50))),
            coste_total_p90=("coste_total_esperado", lambda s: float(np.quantile(s, 0.90))),
            downtime_p50=("downtime_expected_h", lambda s: float(np.quantile(s, 0.50))),
            correctivas_evitables_p50=("correctivas_evitables", lambda s: float(np.quantile(s, 0.50))),
            horas_servicio_preservadas_p50=("service_hours_preserved", lambda s: float(np.quantile(s, 0.50))),
            ahorro_neto_p50_vs_reactiva=("ahorro_neto_vs_reactiva", lambda s: float(np.quantile(s, 0.50))),
            downside_ahorro_p10_vs_reactiva=("ahorro_neto_vs_reactiva", lambda s: float(np.quantile(s, 0.10))),
            upside_ahorro_p90_vs_reactiva=("ahorro_neto_vs_reactiva", lambda s: float(np.quantile(s, 0.90))),
            prob_ahorro_positivo=("ahorro_neto_vs_reactiva", lambda s: float((s > 0).mean())),
        )
        .sort_values(["scenario_profile", "estrategia"])
        .reset_index(drop=True)
    )

    value_ranges = (
        sim.groupby("estrategia", as_index=False)
        .agg(
            coste_total_p10=("coste_total_esperado", lambda s: float(np.quantile(s, 0.10))),
            coste_total_p50=("coste_total_esperado", lambda s: float(np.quantile(s, 0.50))),
            coste_total_p90=("coste_total_esperado", lambda s: float(np.quantile(s, 0.90))),
            ahorro_neto_p10_vs_reactiva=("ahorro_neto_vs_reactiva", lambda s: float(np.quantile(s, 0.10))),
            ahorro_neto_p50_vs_reactiva=("ahorro_neto_vs_reactiva", lambda s: float(np.quantile(s, 0.50))),
            ahorro_neto_p90_vs_reactiva=("ahorro_neto_vs_reactiva", lambda s: float(np.quantile(s, 0.90))),
            downside_case=("ahorro_neto_vs_reactiva", "min"),
            upside_case=("ahorro_neto_vs_reactiva", "max"),
            prob_ahorro_positivo=("ahorro_neto_vs_reactiva", lambda s: float((s > 0).mean())),
        )
        .sort_values("estrategia")
        .reset_index(drop=True)
    )

    # Sensibilidad OFAT (lectura ejecutiva rápida por parámetro).
    param_cols = [
        "downtime_cost_mult",
        "failure_rate_mult",
        "capacity_mult",
        "early_detection_mult",
        "corrective_cost_mult",
        "preventive_cost_mult",
    ]
    ofat_parts = []
    for param in param_cols:
        part = (
            sim.groupby(["scenario_profile", "estrategia", param], as_index=False)
            .agg(
                coste_total_mean=("coste_total_esperado", "mean"),
                ahorro_neto_mean=("ahorro_neto_vs_reactiva", "mean"),
            )
            .rename(columns={param: "level"})
        )
        part["parameter"] = param
        ofat_parts.append(part)
    ofat = pd.concat(ofat_parts, ignore_index=True)
    return summary, value_ranges, ofat


def _build_before_after(legacy: pd.DataFrame, redesigned_base: pd.DataFrame) -> pd.DataFrame:
    cols = ["estrategia", "fleet_availability", "horas_indisponibilidad", "coste_operativo_proxy"]
    old = legacy[cols].copy().rename(columns={c: f"{c}_before" for c in cols if c != "estrategia"})
    new = redesigned_base[cols].copy().rename(columns={c: f"{c}_after" for c in cols if c != "estrategia"})
    out = old.merge(new, on="estrategia", how="outer")
    out["delta_fleet_availability_pp"] = out["fleet_availability_after"] - out["fleet_availability_before"]
    out["delta_horas_indisponibilidad"] = out["horas_indisponibilidad_after"] - out["horas_indisponibilidad_before"]
    out["delta_coste_operativo_proxy"] = out["coste_operativo_proxy_after"] - out["coste_operativo_proxy_before"]
    return out


def _write_framework_doc(
    *,
    observed_table: pd.DataFrame,
    strategy_assumptions: pd.DataFrame,
    scenario_assumptions: pd.DataFrame,
    summary: pd.DataFrame,
    value_ranges: pd.DataFrame,
) -> None:
    lines = [
        "# Maintenance Strategy Comparison Framework",
        "",
        "## Objetivo",
        "Comparar estrategias de mantenimiento con separación explícita entre evidencia observada, hipótesis operativas y proxies económicos.",
        "",
        "## 1) Outputs observados",
        observed_table[observed_table["tipo"] == "observado"].to_markdown(index=False),
        "",
        "## 2) Supuestos estructurales por estrategia",
        strategy_assumptions.to_markdown(index=False),
        "",
        "## 3) Hipótesis operativas por escenario",
        scenario_assumptions.to_markdown(index=False),
        "",
        "## 4) Proxies económicos",
        observed_table[observed_table["tipo"] == "proxy_economico"].to_markdown(index=False),
        "",
        "## 5) Resultados por escenario (P10/P50/P90)",
        summary.to_markdown(index=False),
        "",
        "## 6) Rango plausible de valor",
        value_ranges.to_markdown(index=False),
        "",
        "## 7) Reglas de interpretación",
        "- `ahorro_neto_vs_reactiva > 0`: mejora económica frente a reactiva en ese escenario/sensibilidad.",
        "- `downside_case < 0`: existe cola de riesgo económica donde la estrategia puede no compensar.",
        "- `prob_ahorro_positivo`: robustez de la estrategia bajo incertidumbre.",
        "",
        "## 8) Nota metodológica",
        "No se afirma ganador universal: el resultado depende de capacidad, calidad de detección temprana y estructura de costes.",
    ]
    (DOCS_DIR / "maintenance_strategy_comparison_framework.md").write_text("\n".join(lines), encoding="utf-8")


def _write_strategy_doc(
    *,
    base: pd.DataFrame,
    summary: pd.DataFrame,
    value_ranges: pd.DataFrame,
) -> None:
    cbm_base = base[base["estrategia"] == "basada_en_condicion"].iloc[0]
    react_base = base[base["estrategia"] == "reactiva"].iloc[0]

    cons = summary[summary["scenario_profile"] == "conservador"].copy()
    best_cons = cons.sort_values("coste_total_p50", ascending=True).iloc[0]["estrategia"] if not cons.empty else "n/a"
    base_prof = summary[summary["scenario_profile"] == "base"].copy()
    best_base = base_prof.sort_values("coste_total_p50", ascending=True).iloc[0]["estrategia"] if not base_prof.empty else "n/a"
    aggr = summary[summary["scenario_profile"] == "agresivo"].copy()
    best_aggr = aggr.sort_values("coste_total_p50", ascending=True).iloc[0]["estrategia"] if not aggr.empty else "n/a"

    lines = [
        "# Comparación de Estrategias de Mantenimiento",
        "",
        "## Metodología rediseñada",
        "- Separación explícita entre evidencia observada, supuestos estructurales y proxies económicos.",
        "- Escenarios operativos: conservador, base y agresivo.",
        "- Sensibilidad multidimensional: coste de indisponibilidad, tasa de fallo, capacidad de taller, detección temprana, costes correctivo/preventivo.",
        "",
        "## Resultado base (punto central)",
        base.to_markdown(index=False),
        "",
        "## Sensibilidad por escenario (P10/P50/P90)",
        summary.to_markdown(index=False),
        "",
        "## Rango plausible de valor",
        value_ranges.to_markdown(index=False),
        "",
        "## Lectura ejecutiva defendible",
        f"- En el punto base, CBM vs reactiva: disponibilidad +{cbm_base['fleet_availability'] - react_base['fleet_availability']:.2f} p.p.",
        f"- En el punto base, ahorro neto CBM vs reactiva: {cbm_base['ahorro_neto_vs_reactiva']:.0f} EUR.",
        f"- Mejor estrategia por coste esperado (P50) en conservador: {best_cons}.",
        f"- Mejor estrategia por coste esperado (P50) en base: {best_base}.",
        f"- Mejor estrategia por coste esperado (P50) en agresivo: {best_aggr}.",
        "",
        "## Caveats económicos (anti-overclaim)",
        "- El ahorro es proxy y depende de costes unitarios asumidos.",
        "- El desempeño de CBM es sensible a la calidad de detección temprana y a la capacidad efectiva de taller.",
        "- En escenarios conservadores, CBM puede perder ventaja frente a preventiva rígida si el coste de habilitación domina.",
        "",
        "## Recomendación estratégica",
        "Usar CBM donde la señal temprana y la capacidad de ejecución estén maduras; en contexto de baja madurez operativa,",
        "aplicar transición híbrida con preventiva dirigida antes de escalar plenamente CBM.",
    ]
    (DOCS_DIR / "maintenance_strategy_comparison.md").write_text("\n".join(lines), encoding="utf-8")


def run_strategy_comparison() -> pd.DataFrame:
    legacy_path = DATA_PROCESSED_DIR / "comparativo_estrategias.csv"
    legacy = pd.read_csv(legacy_path) if legacy_path.exists() else pd.DataFrame()

    fleet_week = pd.read_csv(DATA_PROCESSED_DIR / "fleet_week_features.csv")
    unit_day = pd.read_csv(DATA_PROCESSED_DIR / "unit_day_features.csv")
    priorities = pd.read_csv(DATA_PROCESSED_DIR / "workshop_priority_table.csv")
    backlog_raw = pd.read_csv(DATA_RAW_DIR / "backlog_mantenimiento.csv")
    fallas = pd.read_csv(DATA_RAW_DIR / "fallas_historicas.csv")
    mantenimiento = pd.read_csv(DATA_RAW_DIR / "eventos_mantenimiento.csv")
    disponibilidad = pd.read_csv(DATA_RAW_DIR / "disponibilidad_servicio.csv")
    early_warning = pd.read_csv(DATA_PROCESSED_DIR / "early_warning_practical_accuracy.csv")
    inspection_perf = pd.read_csv(DATA_PROCESSED_DIR / "inspection_module_family_performance.csv")

    backlog_raw["fecha"] = pd.to_datetime(backlog_raw["fecha"], errors="coerce")
    latest_backlog_day = backlog_raw["fecha"].max()
    backlog_latest = backlog_raw[backlog_raw["fecha"] == latest_backlog_day].copy()

    (
        observed,
        observed_table,
        strategy_assumptions,
        scenario_assumptions,
        sensitivity_definition,
    ) = _build_inputs_tables(
        fleet_week=fleet_week,
        unit_day=unit_day,
        priorities=priorities,
        backlog_latest=backlog_latest,
        fallas=fallas,
        mantenimiento=mantenimiento,
        disponibilidad=disponibilidad,
        early_warning=early_warning,
        inspection_perf=inspection_perf,
    )

    sim = _run_sensitivity_simulation(
        observed=observed,
        strategy_assumptions=strategy_assumptions,
        scenario_assumptions=scenario_assumptions,
    )
    base = _build_base_comparison(sim)
    summary, value_ranges, ofat = _build_sensitivity_outputs(sim)

    # Merge de robustez al comparativo base (sin romper columnas legacy).
    robust_cols = value_ranges[
        ["estrategia", "ahorro_neto_p50_vs_reactiva", "ahorro_neto_p10_vs_reactiva", "ahorro_neto_p90_vs_reactiva", "downside_case", "prob_ahorro_positivo"]
    ].copy()
    base = base.merge(robust_cols, on="estrategia", how="left")
    base["downside_case_ahorro_vs_reactiva"] = base["ahorro_neto_p10_vs_reactiva"]
    base["rango_plausible_valor_min"] = base["ahorro_neto_p10_vs_reactiva"]
    base["rango_plausible_valor_max"] = base["ahorro_neto_p90_vs_reactiva"]

    # Before/after si existía versión previa.
    if not legacy.empty and {"estrategia", "fleet_availability", "horas_indisponibilidad", "coste_operativo_proxy"}.issubset(legacy.columns):
        before_after = _build_before_after(legacy=legacy, redesigned_base=base)
    else:
        before_after = pd.DataFrame()

    DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUTS_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    base.to_csv(DATA_PROCESSED_DIR / "comparativo_estrategias.csv", index=False)
    sim.to_csv(DATA_PROCESSED_DIR / "comparativo_estrategias_sensibilidad.csv", index=False)
    summary.to_csv(DATA_PROCESSED_DIR / "comparativo_estrategias_escenarios.csv", index=False)
    value_ranges.to_csv(DATA_PROCESSED_DIR / "comparativo_estrategias_value_ranges.csv", index=False)
    ofat.to_csv(DATA_PROCESSED_DIR / "comparativo_estrategias_ofat.csv", index=False)
    observed_table.to_csv(DATA_PROCESSED_DIR / "maintenance_strategy_observed_inputs.csv", index=False)
    strategy_assumptions.to_csv(DATA_PROCESSED_DIR / "maintenance_strategy_structural_assumptions.csv", index=False)
    scenario_assumptions.to_csv(DATA_PROCESSED_DIR / "maintenance_strategy_scenario_assumptions.csv", index=False)
    sensitivity_definition.to_csv(DATA_PROCESSED_DIR / "maintenance_strategy_sensitivity_definition.csv", index=False)
    if not before_after.empty:
        before_after.to_csv(DATA_PROCESSED_DIR / "strategy_comparison_before_after.csv", index=False)

    base.to_csv(OUTPUTS_REPORTS_DIR / "comparativo_estrategias.csv", index=False)
    sim.to_csv(OUTPUTS_REPORTS_DIR / "comparativo_estrategias_sensibilidad.csv", index=False)
    summary.to_csv(OUTPUTS_REPORTS_DIR / "comparativo_estrategias_escenarios.csv", index=False)
    value_ranges.to_csv(OUTPUTS_REPORTS_DIR / "comparativo_estrategias_value_ranges.csv", index=False)
    ofat.to_csv(OUTPUTS_REPORTS_DIR / "comparativo_estrategias_ofat.csv", index=False)
    if not before_after.empty:
        before_after.to_csv(OUTPUTS_REPORTS_DIR / "strategy_comparison_before_after.csv", index=False)

    _write_framework_doc(
        observed_table=observed_table,
        strategy_assumptions=strategy_assumptions,
        scenario_assumptions=scenario_assumptions,
        summary=summary,
        value_ranges=value_ranges,
    )
    _write_strategy_doc(base=base, summary=summary, value_ranges=value_ranges)
    return base


if __name__ == "__main__":
    run_strategy_comparison()
