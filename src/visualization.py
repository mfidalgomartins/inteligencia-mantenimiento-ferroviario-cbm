from __future__ import annotations

from pathlib import Path

import matplotlib
import pandas as pd

from src.config import DATA_PROCESSED_DIR, DATA_RAW_DIR, OUTPUTS_CHARTS_DIR

# Backend no interactivo para ejecución reproducible en entornos headless.
matplotlib.use("Agg")
import matplotlib.pyplot as plt


plt.rcParams["figure.figsize"] = (10.8, 5.8)
plt.rcParams["axes.grid"] = True
plt.rcParams["grid.alpha"] = 0.24
plt.rcParams["font.family"] = "DejaVu Sans"


def _save(fig: plt.Figure, filename: str) -> None:
    OUTPUTS_CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(OUTPUTS_CHARTS_DIR / filename, dpi=170, bbox_inches="tight")
    plt.close(fig)


def _chart_01_fleet_availability_trend() -> None:
    df = pd.read_csv(DATA_PROCESSED_DIR / "fleet_week_features.csv")
    df["week_start"] = pd.to_datetime(df["week_start"])
    trend = df.groupby("week_start", as_index=False)["availability_rate"].mean()

    fig, ax = plt.subplots()
    ax.plot(trend["week_start"], trend["availability_rate"] * 100, color="#0f4c81", linewidth=2.2)
    ax.set_title("Disponibilidad de flota: tendencia semanal y estabilidad operativa")
    ax.set_xlabel("Semana")
    ax.set_ylabel("Disponibilidad (%)")
    _save(fig, "01_fleet_availability_trend.png")


def _chart_02_mtbf_mttr_trend() -> None:
    df = pd.read_csv(DATA_PROCESSED_DIR / "fleet_week_features.csv")
    df["week_start"] = pd.to_datetime(df["week_start"])
    trend = df.groupby("week_start", as_index=False).agg(mtbf=("mtbf_proxy", "mean"), mttr=("mttr_proxy", "mean"))

    fig, ax = plt.subplots()
    ax.plot(trend["week_start"], trend["mtbf"], label="MTBF", color="#1f7a8c", linewidth=2)
    ax.plot(trend["week_start"], trend["mttr"], label="MTTR", color="#bf4342", linewidth=2)
    ax.set_title("Fiabilidad semanal: MTBF al alza y MTTR bajo control")
    ax.set_xlabel("Semana")
    ax.set_ylabel("Horas (proxy)")
    ax.legend()
    _save(fig, "02_mtbf_mttr_trend.png")


def _chart_03_backlog_critico_deposito() -> None:
    df = pd.read_csv(DATA_PROCESSED_DIR / "vw_depot_maintenance_pressure.csv")
    df["fecha"] = pd.to_datetime(df["fecha"])
    if "backlog_critical_items" not in df.columns:
        df["backlog_critical_items"] = df.get("backlog_items", 0)
    backlog_rows = df[df["backlog_critical_items"].fillna(0) > 0]
    ref_date = backlog_rows["fecha"].max() if not backlog_rows.empty else df["fecha"].max()
    latest = df[df["fecha"] == ref_date].copy()
    latest = latest.sort_values("backlog_critical_items", ascending=False)

    fig, ax = plt.subplots()
    ax.bar(latest["deposito_id"], latest["backlog_critical_items"], color="#8f2d56")
    ax.set_title("Backlog crítico físico por depósito (edad/severidad)")
    ax.set_xlabel("Depósito")
    ax.set_ylabel("Pendientes críticos físicos")
    _save(fig, "03_backlog_critico_por_deposito.png")


def _chart_04_top_unidades_riesgo() -> None:
    score = pd.read_csv(DATA_PROCESSED_DIR / "unit_unavailability_risk_score.csv")
    top = score.sort_values("unit_unavailability_risk_score", ascending=False).head(15)

    fig, ax = plt.subplots(figsize=(11, 6.2))
    ax.barh(top["unidad_id"][::-1], top["unit_unavailability_risk_score"][::-1], color="#a23e48")
    ax.set_title("Top unidades por riesgo de indisponibilidad")
    ax.set_xlabel("Score de riesgo")
    _save(fig, "04_top_unidades_por_riesgo.png")


def _chart_05_top_componentes_health_deterioration() -> None:
    score = pd.read_csv(DATA_PROCESSED_DIR / "scoring_componentes.csv")
    top = score.sort_values("health_score", ascending=True).head(20)
    labels = (top["unidad_id"] + "-" + top["componente_id"]).str[-18:]

    fig, ax = plt.subplots(figsize=(11.4, 6.2))
    ax.bar(labels, top["health_score"], color="#d97706")
    ax.set_title("Componentes con mayor deterioro (health score bajo)")
    ax.set_xlabel("Unidad-Componente")
    ax.set_ylabel("Health score (alto=mejor)")
    ax.tick_params(axis="x", rotation=70)
    _save(fig, "05_top_componentes_health_deterioration.png")


def _chart_06_fallas_repetitivas_familia() -> None:
    df = pd.read_csv(DATA_PROCESSED_DIR / "risk_segmentation_component_family.csv")

    fig, ax = plt.subplots()
    ax.bar(df["component_family"], df["failure_risk_avg"], color="#5c7c2f")
    ax.set_title("Fallas repetitivas: familias con mayor intensidad de riesgo")
    ax.set_xlabel("Familia")
    ax.set_ylabel("Riesgo medio de falla")
    _save(fig, "06_fallas_repetitivas_por_familia.png")


def _chart_07_alertas_vs_fallas() -> None:
    alertas = pd.read_csv(DATA_PROCESSED_DIR / "early_warning_practical_accuracy.csv")
    row = alertas.iloc[0]

    fig, ax = plt.subplots()
    ax.bar(["TP", "FP", "FN"], [row["tp"], row["fp"], row["fn"]], color=["#2a9d8f", "#e76f51", "#457b9d"])
    ax.set_title("Alerta temprana vs falla real: balance precisión-cobertura")
    ax.set_ylabel("Conteo")
    _save(fig, "07_alertas_tempranas_vs_fallas_reales.png")


def _chart_08_rul_distribution() -> None:
    rul = pd.read_csv(DATA_PROCESSED_DIR / "component_rul_estimate.csv")

    fig, ax = plt.subplots()
    ax.hist(rul["component_rul_estimate"], bins=22, color="#3a86ff", alpha=0.85)
    ax.set_title("Distribución de RUL: ventana de intervención pendiente")
    ax.set_xlabel("RUL (días)")
    ax.set_ylabel("Nº componentes")
    _save(fig, "08_rul_distribution.png")


def _chart_09_service_impact_by_unit() -> None:
    unit = pd.read_csv(DATA_PROCESSED_DIR / "unit_day_features.csv")
    unit["fecha"] = pd.to_datetime(unit["fecha"])
    latest = unit[unit["fecha"] == unit["fecha"].max()].sort_values("impact_on_service_proxy", ascending=False).head(15)

    fig, ax = plt.subplots(figsize=(11, 6.2))
    ax.barh(latest["unidad_id"][::-1], latest["impact_on_service_proxy"][::-1], color="#ff6b6b")
    ax.set_title("Impacto en servicio por unidad: priorización operativa")
    ax.set_xlabel("Impacto servicio (proxy)")
    _save(fig, "09_service_impact_by_unit.png")


def _chart_10_workshop_saturation_comparison() -> None:
    dep = pd.read_csv(DATA_PROCESSED_DIR / "vw_depot_maintenance_pressure.csv")
    dep["fecha"] = pd.to_datetime(dep["fecha"])
    latest = dep[dep["fecha"] == dep["fecha"].max()].sort_values("saturation_ratio", ascending=False)

    fig, ax = plt.subplots()
    ax.bar(latest["deposito_id"], latest["saturation_ratio"], color="#6d597a")
    ax.axhline(1.0, color="#c1121f", linestyle="--", linewidth=1.4)
    ax.set_title("Comparativa de saturación de taller por depósito")
    ax.set_xlabel("Depósito")
    ax.set_ylabel("Saturation ratio")
    _save(fig, "10_workshop_saturation_comparison.png")


def _chart_11_strategy_comparison() -> None:
    comp = pd.read_csv(DATA_PROCESSED_DIR / "comparativo_estrategias.csv")

    fig, ax = plt.subplots(figsize=(11, 6.2))
    ax.bar(comp["estrategia"], comp["coste_operativo_proxy"] / 1e6, color=["#c1121f", "#f4a261", "#2a9d8f"])
    ax.set_title("Reactiva vs Preventiva vs CBM: coste operativo proxy")
    ax.set_xlabel("Estrategia")
    ax.set_ylabel("Coste (M EUR)")
    _save(fig, "11_reactivo_vs_preventivo_vs_cbm.png")


def _chart_12_inspeccion_automatica_familia() -> None:
    perf = pd.read_csv(DATA_PROCESSED_DIR / "inspection_module_family_performance.csv")
    if "inspection_coverage" not in perf.columns:
        perf["inspection_coverage"] = perf.get("coverage_pre_falla", 0.0)
    if "pre_failure_detection_rate" not in perf.columns:
        perf["pre_failure_detection_rate"] = perf.get("coverage_pre_falla", 0.0)
    if "defect_detection_rate" not in perf.columns:
        perf["defect_detection_rate"] = 0.0
    if "false_alert_proxy" not in perf.columns:
        perf["false_alert_proxy"] = 0.0
    if "confidence_adjusted_detection_value" not in perf.columns:
        perf["confidence_adjusted_detection_value"] = 0.0

    fig, ax = plt.subplots(figsize=(11.2, 6.0))
    x = range(len(perf))
    ax.bar([i - 0.26 for i in x], perf["inspection_coverage"], width=0.17, color="#1f7a8c", label="Coverage")
    ax.bar([i - 0.08 for i in x], perf["pre_failure_detection_rate"], width=0.17, color="#2a9d8f", label="Pre-failure")
    ax.bar([i + 0.10 for i in x], perf["defect_detection_rate"], width=0.17, color="#f4a261", label="Detection")
    ax.bar([i + 0.28 for i in x], 1 - perf["false_alert_proxy"], width=0.17, color="#6d597a", label="1 - False alert")
    ax.plot(list(x), perf["confidence_adjusted_detection_value"], color="#d62828", marker="o", linewidth=2.2, label="Conf.-adjusted value")
    ax.set_xticks(list(x))
    ax.set_xticklabels(perf["family"])
    ax.set_ylim(0, 1.05)
    ax.set_title("Inspección automática por familia: cobertura, anticipación y valor de señal")
    ax.set_xlabel("Familia")
    ax.set_ylabel("Tasa / índice (0-1)")
    ax.legend(loc="upper right", ncol=2, fontsize=8)
    _save(fig, "12_inspeccion_automatica_por_familia.png")


def _chart_13_intervention_priority_ranking() -> None:
    prio = pd.read_csv(DATA_PROCESSED_DIR / "workshop_priority_table.csv").head(20)

    fig, ax = plt.subplots(figsize=(11.2, 6.3))
    labels = (prio["unidad_id"] + "-" + prio["componente_id"]).str[-18:]
    ax.barh(labels[::-1], prio["intervention_priority_score"][::-1], color="#b56576")
    ax.set_title("Ranking de prioridad de intervención")
    ax.set_xlabel("Intervention priority score")
    _save(fig, "13_intervention_priority_ranking.png")


def _chart_14_deferral_risk_comparison() -> None:
    impact = pd.read_csv(DATA_PROCESSED_DIR / "impacto_diferimiento_resumen.csv")

    fig, ax = plt.subplots()
    ax.plot(impact["defer_dias"], impact["costo_total_eur"] / 1e6, marker="o", linewidth=2.2, color="#ef476f")
    ax.set_title("Riesgo de diferimiento: coste esperado por retrasar intervención")
    ax.set_xlabel("Días de diferimiento")
    ax.set_ylabel("Coste esperado (M EUR)")
    _save(fig, "14_deferral_risk_comparison.png")


def _chart_15_risk_drivers() -> None:
    drivers = pd.read_csv(DATA_PROCESSED_DIR / "risk_signal_determinants.csv").head(10)

    fig, ax = plt.subplots()
    ax.barh(drivers["feature"][::-1], drivers["spearman_corr_with_failure_risk"][::-1], color="#118ab2")
    ax.set_title("Drivers principales del riesgo de falla")
    ax.set_xlabel("Correlación Spearman")
    _save(fig, "15_drivers_principales_del_riesgo.png")


def _write_chart_index() -> None:
    lines = [
        "# Índice de Visualizaciones",
        "",
        "1. `01_fleet_availability_trend.png`: evolución semanal de disponibilidad de flota.",
        "2. `02_mtbf_mttr_trend.png`: tendencia de fiabilidad y reparabilidad.",
        "3. `03_backlog_critico_por_deposito.png`: presión crítica por depósito.",
        "4. `04_top_unidades_por_riesgo.png`: ranking de unidades más expuestas.",
        "5. `05_top_componentes_health_deterioration.png`: deterioro de salud en componentes.",
        "6. `06_fallas_repetitivas_por_familia.png`: familias con mayor repetición de riesgo.",
        "7. `07_alertas_tempranas_vs_fallas_reales.png`: desempeño práctico de alerta temprana.",
        "8. `08_rul_distribution.png`: distribución de RUL estimado.",
        "9. `09_service_impact_by_unit.png`: impacto servicio por unidad.",
        "10. `10_workshop_saturation_comparison.png`: comparación de saturación de talleres.",
        "11. `11_reactivo_vs_preventivo_vs_cbm.png`: coste operativo comparado por estrategia.",
        "12. `12_inspeccion_automatica_por_familia.png`: cobertura, detección, anticipación y valor ajustado de inspección por familia.",
        "13. `13_intervention_priority_ranking.png`: ranking de intervenciones sugeridas.",
        "14. `14_deferral_risk_comparison.png`: crecimiento de riesgo/coste al diferir.",
        "15. `15_drivers_principales_del_riesgo.png`: señales más determinantes del riesgo.",
    ]
    (OUTPUTS_CHARTS_DIR / "index_visualizaciones.md").write_text("\n".join(lines), encoding="utf-8")


def run_visualizations() -> None:
    _chart_01_fleet_availability_trend()
    _chart_02_mtbf_mttr_trend()
    _chart_03_backlog_critico_deposito()
    _chart_04_top_unidades_riesgo()
    _chart_05_top_componentes_health_deterioration()
    _chart_06_fallas_repetitivas_familia()
    _chart_07_alertas_vs_fallas()
    _chart_08_rul_distribution()
    _chart_09_service_impact_by_unit()
    _chart_10_workshop_saturation_comparison()
    _chart_11_strategy_comparison()
    _chart_12_inspeccion_automatica_familia()
    _chart_13_intervention_priority_ranking()
    _chart_14_deferral_risk_comparison()
    _chart_15_risk_drivers()
    _write_chart_index()


if __name__ == "__main__":
    run_visualizations()
