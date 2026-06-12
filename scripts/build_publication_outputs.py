"""Construye los artefactos finales de publicación del proyecto.

Salida estricta:
    outputs/graphs/*.png
    outputs/dashboard/centro-control-mantenimiento-ferroviario.html
    outputs/reports/informe_analitico_cbm_ferroviario.pdf
"""
from __future__ import annotations

import shutil
import textwrap
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.font_manager as fm
import matplotlib.image as mpimg
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.ticker import FuncFormatter

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data" / "processed"
OUTPUTS = ROOT / "outputs"
GRAPHS = OUTPUTS / "graphs"
DASHBOARD = OUTPUTS / "dashboard"
REPORTS = OUTPUTS / "reports"
REPORT = REPORTS / "informe_analitico_cbm_ferroviario.pdf"
FONT_DIR = ROOT / "assets" / "fonts" / "ttf"

INK = "#172033"
MUTED = "#667085"
LIGHT = "#e6e9ef"
PANEL = "#f5f7fa"
ACCENT = "#2463d4"
DANGER = "#b42318"
WARNING = "#b76e00"
POSITIVE = "#177245"
NEUTRAL = "#8a94a6"
WHITE = "#ffffff"

SANS = "DejaVu Sans"
MONO = "DejaVu Sans Mono"
if FONT_DIR.exists():
    for font in FONT_DIR.glob("*.ttf"):
        fm.fontManager.addfont(str(font))
    installed = {font.name for font in fm.fontManager.ttflist}
    if "IBM Plex Sans" in installed:
        SANS = "IBM Plex Sans"
    if "IBM Plex Mono" in installed:
        MONO = "IBM Plex Mono"

plt.rcParams.update(
    {
        "font.family": SANS,
        "figure.facecolor": WHITE,
        "axes.facecolor": WHITE,
        "axes.edgecolor": LIGHT,
        "axes.labelcolor": MUTED,
        "xtick.color": MUTED,
        "ytick.color": MUTED,
        "text.color": INK,
        "figure.dpi": 180,
    }
)


def read(name: str) -> pd.DataFrame:
    return pd.read_csv(DATA / name)


def fmt_int(value: float) -> str:
    return f"{int(round(float(value))):,}".replace(",", ".")


def fmt_dec(value: float, decimals: int = 1) -> str:
    return f"{float(value):,.{decimals}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def fmt_money_m(value: float, decimals: int = 1) -> str:
    return f"{fmt_dec(float(value) / 1_000_000, decimals)} M€"


def clean_axis(ax, grid: str | None = None) -> None:
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color(LIGHT)
    ax.spines["bottom"].set_color(LIGHT)
    ax.tick_params(length=0, labelsize=9)
    if grid:
        ax.grid(axis=grid, color=LIGHT, linewidth=0.8, zorder=0)


def chart_title(ax, title: str, subtitle: str) -> None:
    ax.set_title(title, loc="left", fontsize=15, fontweight="bold", pad=28, color=INK)
    ax.text(0, 1.025, subtitle, transform=ax.transAxes, fontsize=9.5, color=MUTED, va="bottom")


def save_chart(fig, filename: str, source: str) -> None:
    source_name = {
        "fleet_week_features.csv": "mart semanal de flota",
        "comparativo_estrategias.csv": "simulación de estrategias de mantenimiento",
        "impacto_diferimiento_resumen.csv": "simulación de diferimiento",
        "workshop_priority_table.csv": "modelo de priorización de taller",
        "vw_depot_maintenance_pressure.csv": "mart de presión de talleres",
        "unit_unavailability_risk_score.csv": "modelo de riesgo de indisponibilidad",
        "scheduling_status_distribution.csv": "simulación de programación de taller",
        "risk_signal_determinants.csv": "análisis de determinantes de riesgo",
        "component_model_scores.csv": "modelo de salud y riesgo de componentes",
        "risk_segmentation_component_family.csv": "segmentación de riesgo por familia",
        "kpi_backlog_mas_critico.csv": "mart de backlog por depósito",
        "kpi_fallas_repetitivas_mas_frecuentes.csv": "mart de fallos repetitivos",
        "rul_family_discrimination_before_after.csv": "validación del proxy de vida remanente",
        "rul_distribution_before_after.csv": "validación del proxy de vida remanente",
        "inspection_module_family_performance.csv": "módulo de inspección automática",
        "workshop_capacity_calendar.csv": "calendario de capacidad de taller",
        "kpi_unidades_mayor_indisponibilidad.csv": "mart de indisponibilidad por unidad",
        "comparativo_estrategias_escenarios.csv": "simulación de sensibilidad estratégica",
        "governance_contract_checks.csv": "registro de controles de gobernanza",
    }.get(source, source)
    fig.text(0.012, 0.008, f"Fuente: {source_name}", fontsize=7.5, color=MUTED)
    fig.savefig(GRAPHS / filename, bbox_inches="tight", pad_inches=0.28, dpi=190)
    plt.close(fig)


def build_charts() -> list[dict[str, str]]:
    charts: list[dict[str, str]] = []
    family_es = {"pantograph": "Pantógrafo", "bogie": "Bogie", "brake": "Freno", "wheel": "Rueda"}

    fleet = read("fleet_week_features.csv")
    fleet["week_start"] = pd.to_datetime(fleet["week_start"])
    trend = fleet.groupby("week_start", as_index=False)["availability_rate"].mean()
    trend["rolling"] = trend["availability_rate"].rolling(8, min_periods=2).mean() * 100
    fig, ax = plt.subplots(figsize=(9.2, 5.2))
    ax.plot(trend["week_start"], trend["availability_rate"] * 100, color=NEUTRAL, lw=1.1, alpha=0.55)
    ax.plot(trend["week_start"], trend["rolling"], color=ACCENT, lw=2.5)
    ax.axhline(trend["availability_rate"].mean() * 100, color=INK, lw=1, ls=(0, (4, 3)))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:.0f}%"))
    ax.set_ylabel("Disponibilidad")
    clean_axis(ax, "y")
    chart_title(ax, "La disponibilidad se mantiene alta, pero no elimina la presión operativa",
                "Media semanal y tendencia móvil de ocho semanas")
    save_chart(fig, "01_tendencia_disponibilidad.png", "fleet_week_features.csv")
    charts.append({"file": "01_tendencia_disponibilidad.png", "title": "Tendencia de disponibilidad"})

    strategy = read("comparativo_estrategias.csv").set_index("estrategia")
    order = ["preventiva_rigida", "basada_en_condicion", "reactiva"]
    labels = ["Preventiva", "CBM", "Reactiva"]
    net = strategy.loc[order, "ahorro_neto_vs_reactiva"] / 1e6
    p10 = strategy.loc[order, "ahorro_neto_p10_vs_reactiva"] / 1e6
    p90 = strategy.loc[order, "ahorro_neto_p90_vs_reactiva"] / 1e6
    fig, ax = plt.subplots(figsize=(9.2, 5.2))
    x = np.arange(3)
    colors = [NEUTRAL, ACCENT, INK]
    ax.bar(x, net, color=colors, width=0.56, zorder=3)
    ax.errorbar(x[:2], net.iloc[:2], yerr=[net.iloc[:2] - p10.iloc[:2], p90.iloc[:2] - net.iloc[:2]],
                fmt="none", ecolor=INK, capsize=5, lw=1.3, zorder=4)
    ax.axhline(0, color=INK, lw=1)
    ax.set_xticks(x, labels)
    ax.set_ylabel("Diferencial neto frente a reactiva, M€")
    for i, value in enumerate(net):
        ax.text(i, value - 2 if value < 0 else value + 2, f"{fmt_dec(value, 1)} M€",
                ha="center", va="top" if value < 0 else "bottom", fontweight="bold", fontsize=10)
    clean_axis(ax, "y")
    chart_title(ax, "La ventaja de servicio de CBM no se traduce en ahorro bajo los supuestos actuales",
                "Valor esperado y rango P10-P90 frente a la estrategia reactiva")
    save_chart(fig, "02_valor_estrategias.png", "comparativo_estrategias.csv")
    charts.append({"file": "02_valor_estrategias.png", "title": "Valor y variancia estratégica"})

    defer = read("impacto_diferimiento_resumen.csv")
    fig, ax = plt.subplots(figsize=(9.2, 5.2))
    ax.plot(defer["defer_dias"], defer["costo_total_eur"] / 1e6, color=ACCENT, marker="o", lw=2.4)
    ax.set_xlabel("Días de diferimiento")
    ax.set_ylabel("Coste proxy, M€")
    ax2 = ax.twinx()
    ax2.plot(defer["defer_dias"], defer["downtime_total_h"], color=DANGER, marker="s", lw=2.2)
    ax2.set_ylabel("Indisponibilidad, horas", color=DANGER)
    ax2.tick_params(colors=DANGER, length=0)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_color(LIGHT)
    clean_axis(ax, "y")
    chart_title(ax, "Aplazar traslada coste al servicio y amplifica la indisponibilidad",
                "Escenarios de diferimiento de la cola priorizada")
    save_chart(fig, "03_coste_diferimiento.png", "impacto_diferimiento_resumen.csv")
    charts.append({"file": "03_coste_diferimiento.png", "title": "Coste del diferimiento"})

    priority = read("workshop_priority_table.csv").nlargest(15, "intervention_priority_score").sort_values(
        "intervention_priority_score"
    )
    fig, ax = plt.subplots(figsize=(9.2, 6.1))
    labels_p = priority["unidad_id"] + " · " + priority["component_family"].map(family_es).fillna(priority["component_family"])
    colors_p = [ACCENT] * len(priority)
    colors_p[-1] = DANGER
    ax.barh(labels_p, priority["intervention_priority_score"], color=colors_p, zorder=3)
    for y, value in enumerate(priority["intervention_priority_score"]):
        ax.text(value + 0.3, y, fmt_dec(value, 1), va="center", fontsize=8.5, fontfamily=MONO)
    ax.set_xlim(78, 94)
    ax.set_xlabel("Índice de prioridad")
    clean_axis(ax, "x")
    chart_title(ax, "La cola ejecutiva está dominada por pocos casos de prioridad muy alta",
                "Quince primeras intervenciones por score compuesto")
    save_chart(fig, "04_ranking_intervenciones.png", "workshop_priority_table.csv")
    charts.append({"file": "04_ranking_intervenciones.png", "title": "Ranking de intervenciones"})

    depot = read("vw_depot_maintenance_pressure.csv")
    depot["fecha"] = pd.to_datetime(depot["fecha"])
    snap = depot[depot["fecha"] == depot["fecha"].max()].sort_values("saturation_ratio")
    fig, ax = plt.subplots(figsize=(9.2, 5.4))
    colors_d = [ACCENT if x < 0.4 else WARNING if x < 0.8 else DANGER for x in snap["saturation_ratio"]]
    ax.barh(snap["deposito_id"], snap["saturation_ratio"] * 100, color=colors_d, zorder=3)
    for y, value in enumerate(snap["saturation_ratio"] * 100):
        ax.text(value + 0.5, y, f"{fmt_dec(value, 1)}%", va="center", fontsize=9, fontfamily=MONO)
    ax.set_xlabel("Saturación del taller")
    ax.set_xlim(0, max(55, snap["saturation_ratio"].max() * 115))
    clean_axis(ax, "x")
    chart_title(ax, "La presión de taller está desigualmente distribuida entre depósitos",
                f"Snapshot a {snap['fecha'].max().date()}")
    save_chart(fig, "05_saturacion_depositos.png", "vw_depot_maintenance_pressure.csv")
    charts.append({"file": "05_saturacion_depositos.png", "title": "Presión por depósito"})

    risk = read("unit_unavailability_risk_score.csv")["unit_unavailability_risk_score"]
    threshold = risk.mean() + 1.5 * risk.std()
    fig, ax = plt.subplots(figsize=(9.2, 5.2))
    ax.hist(risk, bins=18, color=NEUTRAL, edgecolor=WHITE, zorder=3)
    ax.axvline(threshold, color=DANGER, lw=2)
    ax.text(threshold + 0.5, ax.get_ylim()[1] * 0.88, f"alto riesgo: {fmt_dec(threshold, 1)}", color=DANGER, fontsize=9)
    ax.set_xlabel("Score de riesgo de indisponibilidad")
    ax.set_ylabel("Unidades")
    clean_axis(ax, "y")
    chart_title(ax, "El riesgo de unidad se concentra en una cola estrecha",
                f"{int((risk >= threshold).sum())} de {len(risk)} unidades superan media + 1,5σ")
    save_chart(fig, "06_distribucion_riesgo_unidades.png", "unit_unavailability_risk_score.csv")
    charts.append({"file": "06_distribucion_riesgo_unidades.png", "title": "Distribución de riesgo"})

    status = read("scheduling_status_distribution.csv")
    pivot = status.pivot(index="scenario", columns="estado_intervencion", values="share_pct").fillna(0)
    scenario_order = ["baseline_greedy_21d", "heuristica_redisenada_35d"]
    state_order = ["programada", "programable_proxima_ventana", "pendiente_repuesto", "pendiente_capacidad"]
    state_labels = {
        "programada": "Programada",
        "programable_proxima_ventana": "Próxima ventana",
        "pendiente_repuesto": "Pendiente de repuesto",
        "pendiente_capacidad": "Pendiente de capacidad",
    }
    state_colors = [POSITIVE, ACCENT, WARNING, DANGER]
    fig, ax = plt.subplots(figsize=(9.2, 4.8))
    left = np.zeros(2)
    for state, color in zip(state_order, state_colors, strict=True):
        values = pivot.reindex(scenario_order).get(state, pd.Series([0, 0], index=scenario_order)).values
        ax.barh(["Secuencia básica · 21 días", "Heurística rediseñada · 35 días"], values, left=left, color=color,
                label=state_labels[state])
        for i, value in enumerate(values):
            if value >= 7:
                ax.text(left[i] + value / 2, i, f"{value:.0f}%", ha="center", va="center", color=WHITE,
                        fontsize=9, fontweight="bold")
        left += values
    ax.set_xlim(0, 100)
    ax.set_xlabel("Distribución de casos")
    ax.legend(frameon=False, ncol=2, loc="upper center", bbox_to_anchor=(0.5, -0.16), fontsize=8.5)
    clean_axis(ax, "x")
    chart_title(ax, "El rediseño del scheduling convierte capacidad bloqueada en casos accionables",
                "Comparación de estados antes y después")
    save_chart(fig, "07_scheduling_antes_despues.png", "scheduling_status_distribution.csv")
    charts.append({"file": "07_scheduling_antes_despues.png", "title": "Scheduling antes y después"})

    drivers = read("risk_signal_determinants.csv").sort_values("spearman_corr_with_failure_risk")
    driver_labels = {
        "deterioration_index": "Índice de deterioro",
        "degradation_velocity": "Velocidad de degradación",
        "shock_event_count": "Eventos de choque",
        "anomaly_count_30d": "Anomalías en 30 días",
        "inspection_defect_score_recent": "Defecto reciente en inspección",
        "operating_stress_index": "Estrés operativo",
        "backlog_exposure_flag": "Exposición a backlog",
        "days_since_last_failure": "Días desde último fallo",
        "days_since_last_maintenance": "Días desde mantenimiento",
        "maintenance_restoration_index": "Restauración por mantenimiento",
    }
    fig, ax = plt.subplots(figsize=(9.2, 5.6))
    driver_colors = [DANGER if x < 0 else ACCENT for x in drivers["spearman_corr_with_failure_risk"]]
    ax.barh(drivers["feature"].map(driver_labels), drivers["spearman_corr_with_failure_risk"],
            color=driver_colors, zorder=3)
    ax.axvline(0, color=INK, lw=0.8)
    ax.set_xlabel("Correlación de Spearman con riesgo de fallo")
    clean_axis(ax, "x")
    chart_title(ax, "Deterioro y velocidad explican la mayor parte del ordenamiento de riesgo",
                "Relación monotónica entre señales y score de fallo")
    save_chart(fig, "08_determinantes_riesgo.png", "risk_signal_determinants.csv")
    charts.append({"file": "08_determinantes_riesgo.png", "title": "Determinantes del riesgo"})

    components = read("component_model_scores.csv")
    fig, ax = plt.subplots(figsize=(9.2, 5.5))
    ax.scatter(components["component_health_score"], components["component_failure_risk_score"] * 100,
               c=components["deterioration_index"], cmap="Blues", s=15, alpha=0.62, edgecolors="none")
    ax.set_xlabel("Salud del componente")
    ax.set_ylabel("Riesgo de fallo, %")
    clean_axis(ax, "both")
    chart_title(ax, "La salud baja separa el riesgo, pero no sustituye la evidencia de deterioro",
                "Cada punto representa un componente crítico")
    save_chart(fig, "09_salud_vs_riesgo.png", "component_model_scores.csv")
    charts.append({"file": "09_salud_vs_riesgo.png", "title": "Correlación salud y riesgo"})

    families = read("risk_segmentation_component_family.csv").sort_values("failure_risk_avg")
    fig, ax = plt.subplots(figsize=(9.2, 5.1))
    ax.barh(families["component_family"].map(family_es), families["failure_risk_avg"] * 100,
            color=[NEUTRAL, NEUTRAL, NEUTRAL, ACCENT])
    for y, value in enumerate(families["failure_risk_avg"] * 100):
        ax.text(value + 0.4, y, f"{fmt_dec(value, 1)}%", va="center", fontsize=9, fontfamily=MONO)
    ax.set_xlabel("Riesgo medio de fallo")
    clean_axis(ax, "x")
    chart_title(ax, "Pantógrafos combinan peor salud y mayor riesgo medio",
                "Segmentación por familia técnica")
    save_chart(fig, "10_riesgo_por_familia.png", "risk_segmentation_component_family.csv")
    charts.append({"file": "10_riesgo_por_familia.png", "title": "Composición del riesgo por familia"})

    backlog = read("kpi_backlog_mas_critico.csv").sort_values("backlog_fisico_total", ascending=False)
    share = backlog["backlog_fisico_total"] / backlog["backlog_fisico_total"].sum() * 100
    cumulative = share.cumsum()
    fig, ax = plt.subplots(figsize=(9.2, 5.3))
    ax.bar(backlog["deposito_id"], share, color=ACCENT, zorder=3)
    ax.set_ylabel("Cuota del backlog físico, %")
    ax2 = ax.twinx()
    ax2.plot(backlog["deposito_id"], cumulative, color=DANGER, marker="o", lw=2)
    ax2.set_ylim(0, 105)
    ax2.set_ylabel("Cuota acumulada, %", color=DANGER)
    ax2.tick_params(colors=DANGER, length=0)
    ax2.spines["top"].set_visible(False)
    clean_axis(ax, "y")
    chart_title(ax, "El backlog se concentra en pocos depósitos",
                "Participación y curva acumulada sobre el histórico agregado")
    save_chart(fig, "11_concentracion_backlog.png", "kpi_backlog_mas_critico.csv")
    charts.append({"file": "11_concentracion_backlog.png", "title": "Concentración del backlog"})

    failures = read("kpi_fallas_repetitivas_mas_frecuentes.csv").sort_values("repetitive_events", ascending=False).head(15)
    failures = failures.iloc[::-1]
    labels_f = failures["subsistema"] + " · " + failures["modo_falla"].str.replace("_", " ")
    fig, ax = plt.subplots(figsize=(9.2, 6.2))
    ax.barh(labels_f, failures["repetitive_events"], color=ACCENT)
    ax.set_xlabel("Eventos repetitivos")
    clean_axis(ax, "x")
    chart_title(ax, "Un conjunto limitado de modos concentra la repetición de fallos",
                "Quince combinaciones subsistema y modo con mayor frecuencia repetitiva")
    save_chart(fig, "12_pareto_fallos_repetitivos.png", "kpi_fallas_repetitivas_mas_frecuentes.csv")
    charts.append({"file": "12_pareto_fallos_repetitivos.png", "title": "Concentración de fallos repetitivos"})

    rul = read("rul_family_discrimination_before_after.csv").sort_values("new_p50")
    fig, ax = plt.subplots(figsize=(9.2, 5.2))
    y = np.arange(len(rul))
    ax.hlines(y, rul["new_p10"], rul["new_p90"], color=NEUTRAL, lw=5)
    ax.scatter(rul["new_p50"], y, color=ACCENT, s=70, zorder=3)
    ax.set_yticks(y, rul["component_family"].map(family_es))
    ax.set_xlabel("RUL proxy, días")
    clean_axis(ax, "x")
    chart_title(ax, "El RUL discrimina ventanas de intervención por familia",
                "Mediana y rango P10-P90 del proxy rediseñado")
    save_chart(fig, "13_cohortes_rul_familia.png", "rul_family_discrimination_before_after.csv")
    charts.append({"file": "13_cohortes_rul_familia.png", "title": "Coortes RUL por familia"})

    rul_dist = read("rul_distribution_before_after.csv")
    fig, ax = plt.subplots(figsize=(9.2, 5.0))
    methods = ["Legacy lineal", "Proxy por familia"]
    p50 = rul_dist["p50_rul"].values
    p10 = rul_dist["p10_rul"].values
    p90 = rul_dist["p90_rul"].values
    x = np.arange(2)
    ax.bar(x, p50, color=[NEUTRAL, ACCENT], width=0.55)
    ax.errorbar(x, p50, yerr=[p50 - p10, p90 - p50], fmt="none", ecolor=INK, capsize=6)
    ax.set_xticks(x, methods)
    ax.set_ylabel("RUL, días")
    clean_axis(ax, "y")
    chart_title(ax, "El rediseño elimina la saturación artificial del RUL legacy",
                "Mediana y rango P10-P90 antes y después")
    save_chart(fig, "14_rul_antes_despues.png", "rul_distribution_before_after.csv")
    charts.append({"file": "14_rul_antes_despues.png", "title": "RUL antes y después"})

    inspection = read("inspection_module_family_performance.csv").sort_values("pre_failure_detection_rate")
    fig, ax = plt.subplots(figsize=(9.2, 5.2))
    ax.barh(inspection["family"].map(family_es), inspection["pre_failure_detection_rate"] * 100, color=ACCENT)
    for y, value in enumerate(inspection["pre_failure_detection_rate"] * 100):
        ax.text(value + 0.5, y, f"{fmt_dec(value, 1)}%", va="center", fontsize=9, fontfamily=MONO)
    ax.set_xlim(0, 105)
    ax.set_xlabel("Fallos con detección previa, %")
    clean_axis(ax, "x")
    chart_title(ax, "La inspección automática tiene cobertura desigual por familia",
                "Tasa observada de detección previa a fallo")
    save_chart(fig, "15_inspeccion_por_familia.png", "inspection_module_family_performance.csv")
    charts.append({"file": "15_inspeccion_por_familia.png", "title": "Cobertura de inspección"})

    capacity = read("workshop_capacity_calendar.csv").groupby("deposito_id", as_index=False).agg(
        capacity=("total_capacity_h", "sum"), used=("total_used_h", "sum")
    )
    capacity["utilization"] = capacity["used"] / capacity["capacity"] * 100
    capacity = capacity.sort_values("utilization")
    fig, ax = plt.subplots(figsize=(9.2, 5.2))
    ax.barh(capacity["deposito_id"], capacity["utilization"], color=ACCENT)
    for y, value in enumerate(capacity["utilization"]):
        ax.text(value + 0.5, y, f"{fmt_dec(value, 1)}%", va="center", fontsize=9, fontfamily=MONO)
    ax.set_xlabel("Utilización programada de capacidad, %")
    clean_axis(ax, "x")
    chart_title(ax, "La capacidad programada conserva holgura, pero no está alineada con la cola",
                "Utilización acumulada del calendario de 35 días")
    save_chart(fig, "16_utilizacion_capacidad.png", "workshop_capacity_calendar.csv")
    charts.append({"file": "16_utilizacion_capacidad.png", "title": "Utilización de capacidad"})

    unavailable = read("kpi_unidades_mayor_indisponibilidad.csv").nlargest(15, "horas_no_disponibles_total").sort_values(
        "horas_no_disponibles_total"
    )
    fig, ax = plt.subplots(figsize=(9.2, 5.8))
    ax.barh(unavailable["unidad_id"], unavailable["horas_no_disponibles_total"], color=ACCENT)
    ax.set_xlabel("Horas no disponibles acumuladas")
    clean_axis(ax, "x")
    chart_title(ax, "La indisponibilidad histórica también está concentrada",
                "Quince unidades con mayor tiempo fuera de servicio")
    save_chart(fig, "17_ranking_indisponibilidad.png", "kpi_unidades_mayor_indisponibilidad.csv")
    charts.append({"file": "17_ranking_indisponibilidad.png", "title": "Ranking de indisponibilidad"})

    scenarios = read("comparativo_estrategias_escenarios.csv")
    labels_s = {"basada_en_condicion": "CBM", "preventiva_rigida": "Preventiva", "reactiva": "Reactiva"}
    fig, ax = plt.subplots(figsize=(9.2, 5.2))
    for i, (strategy_name, group) in enumerate(scenarios.groupby("estrategia")):
        group = group.sort_values("coste_total_p50")
        ax.scatter(group["coste_total_p50"] / 1e6, [i] * len(group), color=ACCENT if strategy_name == "basada_en_condicion" else NEUTRAL,
                   s=70, alpha=0.85)
        ax.hlines(i, group["coste_total_p50"].min() / 1e6, group["coste_total_p50"].max() / 1e6, color=LIGHT, lw=5, zorder=0)
    strategies_sorted = sorted(scenarios["estrategia"].unique())
    ax.set_yticks(range(len(strategies_sorted)), [labels_s[s] for s in strategies_sorted])
    ax.set_xlabel("Coste total P50, M€")
    clean_axis(ax, "x")
    chart_title(ax, "La conclusión económica depende del perfil de escenario",
                "Dispersión del coste P50 entre perfiles de sensibilidad")
    save_chart(fig, "18_variancia_escenarios.png", "comparativo_estrategias_escenarios.csv")
    charts.append({"file": "18_variancia_escenarios.png", "title": "Variancia entre escenarios"})

    checks = read("governance_contract_checks.csv")
    summary = checks.groupby(["severity", "passed"]).size().unstack(fill_value=0)
    fig, ax = plt.subplots(figsize=(9.2, 4.8))
    summary.plot(kind="bar", stacked=True, color=[DANGER, POSITIVE], ax=ax, zorder=3)
    ax.set_xlabel("")
    ax.set_ylabel("Contratos verificados")
    ax.legend(["Fallidos", "Aprobados"], frameon=False, ncol=2)
    clean_axis(ax, "y")
    chart_title(ax, "Los contratos de publicación no presentan bloqueos activos",
                "Resultado de validaciones por severidad")
    save_chart(fig, "19_gobernanza_validaciones.png", "governance_contract_checks.csv")
    charts.append({"file": "19_gobernanza_validaciones.png", "title": "Gobernanza y validaciones"})

    return charts


def official_metrics() -> dict[str, str]:
    metrics = read("narrative_metrics_official.csv")
    return dict(zip(metrics["metric_id"], metrics["metric_value"], strict=True))


class PublicationReport:
    def __init__(self, pdf: PdfPages):
        self.pdf = pdf
        self.page_number = 0

    def _base(self, section: str = ""):
        fig = plt.figure(figsize=(8.27, 11.69), facecolor=WHITE)
        self.page_number += 1
        if self.page_number > 1:
            fig.text(0.075, 0.965, "Sistema de Inteligencia de Mantenimiento Ferroviario",
                     fontsize=7.5, color=MUTED)
            fig.text(0.925, 0.965, section, fontsize=7.5, color=MUTED, ha="right")
            fig.add_artist(plt.Line2D([0.075, 0.925], [0.952, 0.952], color=LIGHT, lw=0.8))
            fig.add_artist(plt.Line2D([0.075, 0.925], [0.045, 0.045], color=LIGHT, lw=0.8))
            fig.text(0.075, 0.025, "Datos sintéticos y métricas proxy. Uso analítico y demostrativo.",
                     fontsize=7, color=MUTED)
            fig.text(0.925, 0.025, str(self.page_number), fontsize=7.5, color=MUTED, ha="right")
        return fig

    def cover(self, m: dict[str, str]) -> None:
        fig = self._base()
        fig.add_artist(plt.Rectangle((0, 0.72), 1, 0.28, facecolor=INK, transform=fig.transFigure))
        fig.text(0.08, 0.91, "INFORME ANALÍTICO", fontsize=10, color=WHITE, fontweight="bold")
        fig.text(0.08, 0.835, "Sistema de Inteligencia de\nMantenimiento Ferroviario",
                 fontsize=28, color=WHITE, fontweight="bold", va="top", linespacing=1.1)
        fig.text(0.08, 0.75, "Riesgo, priorización, capacidad de taller y valor estratégico", fontsize=12, color="#d8e2f5")
        fig.text(0.08, 0.62, "Alcance analítico", fontsize=10, color=ACCENT, fontweight="bold")
        fig.text(
            0.08,
            0.57,
            f"{fmt_int(m['n_unidades'])} unidades · {fmt_int(m['n_componentes'])} componentes críticos · "
            f"{fmt_int(m['n_depositos'])} depósitos\nCobertura {m['coverage_start']} a {m['coverage_end']}",
            fontsize=13,
            color=INK,
            linespacing=1.6,
        )
        fig.text(0.08, 0.47, "Propósito", fontsize=10, color=ACCENT, fontweight="bold")
        fig.text(0.08, 0.42,
                 "Convertir señales técnicas, backlog y restricciones de capacidad en una cola de intervención\n"
                 "defendible, y separar con claridad el valor operativo del coste económico proxy.",
                 fontsize=11.5, color=MUTED, linespacing=1.6)
        fig.text(0.08, 0.13, "Generado automáticamente", fontsize=9, color=MUTED)
        self.pdf.savefig(fig)
        plt.close(fig)

    def contents(self) -> None:
        fig = self._base("Índice")
        fig.text(0.075, 0.89, "Índice", fontsize=25, color=INK, fontweight="bold")
        rows = [
            ("1", "Resumen ejecutivo", "3"),
            ("2", "Contexto y objetivos", "5"),
            ("3", "Datos y metodología", "7"),
            ("4", "Marco analítico", "11"),
            ("5", "Hallazgos", "13"),
            ("6", "Riesgos, limitaciones y cautelas", "32"),
            ("7", "Recomendaciones y prioridades", "34"),
            ("8", "Apéndice", "36"),
        ]
        y = 0.81
        for number, title, page in rows:
            fig.text(0.08, y, number, fontsize=10, color=ACCENT, fontweight="bold")
            fig.text(0.13, y, title, fontsize=11, color=INK)
            fig.add_artist(plt.Line2D([0.13, 0.86], [y - 0.012, y - 0.012], color=LIGHT, lw=0.7))
            fig.text(0.91, y, page, fontsize=10, color=MUTED, ha="right", fontfamily=MONO)
            y -= 0.075
        self.pdf.savefig(fig)
        plt.close(fig)

    def page(self, section: str, title: str, paragraphs: list[str], chart: str | None = None,
             callout: str | None = None, table: tuple[list[str], list[list[str]]] | None = None) -> None:
        fig = self._base(section)
        fig.text(0.075, 0.91, section.upper(), fontsize=8.5, color=ACCENT, fontweight="bold")
        title_lines = textwrap.wrap(title, 58)
        for index, line in enumerate(title_lines):
            fig.text(0.075, 0.865 - index * 0.034, line, fontsize=19, color=INK, fontweight="bold", va="top")
        y = 0.80 - max(0, len(title_lines) - 1) * 0.034
        if callout:
            lines = textwrap.wrap(callout, 94)
            height = 0.035 + 0.025 * len(lines)
            fig.add_artist(plt.Rectangle((0.075, y - height), 0.85, height, facecolor=PANEL,
                                         edgecolor=LIGHT, lw=0.8, transform=fig.transFigure))
            for line in lines:
                fig.text(0.095, y - 0.025, line, fontsize=10, color=INK, fontweight="bold", va="top")
                y -= 0.025
            y -= 0.035
        for paragraph in paragraphs:
            lines = textwrap.wrap(paragraph, 112)
            for line in lines:
                fig.text(0.075, y, line, fontsize=9.3, color=MUTED, va="top")
                y -= 0.0195
            y -= 0.018
        if chart:
            image = mpimg.imread(GRAPHS / chart)
            available = max(0.26, y - 0.075)
            height = min(0.46, available)
            ax = fig.add_axes([0.075, y - height, 0.85, height])
            ax.imshow(image)
            ax.axis("off")
        if table:
            headers, rows = table
            ax = fig.add_axes([0.075, 0.09, 0.85, max(0.26, y - 0.12)])
            ax.axis("off")
            tab = ax.table(cellText=rows, colLabels=headers, loc="upper left", cellLoc="left", colLoc="left")
            tab.auto_set_font_size(False)
            tab.set_fontsize(7.5)
            tab.scale(1, 1.35)
            for (row, _), cell in tab.get_celld().items():
                cell.set_edgecolor(LIGHT)
                cell.set_linewidth(0.6)
                cell.set_facecolor(INK if row == 0 else WHITE)
                cell.set_text_props(color=WHITE if row == 0 else MUTED, fontweight="bold" if row == 0 else "normal")
        self.pdf.savefig(fig)
        plt.close(fig)


def build_report(charts: list[dict[str, str]]) -> None:
    m = official_metrics()
    strategy = read("comparativo_estrategias.csv").set_index("estrategia")
    sched = read("scheduling_before_after_metrics.csv").set_index("scenario")
    defer = read("impacto_diferimiento_resumen.csv").set_index("defer_dias")
    risk = read("unit_unavailability_risk_score.csv")["unit_unavailability_risk_score"]
    priority = read("workshop_priority_table.csv").sort_values("intervention_priority_score", ascending=False)
    top = priority.iloc[0]
    families = read("risk_segmentation_component_family.csv").set_index("component_family")
    rul = read("rul_distribution_before_after.csv").set_index("metodo")
    checks = read("governance_contract_checks.csv")
    backlog = read("kpi_backlog_mas_critico.csv")
    failures = read("kpi_fallas_repetitivas_mas_frecuentes.csv")
    inspection = read("inspection_module_family_performance.csv").set_index("family")
    capacity = read("workshop_capacity_calendar.csv")

    cbm = strategy.loc["basada_en_condicion"]
    reactive = strategy.loc["reactiva"]
    baseline = sched.loc["baseline_greedy_21d"]
    redesigned = sched.loc["heuristica_redisenada_35d"]
    top3_backlog_share = backlog.nlargest(3, "backlog_fisico_total")["backlog_fisico_total"].sum() / backlog[
        "backlog_fisico_total"
    ].sum() * 100
    top5_fail_share = failures.nlargest(5, "repetitive_events")["repetitive_events"].sum() / failures[
        "repetitive_events"
    ].sum() * 100
    threshold = risk.mean() + 1.5 * risk.std()
    high_risk = int((risk >= threshold).sum())
    avg_capacity_use = capacity["total_used_h"].sum() / capacity["total_capacity_h"].sum() * 100

    with PdfPages(REPORT) as pdf:
        rep = PublicationReport(pdf)
        rep.cover(m)
        rep.contents()
        rep.page(
            "1 · Resumen ejecutivo",
            "La plataforma ordena una decisión operativa concreta, no una lista de indicadores",
            [
                f"La flota sintética mantiene una disponibilidad media del {fmt_dec(m['fleet_availability_pct'], 2)}%, "
                f"pero acumula {fmt_int(m['backlog_physical_items_count'])} pendientes físicos. De ellos, "
                f"{fmt_int(m['backlog_overdue_items_count'])} están vencidos y {fmt_int(m['backlog_critical_physical_count'])} "
                "cumplen la definición de backlog crítico físico. La disponibilidad agregada, por sí sola, oculta una "
                "exposición de mantenimiento que exige ordenar la cola por riesgo y no por volumen.",
                f"El sistema identifica {high_risk} unidades en la cola estadística de alto riesgo y "
                f"{fmt_int(m['high_deferral_risk_cases_count'])} intervenciones con riesgo de diferimiento igual o superior a 70. "
                f"La primera decisión es intervenir {top['unidad_id']} / {top['componente_id']}, familia "
                f"{top['component_family']}, con score de prioridad {fmt_dec(top['intervention_priority_score'], 1)} y "
                f"riesgo de diferimiento {fmt_dec(top['deferral_risk_score'], 1)}.",
            ],
            callout="Conclusión ejecutiva: la prioridad inmediata es proteger servicio mediante una cola de intervención basada en riesgo, mientras se corrige la asignación de capacidad entre depósitos.",
        )
        rep.page(
            "1 · Resumen ejecutivo",
            "Cinco decisiones concentran el valor de gestión",
            [
                f"Primero, ejecutar la cabeza de cola y proteger los {fmt_int(m['high_deferral_risk_cases_count'])} casos de alto riesgo de diferimiento. "
                f"Segundo, adoptar la heurística de 35 días: los casos accionables suben del {fmt_dec(baseline['actionable_pct'], 1)}% "
                f"al {fmt_dec(redesigned['actionable_pct'], 1)}%, mientras el riesgo residual no atendido baja del "
                f"{fmt_dec(baseline['riesgo_residual_no_atendido_pct'], 1)}% al {fmt_dec(redesigned['riesgo_residual_no_atendido_pct'], 1)}%.",
                f"Tercero, evitar aplazamientos no justificados. Diferir 14 días añade {fmt_money_m(float(m['deferral_cost_delta_14d_eur']), 2)} "
                f"y {fmt_dec(m['deferral_downtime_delta_14d_h'], 0)} horas de indisponibilidad frente a intervenir en el día cero. "
                "Cuarto, concentrar acciones de fiabilidad en pantógrafos, que presentan el mayor riesgo medio por familia. "
                "Quinto, tratar CBM como una decisión de nivel de servicio: bajo los supuestos actuales mejora disponibilidad, "
                f"pero su diferencial neto esperado frente a reactiva es {fmt_money_m(cbm['ahorro_neto_vs_reactiva'])}.",
            ],
            callout="La recomendación económica no es declarar un ahorro inexistente. Es fijar el valor corporativo de la disponibilidad adicional y recalibrar costes antes de comprometer inversión.",
        )
        rep.page(
            "2 · Contexto y objetivos",
            "El problema de gestión combina condición técnica, presión de servicio y capacidad limitada",
            [
                "La operación ferroviaria no puede decidir mantenimiento con una única señal. Un componente con salud baja puede "
                "tener escaso impacto de servicio; una unidad con riesgo moderado puede convertirse en prioridad si el aplazamiento "
                "eleva la probabilidad de fallo o si la siguiente ventana operativa es distante. El proyecto integra estas tensiones "
                "en una secuencia de intervención explícita.",
                f"El alcance cubre {fmt_int(m['n_flotas'])} flotas, {fmt_int(m['n_unidades'])} unidades, "
                f"{fmt_int(m['n_componentes'])} componentes críticos y {fmt_int(m['n_depositos'])} depósitos. "
                "La cobertura temporal abarca dos años de señales sintéticas, operación, fallos, inspecciones y mantenimiento.",
            ],
        )
        rep.page(
            "2 · Contexto y objetivos",
            "Los objetivos analíticos se traducen en preguntas de decisión",
            [
                "El análisis responde a seis preguntas. Qué unidades y componentes deben entrar primero en taller. Cuánto cuesta "
                "aplazar cada intervención. Dónde existe presión o capacidad ociosa. Qué señales explican el riesgo. Qué familias "
                "requieren una política diferenciada. Y qué estrategia de mantenimiento ofrece el mejor equilibrio entre servicio, "
                "coste y riesgo.",
                "La estructura de salida evita mezclar conceptos que suelen confundirse. Backlog físico mide carga pendiente real. "
                "Riesgo de diferimiento mide el daño esperado de posponer una decisión. Salud describe condición. Riesgo de fallo "
                "ordena propensión a corto plazo. RUL se utiliza como ventana relativa, no como fecha exacta de fallo.",
            ],
        )
        rep.page(
            "3 · Datos y metodología",
            "La base de datos integra señales técnicas y contexto operativo a varias granularidades",
            [
                "La capa analítica parte de datos sintéticos deterministas de sensores, inspecciones automáticas, fallos, eventos "
                "de mantenimiento, disponibilidad, demanda de servicio y backlog. Las tablas procesadas preservan granularidades "
                "separadas para componente-día, unidad-día, flota-semana, depósito-día e intervención-snapshot.",
                "Esta separación evita sumar métricas incompatibles. La disponibilidad se analiza por flota-semana; el riesgo y RUL "
                "por componente; la presión de taller por depósito-día; y la prioridad por intervención en el snapshot más reciente.",
            ],
        )
        rep.page(
            "3 · Datos y metodología",
            "La calidad se controla mediante contratos de datos y métricas",
            [
                f"El registro de gobernanza contiene {len(checks)} verificaciones activas. "
                f"{int(checks['passed'].sum())} están aprobadas y {int((~checks['passed']).sum())} fallan. "
                f"No existen bloqueos de publicación activos: {int(checks['publish_blocker'].sum())}. "
                "Los contratos fijan fuente de verdad, granularidad, definición, agregación y anti-interpretación.",
                "La narrativa ejecutiva se alimenta del registro oficial de métricas. Esta decisión reduce el riesgo de que dashboard, "
                "README e informe comuniquen cifras distintas para disponibilidad, backlog, riesgo de diferimiento o valor estratégico.",
            ],
            chart="19_gobernanza_validaciones.png",
        )
        rep.page(
            "3 · Datos y metodología",
            "Los scores son interpretables y orientados a priorización",
            [
                "La salud del componente combina condición estimada, deterioro, velocidad de degradación, defectos de inspección y "
                "restauración tras mantenimiento. El riesgo de fallo utiliza una combinación lineal y sigmoide de señales, estrés "
                "operativo, historial y exposición. El score de prioridad añade impacto de servicio, riesgo de diferimiento y ajuste "
                "al taller recomendado.",
                "El diseño favorece trazabilidad sobre complejidad predictiva. Cada recomendación conserva regla, señal dominante, "
                "confianza y rationale. Esta arquitectura es adecuada para prototipado avanzado, pero requiere recalibración antes "
                "de uso operacional real.",
            ],
        )
        rep.page(
            "3 · Datos y metodología",
            "La comparación estratégica separa servicio, técnica y economía",
            [
                "Las estrategias reactiva, preventiva rígida y basada en condición se comparan bajo supuestos estructurales y perfiles "
                "de escenario. La simulación incorpora indisponibilidad, correctivas evitables, backlog crítico, utilización de taller, "
                "coste técnico, coste económico y coste de habilitación.",
                "Los importes son proxies técnico-operativos. No representan presupuesto, P&L ni oferta contractual. Su función es "
                "hacer visibles los trade-offs y probar la robustez de una decisión ante variaciones de coste, capacidad, estrés de "
                "fallo y realización de detección.",
            ],
        )
        rep.page(
            "4 · Marco analítico",
            "La cadena de decisión conecta evidencia con ejecución",
            [
                "La secuencia analítica comienza en la calidad del dato, transforma señales en estado y riesgo, estima una ventana "
                "relativa de intervención, ordena casos por prioridad, asigna capacidad y cuantifica el coste de diferir. El resultado "
                "final no es un score aislado, sino una recomendación ejecutable con depósito, secuencia y ventana sugerida.",
                "La lógica mantiene un control esencial: una señal técnica no genera por sí sola una orden. La prioridad emerge de la "
                "combinación entre riesgo, impacto de servicio, oportunidad operativa y restricciones del taller.",
            ],
        )
        rep.page(
            "4 · Marco analítico",
            "Las métricas se interpretan dentro de límites explícitos",
            [
                "Disponibilidad alta no implica bajo riesgo futuro. Backlog elevado no convierte todas las órdenes en urgentes. "
                "Correlación con riesgo no demuestra causalidad física. RUL no es una fecha de fallo. Ahorro proxy no es ahorro financiero. "
                "Estas distinciones limitan conclusiones incorrectas y permiten que cada métrica cumpla una función operativa concreta.",
                "El marco utiliza umbrales para organizar decisiones, no para simular certeza. Los casos cercanos a un umbral deben "
                "revisarse junto con confianza de señal, criticidad de servicio y disponibilidad de repuesto.",
            ],
        )

        finding_pages = [
            (
                "La disponibilidad agregada es estable, pero es una medida retrospectiva",
                "01_tendencia_disponibilidad.png",
                [
                    f"La disponibilidad media del periodo es {fmt_dec(m['fleet_availability_pct'], 2)}%. La serie semanal conserva un nivel alto, "
                    "sin una ruptura estructural visible que por sí sola justifique una intervención estratégica.",
                    "La estabilidad no invalida la cola de riesgo. Disponibilidad resume lo ocurrido; el score de indisponibilidad y el riesgo de "
                    "diferimiento intentan ordenar exposición futura. La gestión debe usar ambas capas y evitar sustituir una por otra.",
                ],
            ),
            (
                "CBM compra disponibilidad, pero no ahorro bajo el escenario actual",
                "02_valor_estrategias.png",
                [
                    f"CBM alcanza {fmt_dec(cbm['fleet_availability'], 2)}% de disponibilidad frente a {fmt_dec(reactive['fleet_availability'], 2)}% "
                    f"en reactiva, una mejora de {fmt_dec(cbm['fleet_availability'] - reactive['fleet_availability'], 2)} puntos porcentuales.",
                    f"El diferencial neto esperado es {fmt_money_m(cbm['ahorro_neto_vs_reactiva'])}; el rango plausible permanece entre "
                    f"{fmt_money_m(cbm['ahorro_neto_p10_vs_reactiva'])} y {fmt_money_m(cbm['ahorro_neto_p90_vs_reactiva'])}. "
                    "La decisión necesita un precio sombra explícito para disponibilidad y horas de servicio preservadas.",
                ],
            ),
            (
                "El coste de aplazar crece de forma monotónica",
                "03_coste_diferimiento.png",
                [
                    f"El escenario base estima {fmt_money_m(defer.loc[0, 'costo_total_eur'], 2)} y "
                    f"{fmt_dec(defer.loc[0, 'downtime_total_h'], 0)} horas de indisponibilidad. A 45 días, los valores suben a "
                    f"{fmt_money_m(defer.loc[45, 'costo_total_eur'], 2)} y {fmt_dec(defer.loc[45, 'downtime_total_h'], 0)} horas.",
                    "La curva confirma que el aplazamiento no debe utilizarse como mecanismo general para absorber presión de taller. Debe reservarse "
                    "para casos con bajo riesgo de diferimiento, señal débil o restricción material demostrable.",
                ],
            ),
            (
                "La primera decisión está claramente identificada",
                "04_ranking_intervenciones.png",
                [
                    f"{top['unidad_id']} / {top['componente_id']} encabeza la cola con prioridad {fmt_dec(top['intervention_priority_score'], 1)}, "
                    f"riesgo de fallo a 30 días de {fmt_dec(top['prob_fallo_30d'] * 100, 1)}% y RUL proxy de {fmt_int(top['component_rul_estimate'])} días.",
                    f"La recomendación es {str(top['decision_type']).replace('_', ' ')} en {top['deposito_recomendado']}. "
                    "La cola superior presenta scores próximos entre sí; la ejecución debe conservar la secuencia y registrar cualquier excepción.",
                ],
            ),
            (
                "La presión de depósito exige rebalanceo, no expansión inmediata",
                "05_saturacion_depositos.png",
                [
                    f"El depósito más exigido es {m['top_depot_by_saturation']} con {fmt_dec(m['top_depot_saturation_pct'], 1)}% de saturación en el snapshot. "
                    "El nivel no demuestra una insuficiencia estructural de capacidad a escala de red.",
                    "La dispersión entre depósitos apunta primero a reasignación controlada, compatibilidad técnica y ajuste del calendario. Una inversión "
                    "en capacidad antes de corregir la asignación consolidaría ineficiencias existentes.",
                ],
            ),
            (
                "Nueve unidades forman la cola estadística de alto riesgo",
                "06_distribucion_riesgo_unidades.png",
                [
                    f"El umbral media + 1,5σ se sitúa en {fmt_dec(threshold, 1)} puntos y captura {high_risk} unidades. "
                    "Esta cola estrecha permite concentrar revisión técnica sin convertir el modelo en una alarma generalizada.",
                    "El umbral es una regla de segmentación, no una frontera física. La dirección debe revisar estabilidad del ranking y resultados de "
                    "intervención antes de usarlo como criterio automático de parada.",
                ],
            ),
            (
                "El scheduling rediseñado cambia la naturaleza del cuello de botella",
                "07_scheduling_antes_despues.png",
                [
                    f"Los casos accionables aumentan {fmt_dec(redesigned['actionable_pct'] - baseline['actionable_pct'], 1)} puntos porcentuales. "
                    f"Los pendientes por capacidad bajan del {fmt_dec(baseline['pendiente_capacidad_pct'], 1)}% al "
                    f"{fmt_dec(redesigned['pendiente_capacidad_pct'], 1)}%.",
                    f"El valor capturado proxy sube de {fmt_money_m(baseline['valor_capturado_proxy'], 2)} a "
                    f"{fmt_money_m(redesigned['valor_capturado_proxy'], 2)}. El rediseño introduce un {fmt_dec(redesigned['pendiente_repuesto_pct'], 1)}% "
                    "pendiente de repuesto, trasladando parte del problema a coordinación de suministro.",
                ],
            ),
            (
                "Las señales de deterioro dominan el ordenamiento de riesgo",
                "08_determinantes_riesgo.png",
                [
                    "Deterioration index y degradation velocity presentan correlaciones de Spearman de 0,82 y 0,77 con el riesgo de fallo. "
                    "Shock events y anomalías también aportan separación relevante.",
                    "Maintenance restoration index muestra relación negativa, coherente con una reducción de riesgo tras intervención. Las variables con "
                    "correlación nula no deben eliminarse automáticamente: pueden actuar como reglas de contexto o restricciones.",
                ],
            ),
            (
                "La salud resume condición, pero el riesgo incorpora trayectoria",
                "09_salud_vs_riesgo.png",
                [
                    "La nube de componentes confirma una relación inversa entre salud y riesgo, con dispersión material para niveles de salud similares. "
                    "Esa dispersión procede de velocidad de degradación, anomalías, estrés y restauración.",
                    "Una política basada solo en salud perdería componentes con trayectoria adversa y sobrerreaccionaría a componentes estables. La cola "
                    "de taller debe conservar el score de riesgo y la evidencia subyacente.",
                ],
            ),
            (
                "Pantógrafos requieren una política específica",
                "10_riesgo_por_familia.png",
                [
                    f"Pantógrafos presentan riesgo medio de {fmt_dec(families.loc['pantograph', 'failure_risk_avg'] * 100, 1)}% y salud media de "
                    f"{fmt_dec(families.loc['pantograph', 'health_score_avg'], 1)} puntos, los peores valores entre las familias modeladas.",
                    "La diferencia justifica revisar umbrales, repuestos, capacidad técnica e inspección por familia. Un único SLA de mantenimiento para "
                    "todas las familias diluye una exposición claramente segmentada.",
                ],
            ),
            (
                "Tres depósitos concentran la mayor parte de la carga histórica",
                "11_concentracion_backlog.png",
                [
                    f"Los tres depósitos con mayor backlog agregado concentran el {fmt_dec(top3_backlog_share, 1)}% del total histórico representado. "
                    "Esta concentración facilita una intervención focalizada sobre disciplina de cierre, antigüedad y asignación.",
                    "El backlog agregado por periodo no equivale al snapshot ejecutivo de pendientes físicos. Se utiliza aquí para localizar concentración "
                    "persistente, no para comunicar el volumen actual.",
                ],
            ),
            (
                "La repetición de fallos es tratable como cartera priorizada",
                "12_pareto_fallos_repetitivos.png",
                [
                    f"Los cinco modos con mayor repetición concentran el {fmt_dec(top5_fail_share, 1)}% de los eventos repetitivos incluidos en el ranking. "
                    "Arco eléctrico, bloqueos de actuador y pérdida de contacto aparecen entre los primeros casos.",
                    "La respuesta debe combinar análisis de causa raíz, revisión de mantenimiento estándar y seguimiento de reincidencia después de la "
                    "intervención. Reducir repetición requiere cerrar causas, no solo acelerar correctivas.",
                ],
            ),
            (
                "Las ventanas RUL difieren de forma material entre familias",
                "13_cohortes_rul_familia.png",
                [
                    "La mediana RUL del pantógrafo es 26 días, frente a 82 días para bogies. Los rangos P10-P90 también difieren, lo que confirma que una "
                    "regla uniforme de intervención perdería discriminación operativa.",
                    "El RUL debe usarse por bucket y familia, junto con confianza y riesgo de servicio. La cifra no representa una cuenta atrás física ni "
                    "debe comunicarse como fecha garantizada de fallo.",
                ],
            ),
            (
                "El RUL rediseñado corrige una distribución sin utilidad operativa",
                "14_rul_antes_despues.png",
                [
                    f"El modelo legacy tenía mediana de {fmt_int(rul.loc['legacy_lineal_365', 'p50_rul'])} días y "
                    f"{fmt_dec(rul.loc['legacy_lineal_365', 'share_rul_cap'] * 100, 1)}% de observaciones en el tope. "
                    f"El proxy por familia reduce la mediana a {fmt_int(rul.loc['nuevo_proxy_familia', 'p50_rul'])} días y el tope a "
                    f"{fmt_dec(rul.loc['nuevo_proxy_familia', 'share_rul_cap'] * 100, 1)}%.",
                    "La mejora principal es discriminación. El nuevo proxy distribuye casos entre ventanas útiles y permite combinar urgencia con capacidad.",
                ],
            ),
            (
                "La inspección automática no rinde igual en todas las familias",
                "15_inspeccion_por_familia.png",
                [
                    f"La tasa de detección previa a fallo varía desde {fmt_dec(inspection['pre_failure_detection_rate'].min() * 100, 1)}% hasta "
                    f"{fmt_dec(inspection['pre_failure_detection_rate'].max() * 100, 1)}%. La cobertura completa de componentes no elimina diferencias "
                    "de precisión, seguimiento o capacidad de detección.",
                    "La gestión debe medir detección ajustada por confianza, falsa alerta y mantenimiento posterior. Aumentar inspecciones sin mejorar "
                    "conversión a intervención puede elevar carga sin reducir riesgo.",
                ],
            ),
            (
                "La capacidad disponible y la cola priorizada no están plenamente alineadas",
                "16_utilizacion_capacidad.png",
                [
                    f"La utilización agregada del calendario de 35 días es {fmt_dec(avg_capacity_use, 1)}%. La cifra confirma que el bloqueo no se explica "
                    "solo por falta absoluta de horas; también intervienen compatibilidad, secuencia, ventana y depósito.",
                    "La prioridad inmediata es mejorar asignación y flexibilidad controlada. La expansión de capacidad debe justificarse después de medir "
                    "qué restricciones permanecen activas tras el rediseño.",
                ],
            ),
            (
                "La indisponibilidad histórica está concentrada en pocas unidades",
                "17_ranking_indisponibilidad.png",
                [
                    "El ranking histórico identifica unidades que acumulan una exposición de servicio desproporcionada. Este resultado debe cruzarse "
                    "con la cola de riesgo actual para distinguir persistencia estructural de episodios ya corregidos.",
                    "Las unidades recurrentes requieren una revisión de causa raíz y estrategia de activo. Repetir intervenciones aisladas sobre el mismo "
                    "activo puede consumir capacidad sin resolver el mecanismo de indisponibilidad.",
                ],
            ),
            (
                "La incertidumbre económica cambia la confianza, no la dirección operativa",
                "18_variancia_escenarios.png",
                [
                    "Los perfiles de escenario desplazan el coste P50 de cada estrategia y amplían la incertidumbre de comparación. La preventiva rígida "
                    "se aproxima al coste reactivo en algunos escenarios; CBM conserva el mayor coste proxy bajo los supuestos vigentes.",
                    "La dirección operativa permanece más estable: reducir correctivas, proteger horas de servicio y anticipar componentes críticos. "
                    "La decisión de inversión, en cambio, debe esperar calibración financiera con costes reales.",
                ],
            ),
        ]
        for title, chart, paragraphs in finding_pages:
            rep.page("5 · Hallazgos", title, paragraphs, chart=chart)

        rep.page(
            "6 · Riesgos, limitaciones y cautelas",
            "Los resultados son rigurosos dentro de un entorno sintético, no evidencia de producción",
            [
                "Los datos reproducen relaciones plausibles, pero no una red ferroviaria real. Los scores, umbrales y distribuciones pueden demostrar "
                "arquitectura y lógica de decisión; no validan precisión externa, causalidad ni impacto financiero.",
                "Antes de producción deben recalibrarse fallos, costes, disponibilidad, tiempos de reparación, capacidad, ventanas operativas y taxonomías. "
                "La validación debe incluir backtesting temporal, estabilidad por flota y depósito, revisión de falsos positivos y resultados de órdenes ejecutadas.",
            ],
            callout="Riesgo principal: convertir una demostración coherente en una política automática sin calibración con datos reales.",
        )
        rep.page(
            "6 · Riesgos, limitaciones y cautelas",
            "Cuatro interpretaciones incorrectas deben bloquearse",
            [
                "Primera, tratar el RUL como fecha exacta de fallo. Segunda, interpretar el coste proxy como caso financiero cerrado. Tercera, confundir backlog "
                "físico con riesgo de diferimiento. Cuarta, asumir que una correlación del score identifica una causa física.",
                "El scheduling es heurístico y no garantiza óptimo global. La inspección automática se evalúa con métricas proxy y cobertura sintética. "
                "La recomendación de depósito requiere validar competencias, repuestos, logística y restricciones contractuales no presentes en el modelo.",
            ],
        )
        rep.page(
            "7 · Recomendaciones y prioridades",
            "Prioridades para los próximos 30 días",
            [
                f"Ejecutar la intervención de {top['unidad_id']} / {top['componente_id']} y revisar diariamente la cola de "
                f"{fmt_int(m['high_deferral_risk_cases_count'])} casos de alto riesgo de diferimiento. Registrar motivo y autorizador para cualquier excepción.",
                f"Adoptar la heurística de 35 días como baseline operativo controlado. Medir semanalmente accionabilidad, pendientes por capacidad, "
                f"pendientes por repuesto, riesgo residual y valor capturado. Rebalancear carga empezando por {m['top_depot_by_saturation']}.",
                "Crear un frente específico para pantógrafos y los cinco modos de fallo más repetitivos. Vincular cada acción a causa raíz, responsable, "
                "fecha objetivo y evidencia posterior de reducción de reincidencia.",
            ],
            callout="Prioridad 1: proteger servicio en la cabeza de cola. Prioridad 2: corregir asignación de taller. Prioridad 3: reducir repetición por familia.",
        )
        rep.page(
            "7 · Recomendaciones y prioridades",
            "Decisiones de 60 a 120 días",
            [
                "Calibrar el riesgo de fallo y los buckets RUL con histórico real, usando cortes temporales y métricas por familia. Incorporar la respuesta "
                "a intervención para medir si el score ordena casos que efectivamente reducen fallos e indisponibilidad.",
                "Construir el caso económico con costes corporativos auditables. El análisis debe valorar horas de servicio preservadas, correctivas evitadas, "
                "coste de habilitación, inventario, capacitación y riesgo de ejecución. Sin ese paso, la estrategia CBM no debe presentarse como ahorro.",
                "Evaluar optimización matemática del scheduling solo después de estabilizar reglas, datos y restricciones. Un optimizador sobre supuestos débiles "
                "produce precisión aparente, no una mejor decisión.",
            ],
        )

        metric_registry = read("narrative_metrics_official.csv")

        def appendix_value(value: object, unit: str) -> str:
            if unit in {"count"}:
                return fmt_int(float(value))
            if unit in {"count_proxy"}:
                return fmt_dec(float(value), 1)
            if unit in {"pct"}:
                return f"{fmt_dec(float(value), 2)}%"
            if unit in {"pp"}:
                return f"{fmt_dec(float(value), 2)} p.p."
            if unit in {"eur", "eur_proxy"}:
                return fmt_money_m(float(value), 2)
            if unit in {"hours"}:
                return f"{fmt_dec(float(value), 1)} h"
            if unit in {"score_0_100"}:
                return fmt_dec(float(value), 1)
            if unit in {"ratio_0_1"}:
                return fmt_dec(float(value), 3)
            return str(value)

        source_labels = {
            "pipeline_runtime": "pipeline",
            "fleet_week_features.csv": "fleet_week",
            "unit_unavailability_risk_score.csv": "unit_risk",
            "backlog_mantenimiento.csv": "backlog",
            "comparativo_estrategias.csv": "estrategias",
            "inspection_module_value_comparison.csv": "inspección",
            "vw_depot_maintenance_pressure.csv": "presión_depósito",
            "workshop_priority_table.csv": "prioridad_taller",
            "impacto_diferimiento_resumen.csv": "diferimiento",
            "flotas.csv": "flotas",
            "unidades.csv": "unidades",
            "depositos.csv": "depósitos",
            "componentes_criticos.csv": "componentes",
        }
        metric_rows = [
            [
                str(row.label)[:40],
                appendix_value(row.metric_value, str(row.unit)),
                str(row.unit),
                source_labels.get(Path(str(row.source_of_truth)).name, Path(str(row.source_of_truth)).stem[:20]),
            ]
            for row in metric_registry.itertuples()
        ]
        rep.page(
            "8 · Apéndice",
            "Registro oficial de métricas narrativas",
            ["La tabla resume las métricas utilizadas para comunicar resultados. Las definiciones completas, filtros y reglas de agregación permanecen en el registro procesado."],
            table=(["Métrica", "Valor", "Unidad", "Fuente"], metric_rows[:16]),
        )
        rep.page(
            "8 · Apéndice",
            "Registro oficial de métricas narrativas, continuación",
            ["La segunda parte incluye decisiones, saturación, diferimiento y dimensiones del alcance."],
            table=(["Métrica", "Valor", "Unidad", "Fuente"], metric_rows[16:]),
        )
        method_rows = [
            ["Salud", "Componente snapshot", "Condición técnica 0-100", "No es probabilidad de fallo"],
            ["Riesgo 30d", "Componente snapshot", "Ordena propensión a corto plazo", "No implica causalidad"],
            ["RUL proxy", "Componente snapshot", "Bucket de ventana relativa", "No es fecha exacta"],
            ["Backlog físico", "Snapshot depósito/global", "Carga pendiente real", "No equivale a riesgo de diferimiento"],
            ["Riesgo diferimiento", "Intervención snapshot", "Daño esperado de aplazar", "No equivale a backlog crítico"],
            ["Saturación", "Depósito-día", "Uso relativo de capacidad", "No mide productividad"],
            ["Valor estratégico", "Estrategia-escenario", "Trade-off técnico-operativo", "No es P&L"],
        ]
        rep.page(
            "8 · Apéndice",
            "Guía de interpretación",
            ["Cada indicador tiene una función de decisión y una anti-interpretación explícita. Esta disciplina debe mantenerse en cualquier extensión del dashboard o del informe."],
            table=(["Indicador", "Grano", "Uso", "Límite"], method_rows),
        )


def validate_dashboard() -> Path:
    target = DASHBOARD / "centro-control-mantenimiento-ferroviario.html"
    if not target.exists():
        raise FileNotFoundError("No se encontró el dashboard standalone existente.")
    content = target.read_text(encoding="utf-8")
    required = ["<!DOCTYPE html", "<style", "<script", "</html>"]
    missing = [token for token in required if token.lower() not in content.lower()]
    if missing or target.stat().st_size < 100_000:
        raise ValueError(f"Dashboard incompleto o no standalone: {missing}")
    return target


def clean_output_structure() -> None:
    for child in OUTPUTS.iterdir():
        if child.name.lower() not in {"graphs", "dashboard", "reports"} or child.name == "Graphs":
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
    GRAPHS.mkdir(parents=True, exist_ok=True)
    DASHBOARD.mkdir(parents=True, exist_ok=True)
    REPORTS.mkdir(parents=True, exist_ok=True)
    for directory in (GRAPHS, REPORTS):
        for path in directory.iterdir():
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()


def main() -> None:
    validate_dashboard()
    clean_output_structure()
    charts = build_charts()
    from build_report_pdf import build as build_narrative_report

    build_narrative_report()
    validate_dashboard()
    print(f"Gráficos: {len(charts)}")
    print(f"Dashboard: {DASHBOARD / 'centro-control-mantenimiento-ferroviario.html'}")
    print(f"Informe: {REPORT}")


if __name__ == "__main__":
    main()
