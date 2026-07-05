"""Construye los artefactos finales de publicación del proyecto.

Salida estricta:
    outputs/graphs/*.png
    outputs/dashboard/centro-control-mantenimiento-ferroviario.html
    outputs/reports/informe_analitico_cbm_ferroviario.pdf
"""
from __future__ import annotations

import shutil
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.ticker import FuncFormatter, MaxNLocator

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


SUBSISTEMA_ES = {
    "bogie": "bogie",
    "brake": "freno",
    "door": "puerta",
    "gearbox": "caja de engranajes",
    "hvac": "climatización",
    "motor": "motor",
    "pantograph": "pantógrafo",
    "wheelset": "juego de ruedas",
}

MODO_FALLA_ES = {
    "aplanamiento_rueda": "aplanamiento de rueda",
    "arco_electrico": "arco eléctrico",
    "bloqueo_actuador": "bloqueo de actuador",
    "contaminacion_lubricante": "contaminación de lubricante",
    "degradacion_pastilla": "degradación de pastilla",
    "desajuste_mecanismo": "desajuste de mecanismo",
    "desalineacion_engranes": "desalineación de engranajes",
    "desbalance_rotor": "desbalance de rotor",
    "desgaste_amortiguador": "desgaste de amortiguador",
    "desgaste_carbon": "desgaste de carbón",
    "desgaste_perfil": "desgaste de perfil",
    "desgaste_rodamiento": "desgaste de rodamiento",
    "fading_freno": "fading de freno",
    "falla_compresor": "falla de compresor",
    "falla_control_puerta": "falla de control de puerta",
    "fatiga_bastidor": "fatiga de bastidor",
    "fatiga_bobinado": "fatiga de bobinado",
    "fatiga_discos": "fatiga de discos",
    "fisura_termica": "fisura térmica",
    "fuga_refrigerante": "fuga de refrigerante",
    "holgura_suspension": "holgura de suspensión",
    "obstruccion_filtro": "obstrucción de filtro",
    "perdida_contacto": "pérdida de contacto",
    "sobrecalentamiento_estator": "sobrecalentamiento de estator",
}


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
    # Ticks forzados a enteros: el locator automático elegía un paso fino (p. ej.
    # 0.5 p.p.) que el formateador sin decimales redondeaba a etiquetas duplicadas.
    ax.yaxis.set_major_locator(MaxNLocator(integer=True))
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
    # componente_id incluido para garantizar etiquetas únicas: varias intervenciones
    # comparten unidad y familia (p. ej. dos bogies de la misma unidad), y barh con
    # etiquetas de texto repetidas colapsa esas filas en una sola barra.
    labels_p = (
        priority["unidad_id"]
        + " · "
        + priority["component_family"].map(family_es).fillna(priority["component_family"])
        + " · "
        + priority["componente_id"]
    )
    y_pos = range(len(priority))
    colors_p = [ACCENT] * len(priority)
    colors_p[-1] = DANGER
    ax.barh(y_pos, priority["intervention_priority_score"], color=colors_p, zorder=3)
    ax.set_yticks(list(y_pos), labels_p)
    for y, value in zip(y_pos, priority["intervention_priority_score"], strict=True):
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
    labels_f = (
        failures["subsistema"].map(SUBSISTEMA_ES).fillna(failures["subsistema"])
        + " · "
        + failures["modo_falla"].map(MODO_FALLA_ES).fillna(failures["modo_falla"].str.replace("_", " "))
    )
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
    # Reindexar explícitamente a [False, True]: si no hay fallos (caso esperado),
    # unstack() no materializa la columna False y la asignación de color por
    # posición desplazaría DANGER a la única columna presente (Aprobados).
    summary = (
        checks.groupby(["severity", "passed"]).size().unstack(fill_value=0).reindex(columns=[False, True], fill_value=0)
    )
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
