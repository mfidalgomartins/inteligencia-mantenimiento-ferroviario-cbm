"""
Genera el pack de gráficos ejecutivos del Sistema de Inteligencia de
Mantenimiento Basado en Condición (CBM) ferroviario.

Diseño sobrio y cohesionado con el dashboard (tipografía IBM Plex,
misma paleta). Cada gráfico responde a una pregunta ejecutiva real y se
guarda como PNG independiente en outputs/Graphs.

Uso:
    .venv/bin/python scripts/make_graphs.py
"""
from __future__ import annotations

from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data" / "processed"
OUT = ROOT / "outputs" / "Graphs"
FONT_DIR = ROOT / "assets" / "fonts" / "ttf"

# --- Paleta (idéntica al dashboard, modo claro) ---------------------------
INK = "#18181b"      # titulos / texto fuerte
MUTED = "#71717a"    # subtitulos / etiquetas
AXIS = "#71717a"     # ejes
GRID = "#ececed"     # rejilla
LINE = "#e4e4e7"     # bordes
ACCENT = "#2f6ae4"   # azul (serie principal / valor neutro)
POSITIVE = "#15803d" # verde (valor preservado / programado)
WARNING = "#b45309"  # ambar (atencion)
DANGER = "#b42318"   # rojo (riesgo / cuello de botella)
NEUTRAL = "#5b6472"  # gris azulado
S2 = "#64748b"
SURFACE = "#ffffff"

# --- Tipografia (IBM Plex, cohesionada con el dashboard) -------------------
SANS = "DejaVu Sans"
MONO = "DejaVu Sans Mono"
if FONT_DIR.exists():
    for ttf in FONT_DIR.glob("*.ttf"):
        fm.fontManager.addfont(str(ttf))
    names = {f.name for f in fm.fontManager.ttflist}
    if "IBM Plex Sans" in names:
        SANS = "IBM Plex Sans"
    if "IBM Plex Mono" in names:
        MONO = "IBM Plex Mono"

plt.rcParams.update({
    "figure.facecolor": SURFACE,
    "axes.facecolor": SURFACE,
    "savefig.facecolor": SURFACE,
    "font.family": SANS,
    "text.color": INK,
    "axes.edgecolor": LINE,
    "axes.labelcolor": MUTED,
    "xtick.color": AXIS,
    "ytick.color": AXIS,
    "axes.linewidth": 1.0,
    "axes.grid": False,
    "figure.dpi": 200,
})

OUT.mkdir(parents=True, exist_ok=True)


def _title(ax, title: str, subtitle: str = ""):
    # pad / offset en puntos -> separacion consistente en todos los graficos
    ax.set_title(title, loc="left", fontsize=15, fontweight="bold",
                 color=INK, pad=32 if subtitle else 14)
    if subtitle:
        ax.annotate(subtitle, xy=(0, 1), xycoords="axes fraction",
                    xytext=(0, 9), textcoords="offset points",
                    fontsize=10.5, color=MUTED, ha="left", va="bottom")


def _footer(fig, text: str):
    fig.text(0.013, 0.012, text, fontsize=8.2, color=MUTED, ha="left")


def _clean(ax, left_spine=True):
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    ax.spines["left"].set_visible(left_spine)
    ax.spines["bottom"].set_color(LINE)
    if left_spine:
        ax.spines["left"].set_color(LINE)
    ax.tick_params(length=0, labelsize=10.5)
    for lab in ax.get_xticklabels() + ax.get_yticklabels():
        lab.set_fontfamily(MONO)


def _save(fig, name: str):
    path = OUT / name
    fig.savefig(path, bbox_inches="tight", pad_inches=0.32)
    plt.close(fig)
    print(f"  -> {path.relative_to(ROOT)}")


STRAT_LABEL = {
    "basada_en_condicion": "CBM",
    "preventiva_rigida": "Preventiva",
    "reactiva": "Reactiva",
}


# 1) Valor operativo neto vs estrategia reactiva ----------------------------
def chart_strategy_value():
    d = pd.read_csv(DATA / "comparativo_estrategias.csv")
    d = d.set_index("estrategia")
    order = ["preventiva_rigida", "basada_en_condicion", "reactiva"]
    labels = [STRAT_LABEL[s] for s in order]
    net = d.loc[order, "ahorro_neto_vs_reactiva"] / 1e6
    p10 = d.loc[order, "ahorro_neto_p10_vs_reactiva"] / 1e6
    p90 = d.loc[order, "ahorro_neto_p90_vs_reactiva"] / 1e6
    prob = d.loc[order, "prob_ahorro_positivo"]

    colors = [POSITIVE, ACCENT, NEUTRAL]
    fig, ax = plt.subplots(figsize=(8.6, 5.4))
    x = np.arange(len(order))
    bars = ax.bar(x, net.values, width=0.56, color=colors, zorder=3)

    # Bandas P10-P90 (solo donde aplica)
    err_lo = (net - p10).clip(lower=0).values
    err_hi = (p90 - net).clip(lower=0).values
    mask = net.values > 0
    ax.errorbar(x[mask], net.values[mask],
                yerr=[err_lo[mask], err_hi[mask]],
                fmt="none", ecolor=INK, elinewidth=1.4, capsize=5,
                capthick=1.4, zorder=4, alpha=0.65)

    ax.axhline(0, color=AXIS, lw=1.1, zorder=2)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=12, color=INK, fontfamily=SANS)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:.0f}"))
    ax.set_ylabel("Valor neto preservado  (M€)")
    ymax = max(p90.max() * 1.22, 10)
    ax.set_ylim(-6, ymax)
    ax.grid(axis="y", color=GRID, lw=1, zorder=0)

    gap = ymax * 0.018
    for i, s in enumerate(order):
        v = net.values[i]
        if v <= 0:
            ax.text(x[i], gap, "referencia", ha="center", va="bottom",
                    fontsize=10, color=MUTED, fontfamily=SANS)
            continue
        top = net.values[i] + err_hi[i]
        ax.text(x[i], top + gap, f"P(ahorro>0): {prob.values[i]*100:.0f}%",
                ha="center", va="bottom", fontsize=9.5, color=MUTED,
                fontfamily=MONO)
        ax.text(x[i], top + gap * 3.6, f"{v:,.1f} M€".replace(",", "."),
                ha="center", va="bottom", fontsize=14, fontweight="bold",
                color=INK, fontfamily=MONO)

    _clean(ax)
    _title(ax, "El CBM y el plan preventivo preservan valor frente a operar en reactivo",
           "Valor operativo neto esperado vs estrategia reactiva · banda P10–P90 (Monte Carlo)")
    _footer(fig, "Fuente: comparativo_estrategias.csv · Sistema de Inteligencia de Mantenimiento CBM ferroviario")
    _save(fig, "01_valor_estrategia.png")


# 2) Coste de aplazar (trade-off) -------------------------------------------
def chart_deferral_tradeoff():
    d = pd.read_csv(DATA / "impacto_diferimiento_resumen.csv").sort_values("defer_dias")
    x = d["defer_dias"].values
    cost = d["costo_total_eur"].values / 1e6
    down = d["downtime_total_h"].values
    # Indexado a dia 0 = 100 -> revela que la indisponibilidad escala mas rapido
    cost_idx = cost / cost[0] * 100
    down_idx = down / down[0] * 100

    fig, ax = plt.subplots(figsize=(8.6, 5.4))
    l1, = ax.plot(x, cost_idx, color=ACCENT, lw=2.6, marker="o", ms=6,
                  zorder=4, label="Coste total esperado")
    l2, = ax.plot(x, down_idx, color=WARNING, lw=2.6, marker="s", ms=5.5,
                  zorder=4, label="Horas de indisponibilidad")
    ax.axhline(100, color=AXIS, lw=1.0, ls=(0, (4, 3)), zorder=2)

    ax.set_xticks(x)
    ax.set_xlim(-1.5, x[-1] + 7)
    ax.set_xlabel("Días de diferimiento de la intervención")
    ax.set_ylabel("Índice de impacto  (día 0 = 100)")
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:.0f}"))
    ax.grid(axis="y", color=GRID, lw=1, zorder=0)

    ax.annotate(f"+{cost_idx[-1]-100:.0f}%\n{cost[-1]:.2f} M€",
                xy=(x[-1], cost_idx[-1]), xytext=(x[-1] + 1.2, cost_idx[-1]),
                ha="left", va="center", fontsize=10.5, color=ACCENT,
                fontfamily=MONO, fontweight="bold")
    ax.annotate(f"+{down_idx[-1]-100:.0f}%\n{down[-1]:,.0f} h".replace(",", "."),
                xy=(x[-1], down_idx[-1]), xytext=(x[-1] + 1.2, down_idx[-1]),
                ha="left", va="center", fontsize=10.5, color=WARNING,
                fontfamily=MONO, fontweight="bold")

    _clean(ax)
    ax.legend(handles=[l1, l2], loc="upper left", frameon=False,
              fontsize=10.5, prop={"family": SANS})
    _title(ax, "Diferir degrada la disponibilidad más rápido que el coste",
           "Impacto de aplazar la intervención, indexado al día 0 · base: 1,28 M€ y 982 h")
    _footer(fig, "Fuente: impacto_diferimiento_resumen.csv · Sistema de Inteligencia de Mantenimiento CBM ferroviario")
    _save(fig, "02_coste_diferimiento.png")


# 3) Cola de entrada a taller (top unidades por riesgo) ---------------------
def chart_workshop_queue():
    d = pd.read_csv(DATA / "workshop_priority_table.csv")
    d = d.sort_values("intervention_priority_score", ascending=False).head(12).iloc[::-1]
    score = d["intervention_priority_score"].values
    fam_es = {"pantograph": "Pantógrafo", "bogie": "Bogie", "brake": "Freno",
              "door": "Puerta", "hvac": "Climatización", "traction": "Tracción"}
    family = [fam_es.get(str(f), str(f).capitalize()) for f in d["component_family"]]
    labels = [f"{u} · {c}" for u, c in zip(d["unidad_id"], d["componente_id"])]

    colors = [ACCENT] * len(d)
    colors[-1] = DANGER  # la #1 (arriba) destacada

    floor = 80.0  # eje desde 80 para distinguir la cabeza de cola (indice acotado 0-100)
    fig, ax = plt.subplots(figsize=(8.8, 5.9))
    y = np.arange(len(d))
    ax.barh(y, score - floor, left=floor, color=colors, height=0.66, zorder=3)
    ax.set_yticks(y)
    ax.set_yticklabels(labels, fontsize=10.5, fontfamily=MONO, color=INK)
    ax.set_xlabel("Índice de prioridad de intervención  (eje desde 80)")
    ax.set_xlim(floor, score.max() + (score.max() - floor) * 0.42)
    ax.grid(axis="x", color=GRID, lw=1, zorder=0)

    for i in range(len(d)):
        ax.text(score[i] + 0.18, y[i], f"{score[i]:.1f}",
                va="center", ha="left", fontsize=10.5, fontfamily=MONO,
                color=INK, fontweight="bold")
        ax.text(floor + 0.25, y[i], family[i],
                va="center", ha="left", fontsize=9, fontfamily=SANS,
                color="#ffffff")

    _clean(ax)
    _title(ax, "Cola de entrada a taller: qué sustituir primero",
           "Top 12 componentes por índice de prioridad (snapshot 2025-12-31) · en rojo, la primera intervención")
    _footer(fig, "Fuente: workshop_priority_table.csv · Sistema de Inteligencia de Mantenimiento CBM ferroviario")
    _save(fig, "03_cola_taller_riesgo.png")


# 4) Saturacion media por deposito ------------------------------------------
def chart_depot_saturation():
    d = pd.read_csv(DATA / "vw_depot_maintenance_pressure.csv")
    g = (d.groupby("nombre_deposito")["saturation_ratio"].mean()
         .sort_values(ascending=True))
    sat = g.values
    names = g.index.values

    def col(v):
        if v >= 1.0:
            return DANGER
        if v >= 0.8:
            return WARNING
        return ACCENT
    colors = [col(v) for v in sat]

    fig, ax = plt.subplots(figsize=(8.6, 5.6))
    y = np.arange(len(g))
    ax.barh(y, sat, color=colors, height=0.64, zorder=3)
    ax.axvline(1.0, color=INK, lw=1.3, ls=(0, (4, 3)), zorder=4)
    ax.text(1.0, len(g) - 0.35, "Capacidad", color=INK, fontsize=9.5,
            ha="center", va="bottom", fontfamily=SANS, fontweight="bold")

    ax.set_yticks(y)
    ax.set_yticklabels(names, fontsize=10.5, fontfamily=SANS, color=INK)
    ax.set_xlabel("Saturación media del taller  (1.0 = capacidad)")
    ax.set_xlim(0, max(sat) * 1.16)
    ax.grid(axis="x", color=GRID, lw=1, zorder=0)
    for i in range(len(g)):
        ax.text(sat[i] + max(sat) * 0.012, y[i], f"{sat[i]:.2f}",
                va="center", ha="left", fontsize=10.5, fontfamily=MONO,
                color=INK, fontweight="bold")

    _clean(ax)
    _title(ax, "Dos depósitos operan por encima de su capacidad",
           "Saturación media de taller por depósito · candidatos a rebalanceo de carga")
    _footer(fig, "Fuente: vw_depot_maintenance_pressure.csv · Sistema de Inteligencia de Mantenimiento CBM ferroviario")
    _save(fig, "04_saturacion_depositos.png")


# 5) Distribucion del riesgo de la flota ------------------------------------
def chart_fleet_risk_distribution():
    d = pd.read_csv(DATA / "unit_unavailability_risk_score.csv")
    v = d["unit_unavailability_risk_score"].values
    mean = v.mean()
    thr = mean + 1.5 * v.std()  # umbral oficial de alto riesgo (outlier >1.5 sigma)
    n_high = int((v >= thr).sum())

    fig, ax = plt.subplots(figsize=(8.6, 5.2))
    bins = np.linspace(v.min(), v.max(), 22)
    n, edges, patches = ax.hist(v, bins=bins, color=ACCENT, zorder=3,
                                edgecolor=SURFACE, linewidth=1.0)
    for patch, left in zip(patches, edges[:-1]):
        if left >= thr:
            patch.set_facecolor(DANGER)

    ymax = n.max() * 1.2
    ax.axvline(mean, color=AXIS, lw=1.2, ls=(0, (4, 3)), zorder=5)
    ax.text(mean, ymax * 0.99, f"Media {mean:.0f}", color=AXIS,
            fontsize=9.5, ha="right", va="top", fontfamily=MONO)
    ax.axvline(thr, color=DANGER, lw=1.6, ls=(0, (4, 3)), zorder=5)
    ax.text(thr + 0.4, ymax * 0.99, f"Umbral alto riesgo  ·  media + 1,5σ = {thr:.0f}",
            color=DANGER, fontsize=9.8, ha="left", va="top",
            fontfamily=SANS, fontweight="bold")

    ax.set_xlabel("Score de riesgo de indisponibilidad por unidad")
    ax.set_ylabel("Nº de unidades")
    ax.grid(axis="y", color=GRID, lw=1, zorder=0)
    ax.set_ylim(0, ymax)
    ax.text(0.985, 0.6, f"{n_high} unidades\nde alto riesgo",
            transform=ax.transAxes, ha="right", va="top", fontsize=12,
            color=DANGER, fontfamily=SANS, fontweight="bold")

    _clean(ax)
    _title(ax, "El riesgo se concentra en una cola estrecha de la flota",
           f"Distribución del score de riesgo de las {len(v)} unidades · 11 unidades superan el umbral de alto riesgo")
    _footer(fig, "Fuente: unit_unavailability_risk_score.csv · Sistema de Inteligencia de Mantenimiento CBM ferroviario")
    _save(fig, "05_distribucion_riesgo_flota.png")


# 6) Impacto de la reprogramacion heuristica --------------------------------
def chart_scheduling_before_after():
    d = pd.read_csv(DATA / "scheduling_status_distribution.csv")
    scen_label = {
        "baseline_greedy_21d": "Baseline\n(greedy, 21d)",
        "heuristica_redisenada_35d": "Heurística\nrediseñada (35d)",
    }
    state_label = {
        "programada": "Programada",
        "programable_proxima_ventana": "Programable (próx. ventana)",
        "pendiente_repuesto": "Pendiente de repuesto",
        "pendiente_capacidad": "Pendiente por capacidad",
    }
    state_color = {
        "programada": POSITIVE,
        "programable_proxima_ventana": ACCENT,
        "pendiente_repuesto": WARNING,
        "pendiente_capacidad": DANGER,
    }
    order_states = ["programada", "programable_proxima_ventana",
                    "pendiente_repuesto", "pendiente_capacidad"]
    scen_order = ["baseline_greedy_21d", "heuristica_redisenada_35d"]

    piv = (d.pivot_table(index="scenario", columns="estado_intervencion",
                         values="share_pct", aggfunc="sum")
           .reindex(scen_order).fillna(0))

    fig, ax = plt.subplots(figsize=(8.6, 4.6))
    y = np.arange(len(scen_order))
    left = np.zeros(len(scen_order))
    for st in order_states:
        if st not in piv.columns:
            continue
        vals = piv[st].values
        ax.barh(y, vals, left=left, color=state_color[st], height=0.5,
                zorder=3, label=state_label[st])
        for i, val in enumerate(vals):
            if val >= 6:
                ax.text(left[i] + val / 2, y[i], f"{val:.0f}%",
                        ha="center", va="center", fontsize=10,
                        color="#ffffff", fontfamily=MONO, fontweight="bold")
        left += vals

    ax.set_yticks(y)
    ax.set_yticklabels([scen_label[s] for s in scen_order], fontsize=11,
                       fontfamily=SANS, color=INK)
    ax.set_xlim(0, 100)
    ax.set_xlabel("Reparto de intervenciones por estado  (%)")
    ax.grid(axis="x", color=GRID, lw=1, zorder=0)
    ax.invert_yaxis()
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.18), ncol=2,
              frameon=False, fontsize=9.8, prop={"family": SANS})

    _clean(ax, left_spine=False)
    _title(ax, "La reprogramación reduce el bloqueo por capacidad a la mitad",
           "Reparto de intervenciones por estado: ventana de planificación greedy vs heurística rediseñada")
    _footer(fig, "Fuente: scheduling_status_distribution.csv · Sistema de Inteligencia de Mantenimiento CBM ferroviario")
    _save(fig, "06_scheduling_before_after.png")


def main():
    print("Generando pack de gráficos en outputs/Graphs ...")
    print(f"Tipografía: {SANS} / {MONO}")
    chart_strategy_value()
    chart_deferral_tradeoff()
    chart_workshop_queue()
    chart_depot_saturation()
    chart_fleet_risk_distribution()
    chart_scheduling_before_after()
    print("Listo: 6 gráficos.")


if __name__ == "__main__":
    main()
