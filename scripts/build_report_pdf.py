"""Genera el informe analítico final en prosa continua vía WeasyPrint.

Salida única:
    outputs/reports/informe_analitico_cbm_ferroviario.pdf

El informe combina texto ejecutivo, evidencia cuantificada y los 19 gráficos del
paquete de publicación incrustados a lo largo del documento. Todas las cifras se calculan en tiempo
de construcción desde data/processed para garantizar trazabilidad y reproducibilidad.
"""
from __future__ import annotations

import base64
from html import escape
from pathlib import Path

import pandas as pd
from weasyprint import HTML

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data" / "processed"
GRAPHS = ROOT / "outputs" / "graphs"
REPORT = ROOT / "outputs" / "reports" / "informe_analitico_cbm_ferroviario.pdf"
FONT_DIR = ROOT / "assets" / "fonts" / "ttf"

FAM_ES = {"pantograph": "pantógrafo", "bogie": "bogie", "brake": "freno", "wheel": "rueda"}
FAM_ES_CAP = {k: v.capitalize() for k, v in FAM_ES.items()}


# --------------------------------------------------------------------------- #
# Lectura y formato                                                           #
# --------------------------------------------------------------------------- #
def read(name: str) -> pd.DataFrame:
    return pd.read_csv(DATA / name)


def fmt_int(value: float) -> str:
    return f"{int(round(float(value))):,}".replace(",", ".")


def fmt_dec(value: float, decimals: int = 1) -> str:
    return f"{float(value):,.{decimals}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def fmt_money_m(value: float, decimals: int = 1) -> str:
    return f"{fmt_dec(float(value) / 1_000_000, decimals)} M€"


def fmt_pct(value: float, decimals: int = 1) -> str:
    return f"{fmt_dec(value, decimals)}%"


def b64_font(path: Path) -> str:
    return base64.b64encode(path.read_bytes()).decode("ascii")


def b64_img(path: Path) -> str:
    return base64.b64encode(path.read_bytes()).decode("ascii")


# --------------------------------------------------------------------------- #
# Bloques HTML                                                                #
# --------------------------------------------------------------------------- #
class Doc:
    def __init__(self) -> None:
        self.parts: list[str] = []

    def add(self, html: str) -> None:
        self.parts.append(html)

    def h(self, level: int, text: str, *, anchor: str | None = None, section: str | None = None) -> None:
        idattr = f' id="{anchor}"' if anchor else ""
        datasec = f' data-section="{escape(section)}"' if section else ""
        self.parts.append(f'<h{level}{idattr}{datasec}>{escape(text)}</h{level}>')

    def p(self, text: str) -> None:
        self.parts.append(f"<p>{text}</p>")

    def lead(self, text: str) -> None:
        self.parts.append(f'<p class="lead">{text}</p>')

    def callout(self, label: str, text: str, tone: str = "accent") -> None:
        self.parts.append(
            f'<div class="callout {tone}"><span class="callout-label">{escape(label)}</span>'
            f'<span class="callout-text">{text}</span></div>'
        )

    def figure(self, filename: str, caption: str, source: str) -> None:
        data = b64_img(GRAPHS / filename)
        self.parts.append(
            f'<figure><img src="data:image/png;base64,{data}" alt="{escape(caption)}"/>'
            f'<figcaption><strong>Figura.</strong> {caption} '
            f'<span class="src">Fuente: {escape(source)}.</span></figcaption></figure>'
        )

    def html(self) -> str:
        return "\n".join(self.parts)


# --------------------------------------------------------------------------- #
# Construcción                                                                #
# --------------------------------------------------------------------------- #
def build() -> None:
    # ----- Datos --------------------------------------------------------- #
    mr = read("narrative_metrics_official.csv")
    m = dict(zip(mr["metric_id"], mr["metric_value"], strict=True))

    def mv(key: str) -> float:
        return float(m[key])

    strategy = read("comparativo_estrategias.csv").set_index("estrategia")
    cbm = strategy.loc["basada_en_condicion"]
    reactive = strategy.loc["reactiva"]
    preventive = strategy.loc["preventiva_rigida"]
    sched = read("scheduling_before_after_metrics.csv").set_index("scenario")
    baseline = sched.loc["base_inicial_voraz_21d"]
    redesigned = sched.loc["heuristica_redisenada_35d"]
    bottlenecks = read("scheduling_bottleneck_diagnosis.csv").sort_values("pending_rate_pct", ascending=False)
    sensitivity = read("comparativo_estrategias_sensibilidad.csv")
    contracts = read("metric_contract_registry.csv")
    inspection_value = read("inspection_module_value_comparison.csv").set_index("scenario")

    defer = read("impacto_diferimiento_resumen.csv").set_index("defer_dias")
    families = read("risk_segmentation_component_family.csv").set_index("component_family")
    rul_fam = read("rul_family_discrimination_before_after.csv").set_index("component_family")
    rul_dist = read("rul_distribution_before_after.csv").set_index("metodo")
    inspection = read("inspection_module_family_performance.csv").set_index("family")
    determinants = read("risk_signal_determinants.csv").set_index("feature")[
        "spearman_corr_with_failure_risk"
    ]
    checks = read("governance_contract_checks.csv")
    priority = read("workshop_priority_table.csv").sort_values(
        "intervention_priority_score", ascending=False
    )
    top = priority.iloc[0]
    top10 = priority.head(10)
    priority_threshold_85 = int((priority["intervention_priority_score"] >= 85).sum())
    priority_threshold_80 = int((priority["intervention_priority_score"] >= 80).sum())
    priority_threshold_70 = int((priority["intervention_priority_score"] >= 70).sum())
    top10_deferral_avg = float(top10["deferral_risk_score"].mean())
    portfolio_deferral_avg = float(priority["deferral_risk_score"].mean())
    top10_priority_avg = float(top10["intervention_priority_score"].mean())
    top10_service_avg = float(top10["service_impact_score"].mean())
    top10_units_count = int(top10["unidad_id"].nunique())
    top10_pantograph_count = int((top10["component_family"] == "pantograph").sum())
    top10_bogie_count = int((top10["component_family"] == "bogie").sum())
    top3_priority_spread = float(priority.head(3)["intervention_priority_score"].max() - priority.head(3)["intervention_priority_score"].min())

    risk = read("unit_unavailability_risk_score.csv")["unit_unavailability_risk_score"]
    threshold = risk.mean() + 1.5 * risk.std()
    high_risk = int((risk >= threshold).sum())

    backlog = read("kpi_backlog_mas_critico.csv")
    top3_backlog_share = (
        backlog.nlargest(3, "backlog_fisico_total")["backlog_fisico_total"].sum()
        / backlog["backlog_fisico_total"].sum()
        * 100
    )
    n_depots_backlog = len(backlog)

    failures = read("kpi_fallas_repetitivas_mas_frecuentes.csv")
    top5_fail_share = (
        failures.nlargest(5, "repetitive_events")["repetitive_events"].sum()
        / failures["repetitive_events"].sum()
        * 100
    )

    capacity = read("workshop_capacity_calendar.csv")
    avg_capacity_use = capacity["total_used_h"].sum() / capacity["total_capacity_h"].sum() * 100

    insp_min = inspection["pre_failure_detection_rate"].min() * 100
    insp_max = inspection["pre_failure_detection_rate"].max() * 100
    insp_min_fam = FAM_ES[inspection["pre_failure_detection_rate"].idxmin()]
    insp_max_fam = FAM_ES[inspection["pre_failure_detection_rate"].idxmax()]
    insp_lead = inspection["lead_time_medio_dias"].mean()

    cbm_av = float(cbm["fleet_availability"])
    re_av = float(reactive["fleet_availability"])
    av_gap = cbm_av - re_av

    cbm_cost = float(cbm["coste_total_esperado"])
    re_cost = float(reactive["coste_total_esperado"])
    pv_cost = float(preventive["coste_total_esperado"])
    cbm_sens = sensitivity[sensitivity["estrategia"] == "basada_en_condicion"]
    preventive_sens = sensitivity[sensitivity["estrategia"] == "preventiva_rigida"]
    cbm_positive_share = float((cbm_sens["ahorro_neto_vs_reactiva"] > 0).mean() * 100)
    preventive_positive_share = float((preventive_sens["ahorro_neto_vs_reactiva"] > 0).mean() * 100)
    cbm_sens_min = float(cbm_sens["ahorro_neto_vs_reactiva"].min())
    cbm_sens_max = float(cbm_sens["ahorro_neto_vs_reactiva"].max())
    preventive_sens_min = float(preventive_sens["ahorro_neto_vs_reactiva"].min())
    preventive_sens_max = float(preventive_sens["ahorro_neto_vs_reactiva"].max())
    actionable_delta_pp = float(redesigned["actionable_pct"] - baseline["actionable_pct"])
    residual_risk_delta_pp = float(
        baseline["riesgo_residual_no_atendido_pct"] - redesigned["riesgo_residual_no_atendido_pct"]
    )
    captured_value_delta = float(redesigned["valor_capturado_proxy"] - baseline["valor_capturado_proxy"])
    capacity_use_delta_pp = float(redesigned["capacidad_utilizada_pct"] - baseline["capacidad_utilizada_pct"])
    workshop_hours_delta = float(redesigned["horas_taller_usadas"] - baseline["horas_taller_usadas"])
    primary_bottleneck = bottlenecks.sort_values("pendientes_capacidad", ascending=False).iloc[0]
    primary_bottleneck_case_share = float(primary_bottleneck["casos"] / bottlenecks["casos"].sum() * 100)
    primary_bottleneck_pending_share = float(
        primary_bottleneck["pendientes_capacidad"] / bottlenecks["pendientes_capacidad"].sum() * 100
    )
    cbm_vs_preventive_av_gap = cbm_av - float(preventive["fleet_availability"])
    cbm_vs_preventive_cost_gap = cbm_cost - pv_cost
    cbm_cost_per_availability_pp = (cbm_cost - re_cost) / av_gap
    inspection_correctives_delta = (
        float(inspection_value.loc["sin_inspeccion_automatica", "correctivas_estimadas"])
        - float(inspection_value.loc["con_inspeccion_automatica", "correctivas_estimadas"])
    )
    inspection_downtime_delta = (
        float(inspection_value.loc["sin_inspeccion_automatica", "horas_indisponibilidad_estimadas"])
        - float(inspection_value.loc["con_inspeccion_automatica", "horas_indisponibilidad_estimadas"])
    )

    d = Doc()

    # ----- Portada ------------------------------------------------------- #
    d.add(
        f"""
<section class="cover">
  <div class="cover-band">
    <div class="kicker">Informe analítico</div>
    <h1 class="cover-title">Inteligencia de mantenimiento ferroviario</h1>
    <p class="cover-sub">Riesgo, priorización de taller, vida remanente y disciplina económica
    del mantenimiento basado en condición</p>
  </div>
  <div class="cover-meta">
    <div class="cover-meta-row"><span>Periodo analizado</span><strong>{m['coverage_start']} a {m['coverage_end']}</strong></div>
    <div class="cover-meta-row"><span>Alcance</span><strong>{fmt_int(mv('n_unidades'))} unidades · {fmt_int(mv('n_componentes'))} componentes críticos</strong></div>
    <div class="cover-meta-row"><span>Red de talleres</span><strong>{fmt_int(mv('n_depositos'))} depósitos · {fmt_int(mv('n_flotas'))} flotas</strong></div>
    <div class="cover-meta-row"><span>Controles de gobernanza</span><strong>{len(checks)} activos · 0 bloqueos de publicación</strong></div>
  </div>
  <div class="cover-decision">
    <p>La decisión inmediata no es desplegar más analítica: es ejecutar una cola de taller gobernada
    por riesgo, corregir la asignación de capacidad y tratar CBM como una opción de nivel de servicio
    hasta que el caso financiero quede calibrado con costes reales.</p>
  </div>
</section>
"""
    )

    # ----- Índice -------------------------------------------------------- #
    toc_rows = [
        ("Resumen ejecutivo", "sec-resumen"),
        ("1. Contexto y objetivos", "sec-contexto"),
        ("2. Datos y metodología", "sec-datos"),
        ("3. Marco analítico", "sec-marco"),
        ("4. Diagnóstico operativo", "sec-diagnostico"),
        ("5. Fiabilidad, inspección y vida remanente", "sec-fiabilidad"),
        ("6. Priorización y capacidad de taller", "sec-priorizacion"),
        ("7. Economía y decisión estratégica", "sec-economia"),
        ("8. Riesgos, limitaciones y cautelas", "sec-riesgos"),
        ("9. Recomendaciones y prioridades de acción", "sec-recomendaciones"),
        ("Apéndice. Métricas, fuentes e interpretación", "sec-apendice"),
    ]
    toc_html = "".join(
        f'<li><a href="#{anchor}"><span class="toc-title">{escape(title)}</span>'
        f'<span class="toc-dots"></span></a></li>'
        for title, anchor in toc_rows
    )
    d.add(f'<section class="toc"><h2 class="toc-head">Índice</h2><ol class="toc-list">{toc_html}</ol></section>')

    # ----- Resumen ejecutivo -------------------------------------------- #
    d.add('<section class="body">')
    d.h(2, "Resumen ejecutivo", anchor="sec-resumen", section="Resumen ejecutivo")
    d.lead(
        "El sistema no produce una lista de indicadores: ordena una decisión operativa concreta. "
        "La pregunta de gestión es qué unidades y componentes entran primero en taller, cuánto cuesta "
        "aplazar cada intervención y dónde la capacidad está mal asignada. Este informe responde a esas "
        "preguntas con evidencia cuantificada y separa con claridad el valor operativo de la condición "
        "técnica del coste económico, que bajo los supuestos actuales no respalda una afirmación de ahorro."
    )

    d.add(
        f"""
<div class="kpi-row">
  <div class="kpi"><div class="kpi-val">{fmt_pct(mv('fleet_availability_pct'), 1)}</div><div class="kpi-lab">Disponibilidad media de flota</div></div>
  <div class="kpi danger"><div class="kpi-val">{fmt_int(mv('high_deferral_risk_cases_count'))}</div><div class="kpi-lab">Casos con alto riesgo de diferimiento</div></div>
  <div class="kpi positive"><div class="kpi-val">+{fmt_dec(float(redesigned['actionable_pct']) - float(baseline['actionable_pct']), 1)} p.p.</div><div class="kpi-lab">Mejora de casos ejecutables tras rediseño</div></div>
  <div class="kpi warning"><div class="kpi-val">{fmt_money_m(mv('cbm_operational_savings_eur'))}</div><div class="kpi-lab">Diferencial neto de CBM frente a reactiva</div></div>
</div>
"""
    )

    decision_rows_exec = [
        (
            "Ejecutar cola de riesgo",
            f"{priority_threshold_85} casos superan 85 puntos; los 10 primeros concentran "
            f"{fmt_dec(top10_priority_avg, 1)} puntos de prioridad media y "
            f"{fmt_dec(top10_deferral_avg, 1)} de riesgo medio de diferimiento, con impacto de servicio medio "
            f"{fmt_dec(top10_service_avg, 1)}.",
            "Aprobar ejecución P1/P2 con excepción documentada y revisión diaria de repuesto, ventana y capacidad.",
        ),
        (
            "Adoptar programación a 35 días",
            f"Accionabilidad +{fmt_dec(actionable_delta_pp, 1)} p.p.; riesgo residual no atendido "
            f"-{fmt_dec(residual_risk_delta_pp, 1)} p.p.; valor capturado +{fmt_money_m(captured_value_delta, 2)}.",
            "Usar la heurística rediseñada como base inicial operativa y medir restricciones residuales semanalmente.",
        ),
        (
            "Rebalancear talleres",
            f"{primary_bottleneck['deposito_id']} concentra el {fmt_pct(primary_bottleneck_case_share, 1)} "
            f"de los casos diagnosticados y el {fmt_pct(primary_bottleneck_pending_share, 1)} "
            "de los pendientes por capacidad.",
            "Transferir trabajo compatible antes de pedir capacidad adicional estructural.",
        ),
        (
            "Condicionar inversión CBM",
            f"+{fmt_dec(av_gap, 2)} p.p. de disponibilidad frente a reactiva, pero "
            f"{fmt_money_m(mv('cbm_operational_savings_eur'))} de diferencial neto y "
            f"{fmt_pct(mv('cbm_prob_positive_savings') * 100, 0)} de escenarios con ahorro positivo.",
            f"Exigir puerta financiera: costes reales y valor corporativo >= € {fmt_int(mv('cbm_breakeven_value_per_service_hour_eur'))}/h.",
        ),
    ]
    decision_exec_html = "".join(
        f"<tr><td><strong>{escape(decision)}</strong></td><td>{evidence}</td><td>{action}</td></tr>"
        for decision, evidence, action in decision_rows_exec
    )
    d.h(3, "Agenda de comité")
    d.add(
        "<table class='data'><thead><tr><th>Decisión</th><th>Evidencia crítica</th>"
        "<th>Mandato recomendado</th></tr></thead><tbody>"
        f"{decision_exec_html}</tbody></table>"
        "<p class='tbl-note'>Lectura ejecutiva: el informe separa decisiones operativas ya accionables de decisiones "
        "de inversión que todavía requieren calibración financiera.</p>"
    )

    d.p(
        f"La flota mantiene una disponibilidad media del {fmt_pct(mv('fleet_availability_pct'), 2)} "
        f"durante el periodo {m['coverage_start']} a {m['coverage_end']}. Ese nivel agregado es tranquilizador "
        f"y, por sí solo, engañoso. Detrás de él se acumulan {fmt_int(mv('backlog_physical_items_count'))} pendientes "
        f"físicos, de los cuales {fmt_int(mv('backlog_overdue_items_count'))} están vencidos y "
        f"{fmt_int(mv('backlog_critical_physical_count'))} cumplen la definición de pendientes críticos físicos. La "
        "disponibilidad describe lo que ya ocurrió; no mide la exposición que se está acumulando. La gestión "
        "necesita una segunda capa que ordene la cola por riesgo y no por volumen, porque tratar todos los "
        "pendientes con la misma urgencia consume capacidad sin reducir la probabilidad de fallo donde más importa."
    )
    d.p(
        f"El sistema identifica {high_risk} unidades en la cola estadística de alto riesgo y "
        f"{fmt_int(mv('high_deferral_risk_cases_count'))} intervenciones con riesgo de diferimiento igual o superior "
        f"a 70 puntos. La primera decisión está nominada sin ambigüedad: intervenir {top['unidad_id']} / "
        f"{top['componente_id']}, familia {FAM_ES[top['component_family']]}, con una puntuación de prioridad de "
        f"{fmt_dec(top['intervention_priority_score'], 1)} y un riesgo de diferimiento de "
        f"{fmt_dec(top['deferral_risk_score'], 1)}. Su probabilidad estimada de fallo a 30 días es "
        f"{fmt_pct(top['prob_fallo_30d'] * 100, 1)} y su vida remanente aproximada es de {fmt_int(top['component_rul_estimate'])} "
        "días. La cabeza de la cola presenta puntuaciones muy próximas entre sí, de modo que cualquier excepción en la "
        "secuencia desplaza protección de servicio real y debe documentarse."
    )
    d.callout(
        "Conclusión ejecutiva",
        "La prioridad inmediata es proteger servicio con una cola de intervención basada en riesgo, mientras "
        "se corrige la asignación de capacidad entre depósitos. El caso de inversión en CBM no se presenta como "
        "ahorro: se fija el valor corporativo de la disponibilidad adicional y se recalibran costes antes de comprometer capital.",
        "accent",
    )
    d.h(3, "Lectura de comité: qué está decidido y qué no")
    d.p(
        "El comité tiene dos decisiones de naturaleza distinta. La primera es operativa y está lista: proteger la "
        "cabeza de la cola, gobernar los casos de alto riesgo de diferimiento y mover trabajo hacia los depósitos con "
        "capacidad compatible. La evidencia converge en la misma dirección: riesgo técnico, impacto de servicio, ventana "
        "de intervención, restricción de taller y coste de aplazar. La segunda decisión es de capital y aún no está "
        "lista. CBM mejora disponibilidad, pero el modelo no muestra ahorro neto bajo los supuestos actuales. Mezclar "
        "ambas decisiones deterioraría la credibilidad del caso: la organización debe actuar ya sobre la cola de riesgo "
        "y reservar la inversión CBM para una puerta financiera posterior."
    )
    d.p(
        f"La cola priorizada es lo bastante estrecha para gobernarse con disciplina ejecutiva. Solo {priority_threshold_85} "
        f"casos superan 85 puntos, {priority_threshold_80} superan 80 y {priority_threshold_70} superan 70 sobre "
        f"{fmt_int(len(priority))} componentes evaluados. Los diez primeros se concentran en {top10_units_count} unidades, "
        f"con {top10_bogie_count} bogies y {top10_pantograph_count} pantógrafos; su riesgo medio de diferimiento es "
        f"{fmt_dec(top10_deferral_avg, 1)}, frente a {fmt_dec(portfolio_deferral_avg, 1)} en la cartera completa. "
        "La implicación es clara: no hace falta sobregestionar toda la base de activos. Hace falta blindar una franja "
        "pequeña donde cada aplazamiento destruye protección de servicio."
    )
    d.h(3, "Cinco decisiones concentran el valor de gestión")
    d.p(
        f"Primero, ejecutar la cabeza de cola y proteger los {fmt_int(mv('high_deferral_risk_cases_count'))} casos de "
        "alto riesgo de diferimiento con revisión diaria. Segundo, adoptar la heurística de programación a 35 días: "
        f"los casos accionables suben del {fmt_pct(float(baseline['actionable_pct']), 1)} al "
        f"{fmt_pct(float(redesigned['actionable_pct']), 1)}, mientras el riesgo residual no atendido baja del "
        f"{fmt_pct(float(baseline['riesgo_residual_no_atendido_pct']), 1)} al "
        f"{fmt_pct(float(redesigned['riesgo_residual_no_atendido_pct']), 1)} y el valor capturado aumenta "
        f"{fmt_money_m(captured_value_delta, 2)}."
    )
    d.p(
        f"Tercero, evitar aplazamientos no justificados. Diferir 14 días añade {fmt_money_m(mv('deferral_cost_delta_14d_eur'), 2)} "
        f"de coste aproximado y {fmt_dec(mv('deferral_downtime_delta_14d_h'), 0)} horas de indisponibilidad frente a intervenir en "
        "el día cero. Cuarto, concentrar la fiabilidad en pantógrafos, que combinan la peor salud media y el mayor riesgo "
        f"medio de fallo ({fmt_pct(families.loc['pantograph', 'failure_risk_avg'] * 100, 1)}). Quinto, tratar CBM como una "
        "decisión de nivel de servicio: bajo los supuestos vigentes mejora la disponibilidad en "
        f"+{fmt_dec(av_gap, 2)} puntos porcentuales frente a reactiva, pero su diferencial neto esperado es "
        f"{fmt_money_m(mv('cbm_operational_savings_eur'))}, con una probabilidad modelada de ahorro positivo del "
        f"{fmt_pct(mv('cbm_prob_positive_savings') * 100, 0)}."
    )

    # ----- 1. Contexto --------------------------------------------------- #
    d.h(2, "1. Contexto y objetivos", anchor="sec-contexto", section="Contexto y objetivos")
    d.p(
        "Una operación ferroviaria no puede decidir mantenimiento con una única señal. Un componente con salud "
        "baja puede tener escaso impacto de servicio si su unidad opera en rutas redundantes; una unidad con riesgo "
        "moderado puede convertirse en prioridad si el aplazamiento eleva la probabilidad de fallo o si la siguiente "
        "ventana operativa disponible es distante. Estas tensiones se resuelven a diario de forma implícita en la "
        "planificación de taller, casi siempre sin un criterio común y trazable. El proyecto las integra en una "
        "secuencia de intervención explícita que conecta condición técnica, presión de servicio y restricción de capacidad."
    )
    d.p(
        f"El alcance cubre {fmt_int(mv('n_flotas'))} flotas, {fmt_int(mv('n_unidades'))} unidades, "
        f"{fmt_int(mv('n_componentes'))} componentes críticos y {fmt_int(mv('n_depositos'))} depósitos, con dos años "
        f"de cobertura temporal que abarcan señales de sensor, operación, fallos, inspecciones automáticas y eventos "
        "de mantenimiento. La amplitud importa porque permite distinguir patrones estructurales de episodios aislados: "
        "una unidad recurrente en la cola de riesgo durante varios meses plantea un problema de activo distinto al de "
        "una unidad que aparece una sola vez tras un incidente puntual."
    )
    d.h(3, "Los objetivos analíticos se traducen en preguntas de decisión")
    d.p(
        "El análisis responde a seis preguntas operativas. Qué unidades y componentes deben entrar primero en taller. "
        "Cuánto cuesta aplazar cada intervención. Dónde existe presión excesiva o capacidad ociosa. Qué señales "
        "explican el riesgo. Qué familias técnicas requieren una política diferenciada. Y qué estrategia de "
        "mantenimiento ofrece el mejor equilibrio entre servicio, coste y riesgo. Ninguna de estas preguntas se "
        "responde con un único número, y la arquitectura del sistema está deliberadamente construida para no forzarlo."
    )
    d.p(
        "La estructura de salida evita mezclar conceptos que se confunden con frecuencia. Los pendientes físicos miden carga "
        "pendiente real. El riesgo de diferimiento mide el daño esperado de posponer una decisión. La salud describe "
        "condición. El riesgo de fallo ordena propensión a corto plazo. La vida remanente se utiliza como ventana "
        "relativa de intervención, no como fecha exacta de fallo. Mantener separadas estas definiciones es lo que "
        "permite que cada métrica cumpla una función concreta sin contaminar a las demás."
    )

    # ----- 2. Datos y metodología --------------------------------------- #
    d.h(2, "2. Datos y metodología", anchor="sec-datos", section="Datos y metodología")
    d.p(
        "La capa analítica parte de datos sintéticos deterministas de sensores, inspecciones automáticas, fallos, "
        "eventos de mantenimiento, disponibilidad, demanda de servicio y pendientes. El determinismo es una decisión de "
        "diseño: garantiza que cualquier persona que ejecute el flujo obtenga exactamente las mismas cifras, lo "
        "que es condición necesaria para auditar definiciones y reproducir hallazgos. Las tablas procesadas preservan "
        "granularidades separadas para componente-día, unidad-día, flota-semana, depósito-día e intervención-corte, "
        "de modo que cada análisis opera al nivel correcto y no agrega magnitudes incompatibles."
    )
    d.p(
        "Esa separación de grano es la primera línea de defensa contra errores de interpretación. La disponibilidad "
        "se analiza por flota-semana; el riesgo y la vida remanente, por componente; la presión de taller, por "
        "depósito-día; y la prioridad de intervención, por caso en el corte más reciente. Sumar pendientes "
        "históricos acumulados y compararlos con un corte de pendientes actuales produciría una cifra sin significado, "
        "y la arquitectura lo impide por construcción."
    )
    gran_rows = [
        ("Componente-día", "Salud, riesgo de fallo, vida remanente, deterioro", "Modelo de salud y riesgo"),
        ("Unidad-día", "Riesgo de indisponibilidad, exposición de servicio", "Tabla analítica de unidad-día"),
        ("Flota-semana", "Disponibilidad, MTBF y MTTR aproximados", "Tabla analítica semanal de flota"),
        ("Depósito-día", "Saturación, presión y capacidad de taller", "Tabla analítica de presión de talleres"),
        ("Intervención-corte", "Prioridad, riesgo de diferimiento, decisión", "Modelo de priorización"),
    ]
    gran_html = "".join(
        f"<tr><td><strong>{escape(g)}</strong></td><td>{escape(c)}</td><td>{escape(s)}</td></tr>"
        for g, c, s in gran_rows
    )
    d.add(
        "<table class='data'><thead><tr><th>Granularidad</th><th>Qué mide</th><th>Origen</th></tr></thead>"
        f"<tbody>{gran_html}</tbody></table>"
        "<p class='tbl-note'>Niveles de grano preservados por la capa analítica. Cada análisis opera en su grano "
        "natural y no se agregan magnitudes definidas a niveles distintos.</p>"
    )
    d.h(3, "La calidad se controla con contratos de datos y métricas")
    d.figure(
        "19_gobernanza_validaciones.png",
        f"Las validaciones de publicación no presentan bloqueos activos: las {len(checks)} verificaciones, "
        "repartidas entre severidad alta y crítica, están aprobadas.",
        "registro de gobernanza de métricas",
    )
    d.p(
        f"El registro de gobernanza contiene {len(checks)} verificaciones activas. Las "
        f"{int(checks['passed'].sum())} están aprobadas y {int((~checks['passed']).sum())} fallan; el número de "
        f"bloqueos de publicación activos es {int(checks['publish_blocker'].sum())}. Los contratos fijan, para cada "
        "métrica comunicada, su fuente de verdad, su granularidad, su definición, su regla de agregación y su "
        "anti-interpretación, es decir, la lectura que queda explícitamente prohibida. Este último campo es el que "
        "impide, por ejemplo, que la vida remanente se comunique como fecha garantizada de fallo."
    )
    d.p(
        "El resumen ejecutivo toma sus cifras de un registro oficial de métricas. La consecuencia práctica es que el "
        "panel de control, el README y este informe comunican la misma cifra para disponibilidad, pendientes, riesgo de "
        "diferimiento o valor estratégico, porque todos leen del mismo origen. La divergencia de cifras entre "
        "documentos es uno de los fallos más frecuentes y más corrosivos para la credibilidad de un sistema analítico, "
        "y aquí se elimina en origen en lugar de corregirse a mano."
    )
    contract_rows = "".join(
        f"<tr><td><strong>{escape(str(row['business_name']))}</strong></td>"
        f"<td>{escape(str(row['owner']))}</td>"
        f"<td>{escape(str(row['valid_grain']))}</td>"
        f"<td>{escape(str(row['maturity']))}</td></tr>"
        for _, row in contracts.head(8).iterrows()
    )
    d.add(
        "<table class='data'><thead><tr><th>Métrica gobernada</th><th>Responsable</th><th>Grano válido</th>"
        "<th>Madurez</th></tr></thead><tbody>" + contract_rows + "</tbody></table>"
        "<p class='tbl-note'>Extracto del registro de contratos de métricas. El responsable define responsabilidad funcional; "
        "el grano válido bloquea agregaciones incompatibles; la madurez separa métricas listas para comunicación "
        "ejecutiva de métricas que requieren calibración adicional.</p>"
    )
    d.h(3, "Las puntuaciones son interpretables y están orientadas a priorización")
    d.p(
        "La salud del componente combina condición estimada, índice de deterioro, velocidad de degradación, defectos "
        "recientes de inspección y restauración tras mantenimiento. El riesgo de fallo utiliza una combinación lineal "
        "y sigmoide de señales de deterioro, estrés operativo, historial y exposición a pendientes. La puntuación de prioridad "
        "de intervención añade el impacto de servicio, el riesgo de diferimiento y el ajuste al taller recomendado. "
        "Cada capa es legible: una recomendación conserva su regla, su señal dominante, su nivel de confianza y su "
        "justificación, de forma que un planificador puede entender por qué un caso está por encima de otro."
    )
    d.p(
        "El diseño favorece la trazabilidad sobre la complejidad predictiva. Es una elección deliberada: "
        "un modelo legible y auditable tiene más valor operativo que uno más opaco con mayor precisión aparente "
        "sobre datos que no ofrecen señal de validación externa. La contrapartida es explícita: antes de un uso "
        "operacional real, las puntuaciones requieren recalibración con histórico observado y validación temporal."
    )
    d.h(3, "La comparación estratégica separa servicio, técnica y economía")
    d.p(
        "Las estrategias reactiva, preventiva rígida y basada en condición se comparan bajo supuestos estructurales y "
        "varios perfiles de escenario. La simulación incorpora indisponibilidad, correctivas evitables, pendientes "
        "crítico, utilización de taller, coste técnico, coste económico y coste de habilitación. Los importes son "
        "aproximaciones técnico-operativas. No representan presupuesto, cuenta de resultados ni oferta contractual, y su función no es producir "
        "una cifra de ahorro sino hacer visibles las compensaciones y probar la estabilidad de una decisión ante variaciones "
        "de coste, capacidad, estrés de fallo y realización de la detección."
    )

    # ----- 3. Marco analítico ------------------------------------------- #
    d.h(2, "3. Marco analítico", anchor="sec-marco", section="Marco analítico")
    d.p(
        "La cadena de decisión conecta evidencia con ejecución en una secuencia ordenada. Comienza en la calidad del "
        "dato, convierte señales en estado y riesgo, estima una ventana relativa de intervención, ordena casos por "
        "prioridad, asigna capacidad de taller y cuantifica el coste de diferir. El resultado final no es una puntuación "
        "aislada sino una recomendación ejecutable, con depósito sugerido, secuencia y ventana operativa. Un control "
        "es esencial en toda la cadena: una señal técnica no genera por sí sola una orden de trabajo. La prioridad "
        "emerge de la combinación entre riesgo, impacto de servicio, oportunidad operativa y restricción de taller."
    )
    d.p(
        "Las métricas se interpretan dentro de límites explícitos. Disponibilidad alta no implica bajo riesgo futuro. "
        "Un volumen elevado de pendientes no convierte todas las órdenes en urgentes. La correlación de una señal con el riesgo no "
        "prueba causalidad física. La vida remanente no es una fecha de fallo. El ahorro aproximado no es ahorro "
        "financiero. Estas distinciones no son matices académicos: cada una bloquea una decisión equivocada concreta, "
        "y el marco las mantiene activas en lugar de relajarlas para simplificar el mensaje."
    )
    d.p(
        "Los umbrales del sistema organizan decisiones, no simulan certeza. El corte de alto riesgo en media más "
        "uno coma cinco desviaciones típicas es una regla de segmentación que concentra la atención, no una frontera "
        "física entre componentes sanos y enfermos. Los casos próximos a cualquier umbral deben revisarse junto con la "
        "confianza de la señal, la criticidad de servicio y la disponibilidad de repuesto. El marco está construido "
        "para soportar ese juicio experto, no para sustituirlo."
    )
    d.h(3, "El modelo necesita derechos de decisión, no solo precisión")
    d.p(
        "La calidad de un sistema de mantenimiento predictivo se mide por la decisión que cambia, no por la cantidad "
        "de señales que agrega. Por eso el marco separa recomendación, autorización y ejecución. El modelo prioriza; "
        "Planificación confirma factibilidad; Operaciones valida impacto de servicio; Finanzas revisa el caso "
        "económico; Fiabilidad aprende de la intervención ejecutada. Esta asignación reduce dos riesgos habituales: "
        "automatizar una orden sin contexto operativo y, en el extremo opuesto, convertir la puntuación en una recomendación "
        "decorativa que nadie tiene obligación de seguir."
    )
    decision_rows = [
        ("Priorizar intervención", "Modelo + analítica de fiabilidad", "Puntuación, RUL, fallo 30d, factor y confianza", "Clasificación y nivel P1/P2/P3"),
        ("Autorizar excepción", "Dirección de Mantenimiento", "Motivo, restricción, coste de diferir y nueva fecha", "Excepción trazable"),
        ("Asignar capacidad", "Planificación de Taller", "Horas, depósito, repuesto, ventana y compatibilidad", "Plan de 35 días"),
        ("Medir impacto", "Operaciones + Finanzas", "Disponibilidad, indisponibilidad, coste aproximado y servicio preservado", "Revisión mensual"),
        ("Recalibrar modelo", "Reliability Analytics", "Orden ejecutada, resultado observado, falsa alerta y reincidencia", "Nuevo set de umbrales"),
    ]
    decision_html = "".join(
        f"<tr><td><strong>{escape(a)}</strong></td><td>{escape(b)}</td><td>{escape(c)}</td><td>{escape(e)}</td></tr>"
        for a, b, c, e in decision_rows
    )
    d.add(
        "<table class='data'><thead><tr><th>Decisión</th><th>Responsable</th><th>Evidencia mínima</th>"
        "<th>Salida esperada</th></tr></thead><tbody>" + decision_html + "</tbody></table>"
        "<p class='tbl-note'>Modelo operativo recomendado para que el sistema gobierne decisiones reales sin convertir "
        "la analítica en automatismo opaco ni en reporte pasivo.</p>"
    )

    # ----- 4. Diagnóstico operativo ------------------------------------- #
    d.h(2, "4. Diagnóstico operativo", anchor="sec-diagnostico", section="Diagnóstico operativo")
    d.lead(
        "La disponibilidad agregada permanece alta y estable, pero el riesgo y los pendientes se concentran en una parte "
        "limitada de la red. El diagnóstico operativo consiste precisamente en localizar esa concentración para que la "
        "intervención sea focalizada en lugar de uniforme."
    )
    d.figure(
        "01_tendencia_disponibilidad.png",
        f"La disponibilidad media del periodo es {fmt_pct(mv('fleet_availability_pct'), 2)} y la tendencia móvil "
        "de ocho semanas no muestra una ruptura estructural que por sí sola justifique una intervención estratégica.",
        "tabla analítica semanal de flota",
    )
    d.p(
        f"La media del periodo es {fmt_pct(mv('fleet_availability_pct'), 2)}, con un tiempo medio entre fallos aproximado de "
        f"{fmt_dec(mv('mtbf_proxy_hours'), 0)} horas y un tiempo medio de reparación aproximado de {fmt_dec(mv('mttr_proxy_hours'), 1)} "
        "horas. La serie semanal conserva un nivel alto sin quiebres abruptos. Esa estabilidad es real, pero es una "
        "medida retrospectiva: resume lo que ya ocurrió y no anticipa la exposición que se está acumulando en la cola "
        "de componentes deteriorados. La gestión debe usar las dos capas a la vez y evitar la tentación de sustituir el "
        "puntuación de riesgo por el indicador de disponibilidad solo porque este último resulta cómodo y conocido."
    )
    d.figure(
        "06_distribucion_riesgo_unidades.png",
        f"El umbral estadístico de alto riesgo, situado en media más 1,5 desviaciones típicas, captura {high_risk} "
        f"unidades de las {fmt_int(mv('n_unidades'))} de la flota.",
        "modelo de riesgo de indisponibilidad",
    )
    d.p(
        f"El riesgo de unidad se concentra en una cola estrecha. El umbral media más uno coma cinco sigma se sitúa en "
        f"{fmt_dec(threshold, 1)} puntos y captura {high_risk} unidades. Una cola tan acotada es una buena noticia "
        "operativa: permite concentrar la revisión técnica en un número manejable de casos sin convertir el modelo en "
        "una alarma generalizada que la organización aprendería a ignorar. El umbral, conviene insistir, es una regla "
        "de segmentación y no una frontera física; la dirección debe vigilar la estabilidad de la clasificación entre semanas "
        "antes de usarlo como criterio automático de parada."
    )
    d.figure(
        "11_concentracion_backlog.png",
        f"Los tres depósitos con mayor volumen agregado de pendientes concentran el {fmt_pct(top3_backlog_share, 1)} del total "
        f"histórico representado sobre los {n_depots_backlog} depósitos.",
        "tabla analítica de pendientes por depósito",
    )
    d.p(
        f"La carga pendiente está geográficamente concentrada. Los tres depósitos con mayor volumen agregado de pendientes reúnen el "
        f"{fmt_pct(top3_backlog_share, 1)} del total histórico representado. Esa concentración convierte un problema "
        "difuso en uno tratable: en lugar de una campaña de cierre de pendientes en toda la red, la intervención se dirige "
        "a la disciplina de cierre, la antigüedad de las órdenes y la asignación en un puñado de instalaciones. El "
        "volumen agregado por periodo no equivale al corte ejecutivo de pendientes físicos, y aquí se usa para "
        "localizar concentración persistente, no para comunicar el volumen actual."
    )
    d.figure(
        "12_pareto_fallos_repetitivos.png",
        f"Los cinco modos con mayor repetición concentran el {fmt_pct(top5_fail_share, 1)} de los eventos repetitivos "
        "incluidos en la clasificación.",
        "tabla analítica de fallos repetitivos",
    )
    d.p(
        f"La repetición de fallos también admite tratamiento de cartera priorizada. Los cinco modos más frecuentes "
        f"concentran el {fmt_pct(top5_fail_share, 1)} de los eventos repetitivos de la clasificación, con el arco eléctrico, "
        "los bloqueos de actuador y la pérdida de contacto entre los primeros casos. Reducir esta repetición no es "
        "cuestión de acelerar correctivas: requiere análisis de causa raíz, revisión del mantenimiento estándar y "
        "seguimiento de la reincidencia después de cada intervención. Una correctiva más rápida sobre el mismo modo "
        "que reaparece cada pocas semanas mejora un indicador de respuesta sin resolver el mecanismo subyacente."
    )
    d.figure(
        "17_ranking_indisponibilidad.png",
        "Un conjunto reducido de unidades concentra una proporción desproporcionada de las horas históricas fuera de servicio.",
        "tabla analítica de indisponibilidad por unidad",
    )
    d.p(
        "La indisponibilidad histórica está concentrada en pocas unidades, lo que apunta a un problema de activo más "
        "que a episodios independientes. Esta clasificación debe cruzarse con la cola de riesgo actual para distinguir la "
        "persistencia estructural de los episodios ya corregidos. Las unidades que aparecen en ambas listas son "
        "candidatas a una revisión de causa raíz y a una estrategia de activo específica, porque repetir intervenciones "
        "aisladas sobre el mismo vehículo consume capacidad sin atacar el mecanismo que lo devuelve al taller."
    )
    d.callout(
        "Acción ejecutiva",
        "Operaciones debe abrir una revisión de las unidades de alto riesgo y explicar mensualmente la disponibilidad "
        "por flota; Mantenimiento debe lanzar planes de causa raíz para los cinco modos más repetitivos y depurar el "
        "pendientes antiguos en los tres depósitos que concentran la carga.",
        "danger",
    )

    # ----- 5. Fiabilidad ------------------------------------------------- #
    d.h(2, "5. Fiabilidad, inspección y vida remanente", anchor="sec-fiabilidad", section="Fiabilidad y vida remanente")
    d.lead(
        "Las políticas de mantenimiento deben diferenciar familias técnicas. Los pantógrafos concentran el riesgo, el "
        "cálculo aproximado de vida remanente ya distingue ventanas útiles de intervención y la inspección automática rinde de forma "
        "desigual según la familia. Una política uniforme diluiría todas estas diferencias."
    )
    d.figure(
        "08_determinantes_riesgo.png",
        "El índice de deterioro y la velocidad de degradación encabezan la correlación de Spearman con el riesgo de "
        "fallo; la restauración por mantenimiento aparece con signo negativo.",
        "análisis de determinantes de riesgo",
    )
    d.p(
        f"El ordenamiento de riesgo está dominado por señales de deterioro. El índice de deterioro presenta una "
        f"correlación de Spearman de {fmt_dec(determinants['deterioration_index'], 2)} con el riesgo de fallo y la "
        f"velocidad de degradación, de {fmt_dec(determinants['degradation_velocity'], 2)}. Los eventos de choque "
        f"({fmt_dec(determinants['shock_event_count'], 2)}) y las anomalías de los últimos 30 días "
        f"({fmt_dec(determinants['anomaly_count_30d'], 2)}) aportan separación adicional relevante. El índice de "
        f"restauración por mantenimiento muestra una relación negativa de {fmt_dec(determinants['maintenance_restoration_index'], 2)}, "
        "coherente con la reducción de riesgo esperada tras una intervención efectiva."
    )
    d.p(
        "Las variables con correlación próxima a cero no deben eliminarse de forma automática. La exposición a pendientes, "
        "los días desde el último fallo o desde el último mantenimiento no ordenan el riesgo de forma monotónica, pero "
        "pueden actuar como reglas de contexto o como restricciones que condicionan la decisión final. Confundir "
        "ausencia de correlación monotónica con ausencia de valor informativo es un error que empobrecería el modelo."
    )
    d.figure(
        "09_salud_vs_riesgo.png",
        "Para niveles de salud similares persiste dispersión material en el riesgo de fallo, porque la trayectoria de "
        "deterioro, las anomalías y el estrés separan la exposición.",
        "modelo de salud y riesgo de componentes",
    )
    d.p(
        "La salud resume condición, pero el riesgo incorpora trayectoria. La nube de componentes confirma una relación "
        "inversa entre ambos, con una dispersión notable para niveles de salud parecidos. Esa dispersión procede de la "
        "velocidad de degradación, de las anomalías, del estrés operativo y de la restauración tras mantenimiento. Una "
        "política basada solo en salud perdería componentes con trayectoria adversa que todavía exhiben una condición "
        "aceptable, y sobrerreaccionaría ante componentes estables de salud baja pero sin deterioro activo. La cola de "
        "taller debe conservar la puntuación de riesgo y la evidencia subyacente, no colapsar la decisión en una sola cifra."
    )
    d.figure(
        "10_riesgo_por_familia.png",
        f"Los pantógrafos presentan el mayor riesgo medio de fallo entre las familias modeladas, con "
        f"{fmt_pct(families.loc['pantograph', 'failure_risk_avg'] * 100, 1)}.",
        "segmentación de riesgo por familia",
    )
    d.p(
        f"Los pantógrafos requieren una política específica. Presentan un riesgo medio de fallo de "
        f"{fmt_pct(families.loc['pantograph', 'failure_risk_avg'] * 100, 1)} y una salud media de "
        f"{fmt_dec(families.loc['pantograph', 'health_score_avg'], 1)} puntos, los peores valores entre las cuatro "
        f"familias. Los bogies les siguen con {fmt_pct(families.loc['bogie', 'failure_risk_avg'] * 100, 1)}, por delante "
        f"de frenos ({fmt_pct(families.loc['brake', 'failure_risk_avg'] * 100, 1)}) y ruedas "
        f"({fmt_pct(families.loc['wheel', 'failure_risk_avg'] * 100, 1)}). La diferencia justifica revisar umbrales, "
        "repuestos, capacidad técnica e inspección por familia. Un único acuerdo de nivel de servicio de mantenimiento "
        "para todas las familias diluye una exposición que está claramente segmentada."
    )
    d.figure(
        "13_cohortes_rul_familia.png",
        f"La mediana de vida remanente aproximada va de {fmt_int(rul_fam.loc['pantograph', 'new_p50'])} días en pantógrafos "
        f"a {fmt_int(rul_fam.loc['bogie', 'new_p50'])} días en bogies, con rangos P10-P90 también diferenciados.",
        "validación de la vida remanente aproximada",
    )
    d.p(
        f"Las ventanas de vida remanente difieren de forma material entre familias. La mediana del pantógrafo es "
        f"{fmt_int(rul_fam.loc['pantograph', 'new_p50'])} días, frente a {fmt_int(rul_fam.loc['brake', 'new_p50'])} en "
        f"frenos, {fmt_int(rul_fam.loc['wheel', 'new_p50'])} en ruedas y {fmt_int(rul_fam.loc['bogie', 'new_p50'])} en "
        "bogies. Los rangos P10-P90 también se separan, lo que confirma que una regla uniforme de intervención perdería "
        f"discriminación operativa. En el pantógrafo, el {fmt_pct(rul_fam.loc['pantograph', 'new_share_le_30'] * 100, 0)} "
        "de los componentes está dentro de la ventana de 30 días, una proporción que obliga a planificar repuesto y "
        "capacidad con antelación distinta a la del resto de familias."
    )
    d.figure(
        "14_rul_antes_despues.png",
        f"El modelo anterior concentraba el {fmt_pct(rul_dist.loc['legacy_lineal_365', 'share_rul_cap'] * 100, 1)} de las "
        f"observaciones en el tope de 365 días; el cálculo aproximado por familia reduce ese tope al "
        f"{fmt_pct(rul_dist.loc['nuevo_proxy_familia', 'share_rul_cap'] * 100, 1)}.",
        "validación de la vida remanente aproximada",
    )
    d.p(
        f"El rediseño de la vida remanente aproximada corrige una distribución sin utilidad operativa. El modelo anterior "
        f"tenía una mediana de {fmt_int(rul_dist.loc['legacy_lineal_365', 'p50_rul'])} días y acumulaba el "
        f"{fmt_pct(rul_dist.loc['legacy_lineal_365', 'share_rul_cap'] * 100, 1)} de las observaciones en el tope, lo que "
        "lo hacía inservible para secuenciar intervenciones. El cálculo aproximado por familia reduce la mediana a "
        f"{fmt_int(rul_dist.loc['nuevo_proxy_familia', 'p50_rul'])} días y deja prácticamente vacío el tope, con un "
        f"{fmt_pct(rul_dist.loc['nuevo_proxy_familia', 'share_rul_cap'] * 100, 2)}. La mejora principal no es de nivel "
        "sino de discriminación: el nuevo cálculo distribuye los casos entre ventanas útiles y permite combinar urgencia "
        "técnica con capacidad disponible. Sigue siendo una aproximación y no una cuenta atrás física, y así debe comunicarse."
    )
    d.figure(
        "15_inspeccion_por_familia.png",
        f"La tasa de detección previa a fallo varía entre {fmt_pct(insp_min, 1)} y {fmt_pct(insp_max, 1)} según la familia.",
        "módulo de inspección automática",
    )
    d.p(
        f"La inspección automática no rinde igual en todas las familias. La tasa de detección previa a fallo va desde el "
        f"{fmt_pct(insp_min, 1)} en {insp_min_fam} hasta el {fmt_pct(insp_max, 1)} en {insp_max_fam}, con un tiempo de "
        f"anticipación medio del orden de {fmt_dec(insp_lead, 0)} días. La cobertura de componentes es total, pero la "
        "cobertura no garantiza el mismo valor de decisión: la precisión, el seguimiento y la conversión de la alerta "
        "en intervención difieren entre familias. La gestión debe medir la detección ajustada por confianza y por falsa "
        "alerta, porque aumentar el número de inspecciones sin mejorar su conversión a orden de trabajo eleva la carga "
        "sin reducir el riesgo."
    )
    d.p(
        f"El módulo de inspección tiene valor operacional medible incluso antes de cerrar el caso financiero de CBM. "
        f"En el escenario comparativo reduce las correctivas estimadas en {fmt_dec(inspection_correctives_delta, 1)} "
        f"eventos aproximados, evita {fmt_dec(inspection_downtime_delta, 0)} horas de indisponibilidad y disminuye los pendientes "
        f"crítico estimado de {fmt_dec(inspection_value.loc['sin_inspeccion_automatica', 'backlog_critico_estimado'], 1)} "
        f"a {fmt_dec(inspection_value.loc['con_inspeccion_automatica', 'backlog_critico_estimado'], 1)}. La conclusión "
        "no es desplegar inspección indiscriminadamente; es instrumentar la cadena completa alerta → orden → ejecución "
        "→ resultado, porque solo esa cadena convierte detección temprana en disponibilidad preservada."
    )
    fam_order = ["pantograph", "bogie", "brake", "wheel"]
    fam_rows = "".join(
        f"<tr><td><strong>{FAM_ES_CAP[f]}</strong></td>"
        f"<td class='num'>{fmt_int(families.loc[f, 'component_count'])}</td>"
        f"<td class='num'>{fmt_pct(families.loc[f, 'failure_risk_avg'] * 100, 1)}</td>"
        f"<td class='num'>{fmt_dec(families.loc[f, 'health_score_avg'], 1)}</td>"
        f"<td class='num'>{fmt_int(rul_fam.loc[f, 'new_p50'])} d</td>"
        f"<td class='num'>{fmt_pct(inspection.loc[f, 'pre_failure_detection_rate'] * 100, 1)}</td></tr>"
        for f in fam_order
    )
    d.add(
        "<table class='data'><thead><tr><th>Familia</th><th class='num'>Componentes</th>"
        "<th class='num'>Riesgo medio</th><th class='num'>Salud media</th><th class='num'>RUL mediana</th>"
        "<th class='num'>Detección previa</th></tr></thead><tbody>" + fam_rows + "</tbody></table>"
        "<p class='tbl-note'>Perfil técnico por familia. El riesgo medio y la salud proceden de la segmentación de "
        "componentes; la mediana de vida remanente, del cálculo rediseñado; la detección previa, del módulo de "
        "inspección automática.</p>"
    )
    d.callout(
        "Acción ejecutiva",
        "Fiabilidad debe implantar un plan específico para pantógrafos, revisar trimestralmente la falsa alerta y la "
        "detección por familia, y usar la vida remanente únicamente por bucket y familia, validándola con cortes "
        "temporales reales antes de incorporarla a compromisos de servicio.",
        "warning",
    )

    # ----- 6. Priorización ---------------------------------------------- #
    d.h(2, "6. Priorización y capacidad de taller", anchor="sec-priorizacion", section="Priorización y capacidad")
    d.lead(
        "La mejora inmediata no requiere más capacidad. Requiere proteger la cabeza de la cola priorizada y asignar "
        "mejor las horas de taller ya disponibles. La restricción dominante es de asignación y de horizonte de "
        "planificación, no de horas absolutas."
    )
    d.figure(
        "04_ranking_intervenciones.png",
        f"La cola ejecutiva está dominada por un grupo reducido de casos de prioridad muy alta, encabezado por "
        f"{top['unidad_id']} / {top['componente_id']}.",
        "modelo de priorización de taller",
    )
    d.p(
        f"La primera decisión está claramente identificada. {top['unidad_id']} / {top['componente_id']} encabeza la cola "
        f"con una prioridad de {fmt_dec(top['intervention_priority_score'], 1)}, un riesgo de fallo a 30 días de "
        f"{fmt_pct(top['prob_fallo_30d'] * 100, 1)} y una vida remanente aproximada de {fmt_int(top['component_rul_estimate'])} "
        f"días. La recomendación es {str(top['decision_type'])} en el depósito {top['deposito_recomendado']}, dentro de "
        f"una ventana de {fmt_int(top['suggested_window_days'])} días. La cola superior presenta puntuaciones próximas entre sí, "
        "de modo que la ejecución debe conservar la secuencia y registrar cualquier excepción con su motivo y su "
        "autorizador. La tabla siguiente recoge los diez primeros casos con su nivel de prioridad asignado."
    )
    d.p(
        f"La diferencia entre los tres primeros casos es de solo {fmt_dec(top3_priority_spread, 1)} puntos de prioridad. "
        "Ese margen estrecho cambia la forma de gobernar la cola: no basta con seleccionar el primer activo y olvidar "
        "el resto. Los tres primeros casos deben tratarse como un paquete de protección P1, con revisión diaria de "
        "capacidad, repuesto y ventana de servicio. Si una restricción real bloquea el primer caso, la sustitución debe "
        "elegirse dentro del mismo paquete y no mediante una nueva negociación informal de prioridades."
    )

    # Priority table
    fam = FAM_ES_CAP
    rows_html = []
    for i, row in priority.head(10).reset_index(drop=True).iterrows():
        tier = "P1" if i < 3 else "P2" if i < 7 else "P3"
        tier_cls = "p1" if i < 3 else "p2" if i < 7 else "p3"
        rows_html.append(
            f"<tr><td class='tier {tier_cls}'>{tier}</td>"
            f"<td>{row['unidad_id']} / {row['componente_id']}</td>"
            f"<td>{fam.get(row['component_family'], row['component_family'])}</td>"
            f"<td class='num'>{fmt_dec(row['intervention_priority_score'], 1)}</td>"
            f"<td class='num'>{fmt_dec(row['deferral_risk_score'], 1)}</td>"
            f"<td class='num'>{fmt_int(row['suggested_window_days'])} d</td>"
            f"<td>{row['deposito_recomendado']}</td></tr>"
        )
    d.add(
        "<table class='data'><thead><tr><th>Nivel</th><th>Unidad / componente</th><th>Familia</th>"
        "<th class='num'>Prioridad</th><th class='num'>Riesgo de diferir</th><th class='num'>Ventana</th>"
        "<th>Depósito</th></tr></thead><tbody>" + "".join(rows_html) + "</tbody></table>"
        "<p class='tbl-note'>Cola priorizada de intervención. Niveles P1 a P3 según posición en la clasificación compuesta. "
        "Fuente: modelo de priorización de taller.</p>"
    )

    d.figure(
        "07_scheduling_antes_despues.png",
        "El rediseño de la programación convierte capacidad bloqueada en casos accionables y traslada parte del "
        "problema a la coordinación de repuesto.",
        "simulación de programación de taller",
    )
    d.p(
        f"La planificación rediseñada cambia la naturaleza del cuello de botella. Los casos accionables aumentan "
        f"{fmt_dec(actionable_delta_pp, 1)} puntos porcentuales, del "
        f"{fmt_pct(float(baseline['actionable_pct']), 1)} al {fmt_pct(float(redesigned['actionable_pct']), 1)}. Los "
        f"pendientes por capacidad bajan del {fmt_pct(float(baseline['pendiente_capacidad_pct']), 1)} al "
        f"{fmt_pct(float(redesigned['pendiente_capacidad_pct']), 1)}, y el valor capturado aproximado sube de "
        f"{fmt_money_m(float(baseline['valor_capturado_proxy']), 2)} a {fmt_money_m(float(redesigned['valor_capturado_proxy']), 2)}. "
        f"El rediseño introduce un {fmt_pct(float(redesigned['pendiente_repuesto_pct']), 1)} de casos pendientes de "
        "repuesto, lo que no es un defecto sino un cambio de problema: parte de la restricción se desplaza de la "
        "capacidad de taller a la coordinación de suministro, donde es más barata de resolver."
    )
    bottleneck_rows = "".join(
        f"<tr><td><strong>{escape(str(row['deposito_id']))}</strong></td>"
        f"<td class='num'>{fmt_int(row['casos'])}</td>"
        f"<td class='num'>{fmt_dec(row['horas_requeridas'], 1)} h</td>"
        f"<td class='num'>{fmt_int(row['pendientes_capacidad'])}</td>"
        f"<td class='num'>{fmt_pct(row['pending_rate_pct'], 1)}</td></tr>"
        for _, row in bottlenecks.iterrows()
    )
    d.add(
        "<table class='data'><thead><tr><th>Depósito</th><th class='num'>Casos</th>"
        "<th class='num'>Horas requeridas</th><th class='num'>Pendientes por capacidad</th>"
        "<th class='num'>Tasa pendiente</th></tr></thead><tbody>" + bottleneck_rows + "</tbody></table>"
        "<p class='tbl-note'>Diagnóstico de cuello de botella sobre la programación rediseñada. La tasa pendiente "
        "mide casos que siguen sin absorberse por restricción de capacidad del depósito recomendado.</p>"
    )
    d.p(
        f"El cuello de botella no está repartido de forma homogénea. {primary_bottleneck['deposito_id']} reúne el "
        f"{fmt_pct(primary_bottleneck_case_share, 1)} de los casos del diagnóstico y el "
        f"{fmt_pct(primary_bottleneck_pending_share, 1)} de los pendientes por capacidad. La lectura para gestión es "
        "directa: antes de ampliar estructura, hay que mover trabajo compatible, revisar reglas de depósito recomendado "
        "y confirmar qué restricciones son técnicas, logísticas o contractuales. Solo el remanente tras esa limpieza "
        "debe tratarse como falta estructural de capacidad."
    )
    d.figure(
        "16_utilizacion_capacidad.png",
        f"La utilización agregada del calendario de 35 días es {fmt_pct(avg_capacity_use, 1)}, lo que confirma que el "
        "bloqueo no se explica solo por falta absoluta de horas.",
        "calendario de capacidad de taller",
    )
    d.p(
        f"La capacidad disponible y la cola priorizada no están plenamente alineadas. La utilización agregada del "
        f"calendario de 35 días es {fmt_pct(avg_capacity_use, 1)}. Frente a la base inicial, el rediseño utiliza "
        f"{fmt_dec(workshop_hours_delta, 0)} horas adicionales de taller y eleva la utilización "
        f"{fmt_dec(capacity_use_delta_pp, 1)} puntos porcentuales, sin agotar la red. Intervienen la compatibilidad "
        "técnica, la secuencia, la ventana operativa y el depósito asignado. La prioridad inmediata es mejorar la "
        "asignación y la flexibilidad controlada; cualquier expansión de capacidad debe justificarse después de medir "
        "qué restricciones permanecen activas una vez "
        "estabilizado el rediseño, para no consolidar como estructural una ineficiencia que es de asignación."
    )
    d.figure(
        "05_saturacion_depositos.png",
        f"La saturación de taller está desigualmente distribuida; el depósito más exigido es {m['top_depot_by_saturation']} "
        f"con {fmt_pct(mv('top_depot_saturation_pct'), 1)} en el corte.",
        "tabla analítica de presión de talleres",
    )
    d.p(
        f"La presión de depósito exige rebalanceo antes que expansión. El depósito más exigido es "
        f"{m['top_depot_by_saturation']}, con {fmt_pct(mv('top_depot_saturation_pct'), 1)} de saturación en el corte, "
        f"frente a una saturación media de la red del {fmt_pct(mv('mean_depot_saturation_pct'), 1)}. Ese nivel no "
        "prueba una insuficiencia estructural de capacidad a escala de red, sino una dispersión que apunta primero a "
        "reasignación controlada, compatibilidad técnica y ajuste del calendario. Invertir en capacidad antes de "
        "corregir la asignación consolidaría las ineficiencias existentes en lugar de eliminarlas."
    )
    d.callout(
        "Acción ejecutiva",
        "Planificación de Taller debe congelar la secuencia P1 durante 14 días, exigir autorización ejecutiva para "
        "cualquier excepción y formalizar la heurística de 35 días midiendo semanalmente accionabilidad, pendientes por "
        "capacidad, pendientes por repuesto y riesgo residual.",
        "danger",
    )

    # ----- 7. Economía -------------------------------------------------- #
    d.h(2, "7. Economía y decisión estratégica", anchor="sec-economia", section="Economía y decisión")
    d.lead(
        "El mantenimiento basado en condición mejora el servicio, pero el diferencial económico actual no soporta una "
        "afirmación de ahorro. La honestidad de esta conclusión es lo que protege la credibilidad del resto del informe."
    )
    d.figure(
        "02_valor_estrategias.png",
        f"CBM mejora la disponibilidad frente a reactiva, pero su diferencial neto esperado es "
        f"{fmt_money_m(mv('cbm_operational_savings_eur'))} bajo los supuestos actuales.",
        "simulación de estrategias de mantenimiento",
    )
    d.p(
        f"CBM compra disponibilidad, no ahorro, bajo el escenario vigente. Alcanza un {fmt_pct(cbm_av, 2)} de "
        f"disponibilidad frente al {fmt_pct(re_av, 2)} de la estrategia reactiva, una mejora de {fmt_dec(av_gap, 2)} "
        f"puntos porcentuales, y preserva del orden de {fmt_int(cbm['horas_servicio_preservadas_vs_reactiva'])} horas de "
        "servicio adicionales. El coste, sin embargo, también es mayor: el coste total esperado aproximado de CBM es "
        f"{fmt_money_m(cbm_cost)}, frente a {fmt_money_m(re_cost)} de la reactiva y {fmt_money_m(pv_cost)} de la "
        f"preventiva rígida. El diferencial neto esperado de CBM frente a reactiva es {fmt_money_m(mv('cbm_operational_savings_eur'))}, "
        f"con un rango estimado entre {fmt_money_m(mv('cbm_value_range_min_eur'))} y {fmt_money_m(mv('cbm_value_range_max_eur'))} "
        f"y una probabilidad modelada de ahorro positivo del {fmt_pct(mv('cbm_prob_positive_savings') * 100, 0)}."
    )
    d.p(
        f"La comparación con la preventiva rígida endurece la tesis financiera. CBM aporta solo "
        f"{fmt_dec(cbm_vs_preventive_av_gap, 2)} puntos porcentuales adicionales de disponibilidad frente a preventiva, "
        f"pero añade {fmt_money_m(cbm_vs_preventive_cost_gap)} de coste esperado aproximado. Frente a reactiva, cada punto "
        f"porcentual adicional de disponibilidad cuesta aproximadamente {fmt_money_m(cbm_cost_per_availability_pp)} "
        "en el escenario base. Ese precio puede estar justificado si la disponibilidad tiene valor corporativo alto; "
        "lo que no está justificado es presentarlo como ahorro sin validar ese valor."
    )
    strat_order = [("reactiva", reactive), ("preventiva_rigida", preventive), ("basada_en_condicion", cbm)]
    strat_labels = {"reactiva": "Reactiva", "preventiva_rigida": "Preventiva rígida", "basada_en_condicion": "CBM"}
    strat_rows = "".join(
        f"<tr><td><strong>{strat_labels[name]}</strong></td>"
        f"<td class='num'>{fmt_pct(row['fleet_availability'], 2)}</td>"
        f"<td class='num'>{fmt_money_m(row['coste_total_esperado'])}</td>"
        f"<td class='num'>{fmt_money_m(row['ahorro_neto_vs_reactiva'])}</td>"
        f"<td class='num'>{fmt_pct(row['prob_ahorro_positivo'] * 100, 0)}</td>"
        f"<td class='num'>{fmt_int(row['horas_servicio_preservadas_vs_reactiva'])} h</td></tr>"
        for name, row in strat_order
    )
    d.add(
        "<table class='data'><thead><tr><th>Estrategia</th><th class='num'>Disponibilidad</th>"
        "<th class='num'>Coste total aproximado</th><th class='num'>Diferencial vs reactiva</th>"
        "<th class='num'>Prob. ahorro &gt;0</th><th class='num'>Horas servicio preservadas</th></tr></thead><tbody>"
        + strat_rows + "</tbody></table>"
        "<p class='tbl-note'>Comparación de estrategias sobre el escenario base. El diferencial y las horas preservadas "
        "se miden frente a la estrategia reactiva. Los importes son proxies técnico-operativos, no P&amp;L. Las cifras "
        "se redondean de forma independiente a partir de los valores completos; el diferencial puede no coincidir "
        "exactamente con la resta de las cifras de coste ya redondeadas. "
        "Fuente: simulación de estrategias de mantenimiento.</p>"
    )
    d.p(
        "La decisión necesita un precio sombra explícito para la disponibilidad y para las horas de servicio preservadas. "
        "Bajo el escenario base, ese "
        f"umbral es calculable: el coste incremental aproximado de CBM queda compensado si la organización valora cada hora "
        f"de servicio preservada en al menos € {fmt_int(mv('cbm_breakeven_value_per_service_hour_eur'))} por hora; "
        "por debajo de ese umbral, la mejora de disponibilidad no justifica el mayor coste, y no debe comprometerse "
        "capital sobre la base de un ahorro que el modelo no muestra. Este umbral es un punto de partida para la "
        "discusión de comité, no una cifra validada: debe contrastarse contra el valor corporativo real de la "
        "disponibilidad antes de usarse en una decisión de inversión. "
        "La preventiva rígida ocupa una posición intermedia interesante, con una probabilidad de ahorro positivo del "
        f"{fmt_pct(preventive['prob_ahorro_positivo'] * 100, 0)} y un rango que en su extremo superior alcanza "
        f"{fmt_money_m(preventive['ahorro_neto_p90_vs_reactiva'])}."
    )
    d.p(
        f"La sensibilidad refuerza la disciplina del mensaje. CBM tiene un {fmt_pct(cbm_positive_share, 1)} de casos "
        f"con ahorro positivo en la malla completa de {fmt_int(len(cbm_sens))} escenarios, con diferenciales entre "
        f"{fmt_money_m(cbm_sens_min)} y {fmt_money_m(cbm_sens_max)}. La preventiva rígida cruza a positivo en "
        f"{fmt_pct(preventive_positive_share, 1)} de sus escenarios, con rango entre {fmt_money_m(preventive_sens_min)} "
        f"y {fmt_money_m(preventive_sens_max)}. La consecuencia para comité es precisa: aprobar la ejecución operativa "
        "basada en riesgo, pero exigir una segunda puerta financiera antes de escalar CBM como programa de inversión."
    )
    sensitivity_rows = "".join(
        f"<tr><td><strong>{escape(profile.capitalize())}</strong></td>"
        f"<td class='num'>{fmt_money_m(group['ahorro_neto_vs_reactiva'].median())}</td>"
        f"<td class='num'>{fmt_money_m(group['ahorro_neto_vs_reactiva'].min())}</td>"
        f"<td class='num'>{fmt_money_m(group['ahorro_neto_vs_reactiva'].max())}</td>"
        f"<td class='num'>{fmt_pct((group['ahorro_neto_vs_reactiva'] > 0).mean() * 100, 1)}</td></tr>"
        for profile, group in cbm_sens.groupby("scenario_profile", sort=True)
    )
    d.add(
        "<table class='data'><thead><tr><th>Perfil CBM</th><th class='num'>Mediana vs reactiva</th>"
        "<th class='num'>Peor caso</th><th class='num'>Mejor caso</th><th class='num'>Ahorro positivo</th>"
        "</tr></thead><tbody>" + sensitivity_rows + "</tbody></table>"
        "<p class='tbl-note'>Sensibilidad de CBM frente a reactiva en los perfiles conservador, base y agresivo. "
        "Los importes son diferenciales netos aproximados; valores negativos indican coste incremental.</p>"
    )
    d.figure(
        "18_variancia_escenarios.png",
        "Los perfiles de escenario desplazan el coste P50 de cada estrategia y amplían la incertidumbre, sin revertir "
        "la dirección operativa.",
        "simulación de sensibilidad estratégica",
    )
    d.p(
        "La incertidumbre económica cambia la confianza, no la dirección operativa. Los perfiles de escenario desplazan "
        "el coste P50 de cada estrategia y amplían el rango de comparación; la preventiva rígida se aproxima al coste "
        "reactivo en algunos perfiles, mientras que CBM conserva el mayor coste aproximado bajo los supuestos vigentes. La "
        "dirección operativa, en cambio, permanece estable a través de los escenarios: reducir correctivas, proteger "
        "horas de servicio y anticipar componentes críticos. Esa estabilidad es la que permite actuar ya en lo "
        "operativo mientras la decisión de inversión espera una calibración financiera con costes reales."
    )
    d.figure(
        "03_coste_diferimiento.png",
        f"El coste de aplazar crece de forma monotónica: a 45 días el coste aproximado alcanza "
        f"{fmt_money_m(defer.loc[45, 'costo_total_eur'], 2)} y la indisponibilidad {fmt_dec(defer.loc[45, 'downtime_total_h'], 0)} horas.",
        "simulación de diferimiento",
    )
    d.p(
        f"Aplazar transfiere coste al servicio y amplifica la indisponibilidad. El escenario base estima "
        f"{fmt_money_m(defer.loc[0, 'costo_total_eur'], 2)} y {fmt_dec(defer.loc[0, 'downtime_total_h'], 0)} horas de "
        f"indisponibilidad. A 14 días, los valores suben a {fmt_money_m(defer.loc[14, 'costo_total_eur'], 2)} y "
        f"{fmt_dec(defer.loc[14, 'downtime_total_h'], 0)} horas; a 45 días, alcanzan {fmt_money_m(defer.loc[45, 'costo_total_eur'], 2)} "
        f"y {fmt_dec(defer.loc[45, 'downtime_total_h'], 0)} horas. La probabilidad media de fallo de la cola pasa del "
        f"{fmt_pct(defer.loc[0, 'prob_fallo_media'] * 100, 0)} al {fmt_pct(defer.loc[45, 'prob_fallo_media'] * 100, 0)} en "
        "ese intervalo. La curva confirma que el aplazamiento no debe usarse como mecanismo general para absorber "
        "presión de taller; debe reservarse para casos con bajo riesgo de diferimiento, señal débil o restricción "
        "material demostrable, y cada aplazamiento de un caso P1 debe registrar la restricción, el responsable, la "
        "nueva fecha y la exposición incremental."
    )
    defer_rows = "".join(
        f"<tr><td class='num'>{fmt_int(dias)} d</td>"
        f"<td class='num'>{fmt_pct(defer.loc[dias, 'prob_fallo_media'] * 100, 0)}</td>"
        f"<td class='num'>{fmt_dec(defer.loc[dias, 'downtime_total_h'], 0)} h</td>"
        f"<td class='num'>{fmt_money_m(defer.loc[dias, 'costo_total_eur'], 2)}</td></tr>"
        for dias in [0, 7, 14, 21, 30, 45]
    )
    d.add(
        "<table class='data'><thead><tr><th class='num'>Diferimiento</th><th class='num'>Prob. fallo media</th>"
        "<th class='num'>Indisponibilidad</th><th class='num'>Coste total aproximado</th></tr></thead><tbody>"
        + defer_rows + "</tbody></table>"
        "<p class='tbl-note'>Escenarios de diferimiento de la cola priorizada. Cada fila acumula el efecto de posponer "
        "la intervención el número de días indicado. Fuente: simulación de diferimiento.</p>"
    )
    d.callout(
        "Acción ejecutiva",
        "El CFO y la Dirección de Mantenimiento deben recalibrar costes, habilitación y valor de disponibilidad antes "
        "de someter la inversión en CBM a aprobación, e implantar una aprobación formal de los diferimientos P1 con el "
        "coste y las horas incrementales visibles en la decisión.",
        "warning",
    )

    # ----- 8. Riesgos ---------------------------------------------------- #
    d.h(2, "8. Riesgos, limitaciones y cautelas", anchor="sec-riesgos", section="Riesgos y limitaciones")
    d.p(
        "Los resultados son rigurosos dentro de un entorno sintético, pero no son evidencia de producción. Los datos "
        "reproducen relaciones plausibles entre deterioro, fallo, inspección y mantenimiento, pero no una red "
        "ferroviaria real. Las puntuaciones, umbrales y distribuciones demuestran arquitectura y lógica de decisión; no "
        "validan precisión externa, causalidad física ni impacto financiero. Esta distinción es la cautela central del "
        "informe y debe acompañar cualquier comunicación de sus cifras."
    )
    d.p(
        "Antes de un uso en producción deben recalibrarse los modelos de fallo, los costes, la disponibilidad, los "
        "tiempos de reparación, la capacidad, las ventanas operativas y las taxonomías con datos observados. La "
        "validación debe incluir validación retrospectiva temporal, estabilidad de la clasificación por flota y depósito, revisión de "
        "falsos positivos y, sobre todo, el resultado de las órdenes efectivamente ejecutadas, que es la única prueba "
        "de que la puntuación ordena casos cuya intervención reduce de verdad fallos e indisponibilidad."
    )
    production_gate_rows = [
        (
            "Datos reales integrados",
            "Sensores, órdenes, fallos, disponibilidad, repuestos y capacidad con linaje y responsabilidad funcional.",
            "Usar puntuaciones solo en piloto controlado; no automatizar órdenes.",
        ),
        (
            "Validación temporal",
            "Validación retrospectiva por familia, flota y depósito; estabilidad de clasificación; falsos positivos y falsos negativos.",
            "Ajustar umbrales por familia antes de escalar.",
        ),
        (
            "Economía calibrada",
            "Costes corporativos, valor de hora de servicio, coste de habilitación, inventario y capacitación.",
            "No aprobar CBM como caso de negocio de ahorro.",
        ),
        (
            "Modelo operativo aprobado",
            "RACI de excepción, autorizadores, trazabilidad de diferimientos y circuito alerta-orden-ejecución.",
            "Mantener recomendación como soporte de decisión, no como automatismo.",
        ),
    ]
    production_gate_html = "".join(
        f"<tr><td><strong>{escape(item)}</strong></td><td>{evidence}</td><td>{constraint}</td></tr>"
        for item, evidence, constraint in production_gate_rows
    )
    d.add(
        "<table class='data'><thead><tr><th>Puerta de producción</th><th>Evidencia exigida</th>"
        "<th>Restricción si no se cumple</th></tr></thead><tbody>"
        f"{production_gate_html}</tbody></table>"
        "<p class='tbl-note'>Criterios mínimos para pasar del prototipo analítico a uso operativo controlado.</p>"
    )
    d.callout(
        "Riesgo principal",
        "Convertir una demostración coherente en una política automática sin calibración con datos reales. La calidad "
        "interna del prototipo puede generar una confianza que los datos sintéticos no respaldan.",
        "danger",
    )
    d.h(3, "Cuatro interpretaciones incorrectas deben bloquearse")
    d.p(
        "Primera, tratar la vida remanente como fecha exacta de fallo en lugar de como bucket relativo de ventana. "
        "Segunda, interpretar el coste aproximado como un caso financiero cerrado y no como un instrumento para visualizar "
        "compensaciones. Tercera, confundir los pendientes físicos con el riesgo de diferimiento, que miden cosas distintas. "
        "Cuarta, asumir que la correlación de una señal con la puntuación identifica una causa física. Cada una de estas "
        "lecturas está explícitamente prohibida en los contratos de métricas, y mantener esa prohibición activa es "
        "parte del diseño, no una nota a pie de página."
    )
    d.p(
        "Existen además limitaciones de método que conviene declarar. La planificación es heurística y no garantiza un "
        "óptimo global; su valor está en mejorar la asignación, no en demostrar optimalidad. La inspección automática "
        "se evalúa con métricas aproximadas sobre cobertura sintética. La recomendación de depósito requiere validar "
        "competencias técnicas, disponibilidad de repuesto, logística y restricciones contractuales que el modelo no "
        "incorpora. Ninguna de estas limitaciones invalida las conclusiones operativas, pero todas acotan el grado de "
        "automatización admisible antes de la calibración."
    )

    # ----- 9. Recomendaciones ------------------------------------------- #
    d.h(2, "9. Recomendaciones y prioridades de acción", anchor="sec-recomendaciones", section="Recomendaciones")
    d.lead(
        "Las recomendaciones se ordenan por horizonte y se atan a la evidencia de las secciones anteriores. La "
        "secuencia separa lo que debe ejecutarse de inmediato de la calibración necesaria para decidir inversión."
    )
    d.h(3, "Prioridades para los próximos 30 días")
    d.p(
        f"Ejecutar la intervención de {top['unidad_id']} / {top['componente_id']} y revisar diariamente la cola de "
        f"{fmt_int(mv('high_deferral_risk_cases_count'))} casos con alto riesgo de diferimiento, registrando motivo y "
        "autorizador para cualquier excepción. Adoptar la heurística de 35 días como base inicial operativa controlada y "
        "medir semanalmente la accionabilidad, los pendientes por capacidad, los pendientes por repuesto, el riesgo "
        f"residual y el valor capturado. Abrir el rebalanceo en dos frentes: aliviar {m['top_depot_by_saturation']} "
        f"por saturación del corte y revisar {primary_bottleneck['deposito_id']} como cuello de botella de la "
        "planificación rediseñada, reasignando trabajo compatible a instalaciones con holgura."
    )
    d.p(
        "Crear un frente específico para pantógrafos y para los cinco modos de fallo más repetitivos, vinculando cada "
        "acción a una causa raíz, un responsable, una fecha objetivo y una evidencia posterior de reducción de "
        "reincidencia. Estas tres líneas (proteger la cabeza de cola, corregir la asignación de taller y reducir la "
        "repetición por familia) son las que capturan el valor de gestión sin necesidad de inversión adicional."
    )
    execution_rows = [
        (
            "0-14 días",
            "Dirección de Mantenimiento + Planificación",
            "Ejecutar P1/P2, proteger repuesto y bloquear excepciones no autorizadas.",
            f"{fmt_int(mv('high_deferral_risk_cases_count'))} casos de alto riesgo con revisión diaria.",
        ),
        (
            "15-30 días",
            "Planificación de Taller",
            "Adoptar programación a 35 días y publicar tablero semanal de restricciones.",
            f"Accionabilidad +{fmt_dec(actionable_delta_pp, 1)} p.p.; riesgo residual -{fmt_dec(residual_risk_delta_pp, 1)} p.p.",
        ),
        (
            "31-60 días",
            "Operaciones + Jefes de depósito",
            f"Rebalancear {m['top_depot_by_saturation']} / {primary_bottleneck['deposito_id']} y validar compatibilidad técnica.",
            f"{fmt_pct(primary_bottleneck_pending_share, 1)} de pendientes por capacidad concentrados en el cuello de botella.",
        ),
        (
            "61-120 días",
            "CFO + Fiabilidad",
            "Recalibrar costes, umbrales, RUL y valor de disponibilidad con datos reales.",
            f"Puerta CBM: valor validado >= € {fmt_int(mv('cbm_breakeven_value_per_service_hour_eur'))}/h o caso alternativo.",
        ),
    ]
    execution_html = "".join(
        f"<tr><td><strong>{escape(horizon)}</strong></td><td>{escape(owner)}</td><td>{action}</td><td>{control}</td></tr>"
        for horizon, owner, action, control in execution_rows
    )
    d.add(
        "<table class='data'><thead><tr><th>Horizonte</th><th>Responsable</th><th>Mandato</th><th>Control ejecutivo</th>"
        "</tr></thead><tbody>" + execution_html + "</tbody></table>"
        "<p class='tbl-note'>Agenda de ejecución propuesta para comité. Los controles son métricas de gobierno, no "
        "promesas de impacto financiero sin calibración real.</p>"
    )
    d.callout(
        "Orden de prioridad",
        "Prioridad 1: proteger servicio en la cabeza de la cola. Prioridad 2: corregir la asignación de taller. "
        "Prioridad 3: reducir la repetición de fallos por familia.",
        "accent",
    )
    d.h(3, "Decisiones de 60 a 120 días")
    d.p(
        "Calibrar el riesgo de fallo y los buckets de vida remanente con histórico real, usando cortes temporales y "
        "métricas por familia, e incorporar la respuesta a la intervención para comprobar si la puntuación ordena casos que "
        "efectivamente reducen fallos e indisponibilidad. Construir el caso económico con costes corporativos "
        "auditables, valorando de forma explícita las horas de servicio preservadas, las correctivas evitadas, el coste "
        "de habilitación, el inventario, la capacitación y el riesgo de ejecución. Sin ese paso, la estrategia CBM no "
        "debe presentarse como ahorro."
    )
    d.p(
        "Evaluar la optimización matemática de la planificación solo después de estabilizar reglas, datos y restricciones. "
        "Un optimizador construido sobre supuestos débiles produce precisión aparente, no una mejor decisión, y "
        "trasladaría a la organización una falsa sensación de rigor. La hoja de ruta, en síntesis, ejecuta de inmediato "
        "lo que la evidencia operativa ya soporta y reserva la inversión para cuando exista calibración financiera."
    )
    d.add(
        """
<div class="roadmap">
  <div class="phase"><div class="phase-head danger">0-30 días</div><ul>
    <li>Ejecutar la cola P1 y blindar los casos de alto riesgo de diferimiento</li>
    <li>Adoptar la ventana de programación de 35 días</li>
    <li>Registrar excepciones, causas y autorizadores</li></ul></div>
  <div class="phase"><div class="phase-head accent">31-60 días</div><ul>
    <li>Rebalancear carga entre depósitos desde el más saturado</li>
    <li>Plan de causa raíz para pantógrafos y modos repetitivos</li>
    <li>Medir restricciones residuales tras el rediseño</li></ul></div>
  <div class="phase"><div class="phase-head warning">61-120 días</div><ul>
    <li>Calibrar costes y valor de disponibilidad con datos reales</li>
    <li>Validar riesgo y vida remanente con cortes temporales</li>
    <li>Preparar la decisión de inversión en CBM</li></ul></div>
</div>
<p class="tbl-note">El comité ejecutivo debe revisar cinco métricas mensuales: casos P1 ejecutados, riesgo residual,
utilización por depósito, reincidencia por modo y diferencial económico recalibrado.</p>
"""
    )

    # ----- Apéndice ------------------------------------------------------ #
    d.h(2, "Apéndice. Métricas, fuentes e interpretación", anchor="sec-apendice", section="Apéndice")
    d.p(
        "La tabla siguiente recoge las métricas oficiales utilizadas para comunicar resultados, con su valor y su "
        "fuente de verdad. Las definiciones completas, los filtros y las reglas de agregación permanecen en el registro "
        "procesado de métricas, que es el origen único del que leen el panel de control, el README y este informe."
    )
    source_label = {
        "fleet_availability_pct": "Tabla analítica semanal de flota",
        "mtbf_proxy_hours": "Tabla analítica semanal de flota",
        "mttr_proxy_hours": "Tabla analítica semanal de flota",
        "high_risk_units_count": "Modelo de riesgo de unidad",
        "backlog_physical_items_count": "Registro de pendientes",
        "backlog_overdue_items_count": "Registro de pendientes",
        "backlog_critical_physical_count": "Registro de pendientes",
        "high_deferral_risk_cases_count": "Modelo de priorización",
        "cbm_vs_reactiva_availability_pp": "Simulación estratégica",
        "cbm_operational_savings_eur": "Simulación estratégica",
        "cbm_breakeven_value_per_service_hour_eur": "Simulación estratégica",
        "deferral_cost_delta_14d_eur": "Simulación de diferimiento",
        "deferral_downtime_delta_14d_h": "Simulación de diferimiento",
        "mean_depot_saturation_pct": "Tabla analítica de presión de talleres",
        "top_depot_saturation_pct": "Tabla analítica de presión de talleres",
    }
    metric_selection = [
        ("fleet_availability_pct", "Disponibilidad media de flota", lambda v: fmt_pct(v, 2)),
        ("mtbf_proxy_hours", "Tiempo medio entre fallos (aproximado)", lambda v: f"{fmt_dec(v, 1)} h"),
        ("mttr_proxy_hours", "Tiempo medio de reparación (aproximado)", lambda v: f"{fmt_dec(v, 1)} h"),
        ("high_risk_units_count", "Unidades de alto riesgo", fmt_int),
        ("backlog_physical_items_count", "Pendientes físicos", fmt_int),
        ("backlog_overdue_items_count", "Pendientes vencidos", fmt_int),
        ("backlog_critical_physical_count", "Pendientes críticos físicos", fmt_int),
        ("high_deferral_risk_cases_count", "Casos con alto riesgo de diferimiento", fmt_int),
        ("mean_depot_saturation_pct", "Saturación media de depósito", lambda v: fmt_pct(v, 1)),
        ("top_depot_saturation_pct", "Saturación del depósito más exigido", lambda v: fmt_pct(v, 1)),
        ("cbm_vs_reactiva_availability_pp", "Mejora de disponibilidad de CBM", lambda v: f"+{fmt_dec(v, 2)} p.p."),
        ("cbm_operational_savings_eur", "Diferencial neto de CBM frente a reactiva", fmt_money_m),
        (
            "cbm_breakeven_value_per_service_hour_eur",
            "Valor sombra de equilibrio de CBM por hora de servicio",
            lambda v: f"€ {fmt_int(v)}/h",
        ),
        ("deferral_cost_delta_14d_eur", "Coste incremental al diferir 14 días", lambda v: fmt_money_m(v, 2)),
        ("deferral_downtime_delta_14d_h", "Indisponibilidad incremental al diferir 14 días", lambda v: f"{fmt_dec(v, 0)} h"),
    ]
    metric_rows = "".join(
        f"<tr><td>{escape(label)}</td><td class='num'>{formatter(mv(mid))}</td>"
        f"<td>{escape(source_label[mid])}</td></tr>"
        for mid, label, formatter in metric_selection
    )
    d.add(
        "<table class='data'><thead><tr><th>Métrica</th><th class='num'>Valor</th><th>Fuente de verdad</th></tr></thead>"
        f"<tbody>{metric_rows}</tbody></table>"
    )
    d.h(3, "Guía de interpretación", anchor="guia-interpretacion")
    d.p(
        "Cada indicador tiene una función de decisión y una interpretación explícitamente bloqueada. Esta disciplina "
        "debe mantenerse en cualquier extensión del panel de control o del informe, porque es la que impide que una cifra útil "
        "para un fin se utilice para otro que no soporta."
    )
    method_rows = [
        ("Salud", "Condición técnica actual del componente", "No es probabilidad de fallo"),
        ("Riesgo a 30 días", "Ordenar propensión a fallo a corto plazo", "No prueba causalidad física"),
        ("Vida remanente", "Bucket relativo de ventana de intervención", "No es fecha exacta de fallo"),
        ("Pendientes físicos", "Carga de mantenimiento pendiente real", "No equivale a riesgo de diferimiento"),
        ("Riesgo de diferimiento", "Daño esperado de aplazar una decisión", "No equivale a pendientes críticos"),
        ("Saturación de taller", "Uso relativo de la capacidad del depósito", "No mide productividad"),
        ("Valor estratégico", "Comparar compensaciones entre estrategias", "No es P&L ni ahorro contractual"),
        (
            "Valor sombra de equilibrio",
            "Valorar una hora de servicio en el comité",
            "No es una cifra de mercado validada",
        ),
    ]
    method_html = "".join(
        f"<tr><td><strong>{escape(a)}</strong></td><td>{escape(b)}</td><td>{escape(c)}</td></tr>"
        for a, b, c in method_rows
    )
    d.add(
        "<table class='data'><thead><tr><th>Indicador</th><th>Uso correcto</th><th>Interpretación bloqueada</th></tr></thead>"
        f"<tbody>{method_html}</tbody></table>"
    )
    d.p(
        f"Conclusión: ejecutar la cola de intervención basada en riesgo, corregir la asignación de capacidad y condicionar "
        f"la inversión en CBM a una calibración económica honesta. Las cifras que sostienen esa decisión proceden de "
        f"{fmt_int(mv('n_componentes'))} componentes críticos, {fmt_int(mv('n_unidades'))} unidades y "
        f"{len(checks)} controles de gobernanza sin bloqueos activos, y todas son reproducibles desde el flujo."
    )
    d.add("</section>")

    # ----- Ensamblado HTML + CSS ---------------------------------------- #
    fonts_css = build_fonts_css()
    html = f"""<!DOCTYPE html><html lang="es"><head><meta charset="utf-8"/>
<style>{fonts_css}{base_css()}</style></head><body>{d.html()}</body></html>"""

    REPORT.parent.mkdir(parents=True, exist_ok=True)
    HTML(string=html, base_url=str(ROOT)).write_pdf(str(REPORT))
    print(f"Informe generado: {REPORT}")


def build_fonts_css() -> str:
    faces = []
    mapping = [
        ("IBM Plex Sans", "IBMPlexSans-Regular.ttf", 400, "normal"),
        ("IBM Plex Sans", "IBMPlexSans-SemiBold.ttf", 600, "normal"),
        ("IBM Plex Sans", "IBMPlexSans-Bold.ttf", 700, "normal"),
        ("IBM Plex Mono", "IBMPlexMono-Medium.ttf", 500, "normal"),
        ("IBM Plex Mono", "IBMPlexMono-SemiBold.ttf", 600, "normal"),
    ]
    for family, fname, weight, style in mapping:
        path = FONT_DIR / fname
        if path.exists():
            data = b64_font(path)
            faces.append(
                f"@font-face{{font-family:'{family}';font-weight:{weight};font-style:{style};"
                f"src:url(data:font/ttf;base64,{data}) format('truetype');}}"
            )
    return "".join(faces)


def base_css() -> str:
    return """
:root{
  --ink:#172033; --muted:#5b6577; --light:#e6e9ef; --panel:#f5f7fa;
  --accent:#2463d4; --danger:#b42318; --warning:#b76e00; --positive:#177245;
}
@page{
  size:A4; margin:20mm 18mm 18mm 18mm;
  @top-left{ content:"Inteligencia de mantenimiento ferroviario"; font-family:'IBM Plex Sans'; font-size:7pt; color:#98a2b3; font-weight:600; letter-spacing:.04em; }
  @top-right{ content:string(section); font-family:'IBM Plex Sans'; font-size:7pt; color:#98a2b3; font-weight:600; text-transform:uppercase; letter-spacing:.04em; }
  @bottom-left{ content:"Inteligencia de mantenimiento ferroviario"; font-family:'IBM Plex Sans'; font-size:7pt; color:#98a2b3; }
  @bottom-right{ content:counter(page) " / " counter(pages); font-family:'IBM Plex Mono'; font-size:7.5pt; color:#98a2b3; }
}
@page cover{ margin:0; @top-left{content:""} @top-right{content:""} @bottom-left{content:""} @bottom-right{content:""} }
@page toc{ @top-left{content:""} @top-right{content:""} }
*{box-sizing:border-box;}
html{ font-family:'IBM Plex Sans'; color:var(--ink); font-size:10pt; line-height:1.62; }
body{ margin:0; }
p{ margin:0 0 9pt 0; text-align:justify; hyphens:auto; }
h2{ string-set:section attr(data-section); }

/* ---------- Portada ---------- */
.cover{ page:cover; height:297mm; width:210mm; position:relative; color:var(--ink); }
.cover-band{ background:var(--ink); color:#fff; padding:34mm 20mm 16mm 20mm; height:120mm; }
.cover-band .kicker{ color:#8fb4ff; font-size:9pt; font-weight:600; letter-spacing:.18em; text-transform:uppercase; }
.cover-title{ font-size:34pt; font-weight:700; line-height:1.05; margin:14mm 0 6mm 0; max-width:150mm; }
.cover-sub{ font-size:13pt; line-height:1.5; color:#cdd9ef; max-width:140mm; text-align:left; }
.cover-meta{ padding:14mm 20mm 0 20mm; }
.cover-meta-row{ display:flex; justify-content:space-between; border-bottom:.4pt solid var(--light); padding:5pt 0; }
.cover-meta-row span{ font-size:8pt; font-weight:600; letter-spacing:.06em; text-transform:uppercase; color:var(--muted); }
.cover-meta-row strong{ font-size:10.5pt; color:var(--ink); font-weight:600; }
.cover-decision{ margin:12mm 20mm 0 20mm; padding:7mm; background:var(--panel); border-left:3pt solid var(--accent); }
.cover-decision p{ font-size:11.5pt; font-weight:600; line-height:1.5; margin:0; text-align:left; }

/* ---------- Índice ---------- */
.toc{ page:toc; padding-top:6mm; break-after:page; }
.toc-head{ font-size:24pt; font-weight:700; margin:0 0 8mm 0; }
.toc-list{ list-style:none; padding:0; margin:0; counter-reset:toc; }
.toc-list li{ margin:0 0 4.5mm 0; }
.toc-list a{ display:flex; align-items:baseline; text-decoration:none; color:var(--ink); }
.toc-title{ font-size:11pt; font-weight:600; }
.toc-dots{ flex:1; border-bottom:.5pt dotted #c2cad6; margin:0 4pt 0 6pt; transform:translateY(-2pt); }
.toc-list a::after{ content:target-counter(attr(href), page); font-family:'IBM Plex Mono'; font-size:9.5pt; color:var(--muted); }

/* ---------- Cuerpo ---------- */
/* Las secciones fluyen de forma continua: un salto de página
   forzado en cada h2 desperdicia espacio cuando una sección termina temprano.
   break-after:avoid basta para que un título nunca quede huérfano al pie de página. */
.body h2{ font-size:19pt; font-weight:700; color:var(--ink); margin:14mm 0 3mm 0; padding-bottom:2.5mm;
  border-bottom:1.6pt solid var(--ink); break-after:avoid; }
.body h3{ font-size:12.5pt; font-weight:600; color:var(--accent); margin:7mm 0 2.5mm 0; break-after:avoid; }
#guia-interpretacion{ break-before:page; }
.lead{ font-size:11.5pt; line-height:1.6; color:var(--ink); font-weight:500; margin-bottom:7pt;
  border-left:2.5pt solid var(--accent); padding-left:5mm; text-align:left; }

/* Tarjetas de indicadores */
.kpi-row{ display:flex; gap:4mm; margin:5mm 0 6mm 0; }
.kpi{ flex:1; background:var(--panel); border:.5pt solid var(--light); border-top:2.5pt solid var(--muted); padding:4mm; }
.kpi.danger{ border-top-color:var(--danger); } .kpi.positive{ border-top-color:var(--positive); }
.kpi.warning{ border-top-color:var(--warning); }
.kpi-val{ font-family:'IBM Plex Mono'; font-size:16pt; font-weight:600; color:var(--ink); line-height:1.1; white-space:nowrap; }
.kpi-lab{ font-size:7.8pt; color:var(--muted); margin-top:2mm; line-height:1.3; }

/* Callouts */
.callout{ background:var(--panel); border:.5pt solid var(--light); border-left:3pt solid var(--accent);
  padding:4mm 5mm; margin:4mm 0 6mm 0; break-inside:avoid; }
.callout.danger{ border-left-color:var(--danger); } .callout.warning{ border-left-color:var(--warning); }
.callout.positive{ border-left-color:var(--positive); }
.callout-label{ display:block; font-size:7.5pt; font-weight:700; letter-spacing:.07em; text-transform:uppercase;
  color:var(--accent); margin-bottom:2mm; }
.callout.danger .callout-label{ color:var(--danger); } .callout.warning .callout-label{ color:var(--warning); }
.callout.positive .callout-label{ color:var(--positive); }
.callout-text{ font-size:9.6pt; line-height:1.5; color:var(--ink); }

/* Figuras */
figure{ margin:5mm 0 5mm 0; break-inside:avoid; }
figure img{ width:100%; height:auto; display:block; border:.5pt solid var(--light); }
figcaption{ font-size:8pt; color:var(--muted); line-height:1.45; margin-top:2.5mm;
  border-left:2pt solid var(--light); padding-left:3mm; }
figcaption strong{ color:var(--ink); }
figcaption .src{ color:#98a2b3; }

/* Tablas */
table.data{ width:100%; border-collapse:collapse; margin:4mm 0 2mm 0; font-size:8.5pt; break-inside:avoid; }
table.data thead th{ background:var(--ink); color:#fff; font-weight:600; text-align:left; padding:2.5mm 3mm;
  font-size:8pt; }
table.data th.num, table.data td.num{ text-align:right; font-family:'IBM Plex Mono'; }
table.data tbody td{ padding:2.2mm 3mm; border-bottom:.5pt solid var(--light); color:var(--muted); }
table.data tbody tr:nth-child(even){ background:#fafbfc; }
table.data td.tier{ font-family:'IBM Plex Mono'; font-weight:600; color:#fff; text-align:center; }
td.tier.p1{ background:var(--danger); } td.tier.p2{ background:var(--warning); } td.tier.p3{ background:var(--accent); }
.tbl-note{ font-size:7.8pt; color:var(--muted); margin-top:1mm; text-align:left; }

/* Roadmap */
.roadmap{ display:flex; gap:4mm; margin:5mm 0 2mm 0; break-inside:avoid; }
.phase{ flex:1; background:var(--panel); border:.5pt solid var(--light); }
.phase-head{ color:#fff; font-family:'IBM Plex Mono'; font-size:8.5pt; font-weight:600; padding:2.5mm 3mm; }
.phase-head.danger{ background:var(--danger); } .phase-head.accent{ background:var(--accent); }
.phase-head.warning{ background:var(--warning); }
.phase ul{ margin:0; padding:3mm 4mm 3mm 7mm; }
.phase li{ font-size:8.3pt; line-height:1.4; color:var(--ink); margin-bottom:2mm; }
"""


if __name__ == "__main__":
    build()
