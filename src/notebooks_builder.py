from __future__ import annotations

import json

from src.config import NOTEBOOKS_DIR


def _build_notebook(cells: list[dict], file_name: str) -> None:
    notebook = {
        "cells": cells,
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python", "version": "3"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    (NOTEBOOKS_DIR / file_name).write_text(json.dumps(notebook, ensure_ascii=False, indent=2), encoding="utf-8")


def _md(text: str) -> dict:
    return {"cell_type": "markdown", "metadata": {}, "source": [line + "\n" for line in text.strip("\n").split("\n")]}


def _code(code: str) -> dict:
    return {"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [], "source": [line + "\n" for line in code.strip("\n").split("\n")]}


def build_notebooks() -> None:
    NOTEBOOKS_DIR.mkdir(parents=True, exist_ok=True)

    common_setup = _code(
        """
import pandas as pd
import numpy as np
from pathlib import Path

root = Path("..").resolve()
proc = root / "data" / "processed"
rep = root / "outputs" / "reports"
charts = root / "outputs" / "charts"
        """
    )

    nb1 = [
        _md(
            """
# 01 - Exploración y Auditoría de Datos

## Pregunta analítica
¿La base sintética tiene calidad suficiente para soportar scoring, RUL, priorización y decisiones de taller?

## Hipótesis
La mayor parte de problemas de credibilidad no estarán en nulls/duplicados básicos, sino en coherencia semántica y distribución de señales.
            """
        ),
        common_setup,
        _code(
            """
summary = pd.read_csv(rep / "data_profile_summary.csv")
quality = pd.read_csv(rep / "data_quality_checks.csv")
val = pd.read_csv(rep / "validation_checks_detailed.csv")
summary.head(20), quality.head(20), val.head(20)
            """
        ),
        _md(
            """
## Metodología
1. Profiling estructural por tabla (grain, claves candidatas, cobertura temporal).
2. Validaciones por capa (raw, marts, features, scores, recomendaciones, dashboard, narrativa).
3. Priorización por severidad con bloqueadores de publicación.
            """
        ),
        _code(
            """
issues = pd.read_csv(rep / "issues_found.csv")
issues.sort_values(["publish_blocker", "severity"], ascending=[False, True]).head(30)
            """
        ),
        _code(
            """
controls = pd.read_csv(rep / "validation_control_matrix.csv")
controls
            """
        ),
        _md(
            """
## Interpretación
- Un control "OK" no implica valor operativo: se revisan saturación, entropía y colapso de clases.
- Los checks críticos que fallen bloquean publicación.

## Limitaciones
- Entorno sintético: coherencia estadística no equivale a causalidad real.
- La validación no sustituye calibración con histórico contractual.

## Decisión resultante
Publicar únicamente si no hay bloqueadores críticos y la señal mantiene discriminación operativa.
            """
        ),
    ]

    nb2 = [
        _md(
            """
# 02 - Degradación, Riesgo y RUL

## Pregunta analítica
¿Qué señales explican mejor el deterioro y qué tan útil es el RUL para decidir ventanas de intervención?

## Hipótesis
La señal útil surge de combinar deterioro + estrés + historial (falla/mantenimiento), no de un único sensor.
            """
        ),
        common_setup,
        _code(
            """
scores = pd.read_csv(proc / "component_model_scores.csv")
rul = pd.read_csv(proc / "component_rul_estimate.csv")
det = pd.read_csv(proc / "risk_signal_determinants.csv")
scores.head(), rul.head(), det
            """
        ),
        _code(
            """
latest = scores.sort_values("fecha").groupby(["unidad_id","componente_id"]).tail(1)
latest["risk_bucket"] = pd.cut(latest["component_failure_risk_score"], bins=[0,40,60,80,100], labels=["bajo","medio","alto","critico"], include_lowest=True)
latest.groupby("risk_bucket", observed=True).size().reset_index(name="n")
            """
        ),
        _code(
            """
rul_dist = pd.read_csv(proc / "rul_distribution_before_after.csv")
rul_fam = pd.read_csv(proc / "rul_family_discrimination_before_after.csv")
rul_dist, rul_fam
            """
        ),
        _md(
            """
## Interpretación
- Health score y deterioration index no son equivalentes: uno resume estado, el otro intensidad de daño.
- RUL se usa por buckets de planificación, no como fecha exacta de fallo.

## Limitaciones
- El backtest de falla está condicionado por el entorno sintético.
- La discriminación por familia mejora, pero requiere calibración por material real.

## Decisión resultante
Usar riesgo + RUL + criticidad para clasificar intervención inmediata vs próxima ventana.
            """
        ),
    ]

    nb3 = [
        _md(
            """
# 03 - Priorización y Scheduling de Taller

## Pregunta analítica
¿Qué intervenir primero, dónde y con qué estado de programación bajo capacidad limitada?

## Hipótesis
Una cola jerárquica con aging y ajuste por capacidad mejora ejecutabilidad frente al colapso en `pendiente_capacidad`.
            """
        ),
        common_setup,
        _code(
            """
prior = pd.read_csv(proc / "workshop_priority_table.csv")
plan = pd.read_csv(proc / "workshop_scheduling_recommendation.csv")
before_after = pd.read_csv(proc / "scheduling_before_after_metrics.csv")
status = pd.read_csv(proc / "scheduling_status_distribution.csv")
prior.head(), plan.head(), before_after, status
            """
        ),
        _code(
            """
top = prior.sort_values("intervention_priority_score", ascending=False).head(20)
top[["unidad_id","componente_id","deposito_recomendado","intervention_priority_score","deferral_risk_score","decision_type","reason_main"]]
            """
        ),
        _code(
            """
plan.groupby(["estado_intervencion","deposito_recomendado"], observed=True).size().reset_index(name="n").sort_values("n", ascending=False).head(30)
            """
        ),
        _md(
            """
## Interpretación
- No toda prioridad alta es programable inmediata: capacidad, ventana y conflicto operativo mandan.
- `pendiente_capacidad` se monitorea junto con riesgo residual no atendido.

## Limitaciones
- Heurístico, no optimización global.
- No modela explícitamente restricciones de repuesto real.

## Decisión resultante
Aplicar secuencia sugerida y escalar conflictos con alto riesgo de diferimiento.
            """
        ),
    ]

    nb4 = [
        _md(
            """
# 04 - Estrategias de Mantenimiento e Impacto de Diferimiento

## Pregunta analítica
¿Cuándo conviene CBM frente a reactivo/preventivo rígido y cuál es el coste de diferir?

## Hipótesis
CBM no gana siempre: depende de presión de taller, precisión de detección temprana y coste de indisponibilidad.
            """
        ),
        common_setup,
        _code(
            """
comp = pd.read_csv(proc / "comparativo_estrategias.csv")
sens = pd.read_csv(proc / "comparativo_estrategias_sensibilidad.csv")
ranges = pd.read_csv(proc / "comparativo_estrategias_value_ranges.csv")
defer = pd.read_csv(proc / "impacto_diferimiento_resumen.csv")
comp, ranges, defer
            """
        ),
        _code(
            """
sens.groupby(["escenario","estrategia"], observed=True)["ahorro_neto_vs_reactiva_eur"].median().reset_index().sort_values(["escenario","ahorro_neto_vs_reactiva_eur"], ascending=[True,False]).head(20)
            """
        ),
        _code(
            """
before_after = pd.read_csv(proc / "strategy_comparison_before_after.csv")
before_after
            """
        ),
        _md(
            """
## Interpretación
- Se separa output observado de hipótesis estructural/proxy económico.
- El valor CBM se reporta en rangos plausibles (no en punto único sobrevendido).

## Limitaciones
- Costes y ahorro en proxy.
- Sensibilidades OFAT no capturan toda interacción no lineal.

## Decisión resultante
Escalar CBM donde el downside sea acotado y el upside operativo supere coste incremental.
            """
        ),
    ]

    _build_notebook(nb1, "01_exploracion_y_auditoria.ipynb")
    _build_notebook(nb2, "02_degradacion_riesgo_rul.ipynb")
    _build_notebook(nb3, "03_priorizacion_y_scheduling.ipynb")
    _build_notebook(nb4, "04_estrategias_y_diferimiento.ipynb")

    # Compatibilidad con referencias previas
    _build_notebook(nb2, "sistema_mantenimiento_ferroviario_principal.ipynb")
    _build_notebook(nb3, "priorizacion_y_scheduling_taller.ipynb")


if __name__ == "__main__":
    build_notebooks()
