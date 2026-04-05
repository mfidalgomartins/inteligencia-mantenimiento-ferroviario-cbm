# Modeling Framework | Salud, Riesgo y RUL

## 1) Degradation scoring basado en reglas
- Inputs: `deterioration_index`, `degradation_velocity`, `inspection_defect_score_recent`, `critical_alerts_count`, `backlog_exposure_flag`.
- Lógica: penalización aditiva interpretable con límites [0,100].
- Supuesto: deterioro acumulado y alertas críticas elevan riesgo de forma no lineal pero acotable.
- Limitación: no estima física de daño específica por fabricante/componente.
- Utilidad operativa: priorización rápida y trazable por componente.

## 2) Failure risk scoring interpretable
- Inputs: salud, deterioro, velocidad de degradación, estrés operativo, historial de fallas, exposición de backlog.
- Lógica: combinación lineal + sigmoide para `prob_fallo_30d`.
- Supuesto: la probabilidad depende de degradación reciente y carga operacional.
- Limitación: calibración sobre dato sintético; requiere recalibración con histórico real.
- Utilidad operativa: ranking de riesgo para decidir entrada a taller.

## 3) Early warning rules
- Inputs: `prob_fallo_30d`, `component_health_score`, confianza de señal y criticidad operacional.
- Lógica: jerarquía de decisión no destructiva con 7 categorías de acción.
- Supuesto: umbrales conservadores para minimizar fallas no detectadas.
- Limitación: trade-off precision/recall depende de ventanas de alerta elegidas.

## 4) RUL proxy
- Inputs: tendencia de salud 60 días + distancia a umbral técnico.
- Lógica: extrapolación lineal con tope de 365 días y banda de confianza.
- Supuesto: degradación localmente cuasi-lineal en ventana corta.
- Limitación: puede infraestimar mejoras tras mantenimiento mayor.

## 5) Riesgo de indisponibilidad por unidad
- Inputs: riesgo de componentes, criticidad de servicio, backlog, impacto de servicio y sustitución requerida.
- Lógica: agregación ponderada al nivel unidad para `unit_unavailability_risk_score`.
- Utilidad operativa: secuenciar intervenciones con impacto en servicio.