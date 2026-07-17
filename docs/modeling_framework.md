# Modelado de Salud, Riesgo y RUL

## 1) Puntuación de degradación basada en reglas
- Entradas: `deterioration_index`, `degradation_velocity`, `inspection_defect_score_recent`, `critical_alerts_count`, `backlog_exposure_flag`.
- Lógica: penalización aditiva interpretable con límites [0,100].
- Supuesto: deterioro acumulado y alertas críticas elevan riesgo de forma no lineal pero acotable.
- Limitación: no estima física de daño específica por fabricante/componente.
- Utilidad operativa: priorización rápida y trazable por componente.

## 2) Puntuación interpretable de riesgo de fallo
- Entradas: salud, deterioro, velocidad de degradación, estrés operativo, historial de fallas, exposición a pendientes.
- Lógica: señal ponderada, ranking por familia/global y transformación acotada para `prob_fallo_30d`.
- Semántica: pese al nombre histórico de la columna, es una puntuación no calibrada en [0,1], no una probabilidad operacional.
- Supuesto: la propensión al fallo aumenta con degradación reciente y carga operacional.
- Limitación: evaluada sobre dato sintético; exige calibración temporal y validación fuera de muestra con histórico real.
- Utilidad operativa: clasificación de riesgo para decidir entrada a taller.

## 3) Reglas de alerta temprana
- Entradas: `prob_fallo_30d`, `component_health_score`, confianza de señal y criticidad operacional.
- Lógica: jerarquía de decisión no destructiva con 7 categorías de acción.
- Supuesto: umbrales conservadores para minimizar fallas no detectadas.
- Limitación: la compensación entre precisión y cobertura depende de las ventanas de alerta elegidas.

## 4) RUL aproximado
- Entradas: salud actual, deterioro, velocidad, estrés, restauración, repetitividad y alertas.
- Lógica: daño diario efectivo no lineal y umbrales específicos por familia técnica.
- Supuesto: las señales observadas resumen la trayectoria de degradación relevante.
- Limitación: ventana relativa de intervención; no estima una fecha física de fallo calibrada.

## 5) Riesgo de indisponibilidad por unidad
- Entradas: riesgo de componentes, criticidad de servicio, pendientes, impacto de servicio y sustitución requerida.
- Lógica: agregación ponderada al nivel unidad para `unit_unavailability_risk_score`.
- Utilidad operativa: secuenciar intervenciones con impacto en servicio.
