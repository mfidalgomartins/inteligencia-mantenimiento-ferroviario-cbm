# Ensamblaje Final del Proyecto

## Estado
- Pipeline ejecutada end-to-end con éxito (`.venv/bin/python -m src.run_pipeline`).
- Tests ejecutados: `29 passed` (`.venv/bin/pytest -q`).

## Estructura final de repositorio
- `data/raw/`
- `data/processed/`
- `sql/`
- `src/`
- `notebooks/`
- `outputs/charts/`
- `outputs/dashboard/`
- `outputs/reports/`
- `docs/`
- `tests/`

## Datos generados (raw)
- `flotas.csv`: 6 filas
- `unidades.csv`: 144 filas
- `depositos.csv`: 10 filas
- `componentes_criticos.csv`: 1,152 filas
- `sensores_componentes.csv`: 2,457,248 filas
- `inspecciones_automaticas.csv`: 253,336 filas
- `eventos_mantenimiento.csv`: 16,971 filas
- `fallas_historicas.csv`: 10,427 filas
- `alertas_operativas.csv`: 394,102 filas
- `intervenciones_programadas.csv`: 16,955 filas
- `disponibilidad_servicio.csv`: 105,264 filas
- `asignacion_servicio.csv`: 105,264 filas
- `backlog_mantenimiento.csv`: 87,266 filas
- `parametros_operativos_contexto.csv`: 11,696 filas
- `escenarios_mantenimiento.csv`: 2,193 filas

## Tablas analíticas creadas (processed)
- `component_day_features.csv`: 768,288 filas
- `unit_day_features.csv`: 105,264 filas
- `fleet_week_features.csv`: 630 filas
- `workshop_priority_features.csv`: 1,152 filas
- `component_health_score.csv`: 1,152 filas
- `component_failure_risk_score.csv`: 1,152 filas
- `component_rul_estimate.csv`: 1,152 filas
- `unit_unavailability_risk_score.csv`: 144 filas
- `workshop_priority_table.csv`: 1,152 filas
- `workshop_scheduling_recommendation.csv`: 1,152 filas
- `comparativo_estrategias.csv`: 3 filas

## Dashboard HTML final
- `outputs/dashboard/index.html` (Chart.js, filtros globales, tabla interactiva, sección de decisión ejecutiva).

## Hallazgos principales
- Disponibilidad media: 90.45%
- Unidades alto riesgo: 144
- Backlog físico: 2,054
- Backlog vencido: 2,000
- Backlog crítico físico: 1,957
- Casos de alto riesgo de diferimiento: 0
- Ahorro operativo proxy CBM vs reactiva: 66,941,820 EUR
- Primera entrada recomendada: unidad `UNI0057`, componente `COMP000454` (score=91.4)

## Recomendaciones ejecutivas
- Separar gestión de backlog físico (`backlog_físico/vencido/crítico`) de riesgo de diferimiento (`deferral_risk_score`).
- Priorizar cola de intervención por `intervention_priority_score` + `deferral_risk_score`.
- Escalar CBM en familias críticas y depósitos con saturación persistente.
- Recalibrar umbrales con histórico real para producción.

## Resumen de validación
- Estado: **BLOCKED**
- Checks aprobados: 107/110
- Publish blockers activos: 3
- Issues críticos/alta prioridad abiertos: degeneración RUL, colapso de confidence flag, desviación cross-output de scores
- Reporte completo: `outputs/reports/validation_report.md`

## Limitaciones
- Datos sintéticos y costes proxy; no sustituyen benchmark contractual real.
- Scheduling heurístico (no optimización matemática exacta).

## Próximos pasos
1. Integrar datos reales de órdenes, fallas y telemetría.
2. Añadir optimización MILP/CP para secuenciación de taller.
3. Instrumentar monitor de drift de sensores y de reglas de alerta.

## Publicación en GitHub (sugerencia exacta)
1. Crear repositorio público con nombre `railway-cbm-intelligence-system`.
2. Subir estructura completa y verificar que `outputs/dashboard/index.html` abre correctamente.
3. Incluir en la descripción del repo: problema de negocio, métricas clave y enlace al dashboard.
4. Añadir captura del dashboard y 3 gráficos clave en el README.
5. Etiquetar release `v1.0.0-portfolio` con changelog corto.
