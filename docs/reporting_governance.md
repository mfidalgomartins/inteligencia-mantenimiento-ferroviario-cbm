# Reporting Governance

## Objetivo
Evitar desincronización entre narrativa y resultados: todas las cifras ejecutivas se generan desde métricas oficiales (SSOT).

## Artefactos narrativos bajo gobierno
- README.md
- docs/memo_ejecutivo_es.md
- outputs/reports/memo_ejecutivo_es.md
- outputs/reports/summary_blocks.md
- outputs/dashboard/index.html (KPIs + bloque de decisión)

## Single Source of Truth
Archivo oficial: `data/processed/narrative_metrics_official.csv`

## Métricas Narrativas Oficializadas
| metric_id                           | label                                                         | unit        | source_of_truth                                                                     | window_definition                                    | filter_definition                                                             | aggregation_definition                                       |
|:------------------------------------|:--------------------------------------------------------------|:------------|:------------------------------------------------------------------------------------|:-----------------------------------------------------|:------------------------------------------------------------------------------|:-------------------------------------------------------------|
| as_of_ts                            | as_of_ts                                                      | meta        | pipeline_runtime                                                                    | nan                                                  | nan                                                                           | nan                                                          |
| coverage_start                      | coverage_start                                                | meta        | pipeline_runtime                                                                    | nan                                                  | nan                                                                           | nan                                                          |
| coverage_end                        | coverage_end                                                  | meta        | pipeline_runtime                                                                    | nan                                                  | nan                                                                           | nan                                                          |
| fleet_availability_pct              | Disponibilidad media de flota                                 | pct         | data/processed/fleet_week_features.csv                                              | histórico completo semanal disponible                | todos los registros válidos                                                   | mean(availability_rate) * 100                                |
| mtbf_proxy_hours                    | MTBF proxy                                                    | hours       | data/processed/fleet_week_features.csv                                              | histórico completo semanal disponible                | todos los registros válidos                                                   | mean(mtbf_proxy)                                             |
| mttr_proxy_hours                    | MTTR proxy                                                    | hours       | data/processed/fleet_week_features.csv                                              | histórico completo semanal disponible                | todos los registros válidos                                                   | mean(mttr_proxy)                                             |
| high_risk_units_count               | Unidades de alto riesgo                                       | count       | data/processed/unit_unavailability_risk_score.csv                                   | snapshot actual por unidad                           | unit_unavailability_risk_score >= 70                                          | count(*)                                                     |
| backlog_physical_items_count        | Backlog físico                                                | count       | data/raw/backlog_mantenimiento.csv                                                  | última fecha disponible de backlog                   | todos los pendientes abiertos del día                                         | count(*)                                                     |
| backlog_overdue_items_count         | Backlog vencido                                               | count       | data/raw/backlog_mantenimiento.csv                                                  | última fecha disponible de backlog                   | antiguedad_backlog_dias >= 14                                                 | count(*)                                                     |
| backlog_critical_physical_count     | Backlog crítico físico                                        | count       | data/raw/backlog_mantenimiento.csv                                                  | última fecha disponible de backlog                   | antiguedad>=21 con severidad alta/crítica o riesgo_acumulado>=70              | count(*)                                                     |
| high_deferral_risk_cases_count      | Casos de alto riesgo de diferimiento                          | count       | data/processed/workshop_priority_table.csv                                          | snapshot actual de priorización                      | deferral_risk_score >= 70                                                     | count(*)                                                     |
| cbm_vs_reactiva_availability_pp     | CBM vs reactiva: mejora de disponibilidad                     | pp          | data/processed/comparativo_estrategias.csv                                          | escenario comparativo de estrategia                  | estrategia in (reactiva, basada_en_condicion)                                 | fleet_availability(CBM) - fleet_availability(reactiva)       |
| cbm_operational_savings_eur         | Ahorro operativo CBM vs reactiva                              | eur         | data/processed/comparativo_estrategias.csv                                          | escenario comparativo de estrategia                  | estrategia in (reactiva, basada_en_condicion)                                 | coste_operativo_proxy(reactiva) - coste_operativo_proxy(CBM) |
| cbm_value_range_min_eur             | CBM vs reactiva: ahorro neto mínimo plausible (P10)           | eur         | data/processed/comparativo_estrategias.csv                                          | escenario comparativo de estrategia con sensibilidad | estrategia = basada_en_condicion                                              | rango_plausible_valor_min(CBM)                               |
| cbm_value_range_max_eur             | CBM vs reactiva: ahorro neto máximo plausible (P90)           | eur         | data/processed/comparativo_estrategias.csv                                          | escenario comparativo de estrategia con sensibilidad | estrategia = basada_en_condicion                                              | rango_plausible_valor_max(CBM)                               |
| cbm_prob_positive_savings           | CBM vs reactiva: probabilidad de ahorro positivo              | ratio_0_1   | data/processed/comparativo_estrategias.csv                                          | escenario comparativo de estrategia con sensibilidad | estrategia = basada_en_condicion                                              | prob_ahorro_positivo(CBM)                                    |
| avoidable_downtime_hours_inspection | Horas de indisponibilidad evitables por inspección automática | hours       | data/processed/inspection_module_value_comparison.csv                               | escenario comparativo inspección automática          | scenario in (sin_inspeccion_automatica, con_inspeccion_automatica)            | horas_indisponibilidad(sin) - horas_indisponibilidad(con)    |
| avoidable_correctives_inspection    | Correctivas evitables por inspección automática               | count_proxy | data/processed/inspection_module_value_comparison.csv                               | escenario comparativo inspección automática          | scenario in (sin_inspeccion_automatica, con_inspeccion_automatica)            | correctivas(sin) - correctivas(con)                          |
| mean_depot_saturation_pct           | Saturación media de depósitos                                 | pct         | data/processed/vw_depot_maintenance_pressure.csv                                    | última fecha disponible                              | fecha = max(fecha)                                                            | mean(saturation_ratio) * 100                                 |
| backlog_exposure_adjusted_mean      | Exposure backlog-adjusted medio                               | score_0_100 | data/processed/vw_depot_maintenance_pressure.csv                                    | última fecha con backlog físico disponible           | fecha = max(fecha) alineada con snapshot de backlog                           | mean(backlog_exposure_adjusted_score)                        |
| top_unit_by_priority                | Unidad prioritaria                                            | id          | data/processed/workshop_priority_table.csv                                          | snapshot actual de priorización                      | top 1 ordenado por intervention_priority_score desc, deferral_risk_score desc | first(unidad_id)                                             |
| top_component_by_priority           | Componente prioritario                                        | id          | data/processed/workshop_priority_table.csv                                          | snapshot actual de priorización                      | misma fila top_unit_by_priority                                               | first(componente_id)                                         |
| top_component_family_by_priority    | Familia de componente prioritario                             | label       | data/processed/scoring_componentes.csv + data/processed/workshop_priority_table.csv | snapshot actual                                      | misma fila top_unit_by_priority                                               | lookup component_family                                      |
| top_depot_by_saturation             | Depósito más saturado                                         | id          | data/processed/vw_depot_maintenance_pressure.csv                                    | última fecha disponible                              | fecha = max(fecha)                                                            | argmax(saturation_ratio)                                     |
| top_depot_saturation_pct            | Saturación del depósito más exigido                           | pct         | data/processed/vw_depot_maintenance_pressure.csv                                    | última fecha disponible                              | fecha = max(fecha)                                                            | max(saturation_ratio) * 100                                  |
| deferral_cost_delta_14d_eur         | Impacto de diferimiento a 14 días (coste)                     | eur         | data/processed/impacto_diferimiento_resumen.csv                                     | escenarios de diferimiento                           | comparación defer_dias = 14 vs defer_dias = 0                                 | coste_total_eur(14) - coste_total_eur(0)                     |
| deferral_downtime_delta_14d_h       | Impacto de diferimiento a 14 días (downtime)                  | hours       | data/processed/impacto_diferimiento_resumen.csv                                     | escenarios de diferimiento                           | comparación defer_dias = 14 vs defer_dias = 0                                 | downtime_total_h(14) - downtime_total_h(0)                   |
| top_priority_score                  | Score de prioridad de intervención (top)                      | score_0_100 | data/processed/workshop_priority_table.csv                                          | snapshot actual de priorización                      | fila top por prioridad                                                        | first(intervention_priority_score)                           |
| top_deferral_risk_score             | Score de riesgo por diferimiento (top)                        | score_0_100 | data/processed/workshop_priority_table.csv                                          | snapshot actual de priorización                      | fila top por prioridad                                                        | first(deferral_risk_score)                                   |
| n_flotas                            | n_flotas                                                      | meta        | pipeline_runtime                                                                    | nan                                                  | nan                                                                           | nan                                                          |
| n_unidades                          | n_unidades                                                    | meta        | pipeline_runtime                                                                    | nan                                                  | nan                                                                           | nan                                                          |
| n_depositos                         | n_depositos                                                   | meta        | pipeline_runtime                                                                    | nan                                                  | nan                                                                           | nan                                                          |
| n_componentes                       | n_componentes                                                 | meta        | pipeline_runtime                                                                    | nan                                                  | nan                                                                           | nan                                                          |

## Mapeo métrica -> artefacto narrativo
| artifact                          | block                    | metric_id                           |
|:----------------------------------|:-------------------------|:------------------------------------|
| README.md                         | Key Findings             | fleet_availability_pct              |
| README.md                         | Key Findings             | high_risk_units_count               |
| README.md                         | Key Findings             | backlog_physical_items_count        |
| README.md                         | Key Findings             | backlog_overdue_items_count         |
| README.md                         | Key Findings             | backlog_critical_physical_count     |
| README.md                         | Key Findings             | high_deferral_risk_cases_count      |
| README.md                         | Key Findings             | cbm_vs_reactiva_availability_pp     |
| README.md                         | Key Findings             | cbm_operational_savings_eur         |
| README.md                         | Key Findings             | cbm_value_range_min_eur             |
| README.md                         | Key Findings             | cbm_value_range_max_eur             |
| README.md                         | Key Findings             | cbm_prob_positive_savings           |
| README.md                         | Key Findings             | avoidable_downtime_hours_inspection |
| README.md                         | Decisión Final           | top_unit_by_priority                |
| README.md                         | Decisión Final           | top_component_by_priority           |
| README.md                         | Decisión Final           | top_component_family_by_priority    |
| README.md                         | Decisión Final           | deferral_cost_delta_14d_eur         |
| README.md                         | Decisión Final           | deferral_downtime_delta_14d_h       |
| docs/memo_ejecutivo_es.md         | Hallazgos                | fleet_availability_pct              |
| docs/memo_ejecutivo_es.md         | Hallazgos                | high_risk_units_count               |
| docs/memo_ejecutivo_es.md         | Hallazgos                | backlog_physical_items_count        |
| docs/memo_ejecutivo_es.md         | Hallazgos                | backlog_overdue_items_count         |
| docs/memo_ejecutivo_es.md         | Hallazgos                | backlog_critical_physical_count     |
| docs/memo_ejecutivo_es.md         | Hallazgos                | high_deferral_risk_cases_count      |
| docs/memo_ejecutivo_es.md         | Implicaciones económicas | cbm_operational_savings_eur         |
| docs/memo_ejecutivo_es.md         | Implicaciones económicas | cbm_value_range_min_eur             |
| docs/memo_ejecutivo_es.md         | Implicaciones económicas | cbm_value_range_max_eur             |
| docs/memo_ejecutivo_es.md         | Implicaciones económicas | cbm_prob_positive_savings           |
| docs/memo_ejecutivo_es.md         | Trade-offs               | deferral_cost_delta_14d_eur         |
| docs/memo_ejecutivo_es.md         | Trade-offs               | deferral_downtime_delta_14d_h       |
| docs/memo_ejecutivo_es.md         | Prioridades              | top_unit_by_priority                |
| docs/memo_ejecutivo_es.md         | Prioridades              | top_component_by_priority           |
| outputs/dashboard/index.html      | KPI Cards                | fleet_availability_pct              |
| outputs/dashboard/index.html      | KPI Cards                | mtbf_proxy_hours                    |
| outputs/dashboard/index.html      | KPI Cards                | mttr_proxy_hours                    |
| outputs/dashboard/index.html      | KPI Cards                | high_risk_units_count               |
| outputs/dashboard/index.html      | KPI Cards                | backlog_physical_items_count        |
| outputs/dashboard/index.html      | KPI Cards                | backlog_overdue_items_count         |
| outputs/dashboard/index.html      | KPI Cards                | backlog_critical_physical_count     |
| outputs/dashboard/index.html      | KPI Cards                | high_deferral_risk_cases_count      |
| outputs/dashboard/index.html      | KPI Cards                | cbm_operational_savings_eur         |
| outputs/dashboard/index.html      | KPI Cards                | cbm_value_range_min_eur             |
| outputs/dashboard/index.html      | KPI Cards                | cbm_value_range_max_eur             |
| outputs/dashboard/index.html      | KPI Cards                | cbm_prob_positive_savings           |
| outputs/dashboard/index.html      | KPI Cards                | mean_depot_saturation_pct           |
| outputs/dashboard/index.html      | Decisión Ejecutiva       | top_unit_by_priority                |
| outputs/dashboard/index.html      | Decisión Ejecutiva       | top_component_by_priority           |
| outputs/dashboard/index.html      | Decisión Ejecutiva       | top_deferral_risk_score             |
| outputs/reports/summary_blocks.md | Snapshot                 | top_unit_by_priority                |
| outputs/reports/summary_blocks.md | Snapshot                 | top_component_by_priority           |
| outputs/reports/summary_blocks.md | Snapshot                 | cbm_operational_savings_eur         |

## Reglas de consistencia
- Misma definición de métrica para README, memo, dashboard y summaries.
- Misma ventana temporal para narrativa ejecutiva (`histórico completo` o `última fecha`, según métrica).
- Misma unidad de medida y formato de presentación.
- Si el output de datos cambia, la narrativa se regenera automáticamente en el pipeline.

## Publicación
- Bloquear publicación cuando los tests de consistencia interartefactos fallen.
- No editar manualmente cifras en README/memo/dashboard.