# Backlog Metric Governance

## Objetivo
Separar formalmente backlog físico y riesgo de diferimiento para evitar KPIs híbridos mal rotulados.

## Taxonomía oficial
1. backlog físico: pendientes reales abiertos.
2. backlog vencido: backlog físico fuera de ventana operativa (>=14 días).
3. backlog crítico por edad/severidad: vencido severo o riesgo acumulado alto.
4. riesgo de diferimiento: probabilidad de daño al aplazar una intervención (score de decisión).
5. exposure backlog-adjusted: exposición compuesta 0-100 del backlog físico.

## Regla de gobierno obligatoria
- Nunca usar `deferral_risk_score` para reportar backlog físico.
- Nunca usar backlog físico para inferir automáticamente riesgo de diferimiento sin score explícito.

## Tabla Before/After
| kpi_name                  | definition_before                                                               |   value_before | source_before                              | definition_after                                                           |   value_after | source_after                                     | decision_supported                                        |
|:--------------------------|:--------------------------------------------------------------------------------|---------------:|:-------------------------------------------|:---------------------------------------------------------------------------|--------------:|:-------------------------------------------------|:----------------------------------------------------------|
| backlog_fisico            | n/a                                                                             |            nan | nan                                        | pendientes reales abiertos de mantenimiento                                |       2054    | data/raw/backlog_mantenimiento.csv               | dimensionar carga real de taller                          |
| backlog_vencido           | n/a                                                                             |            nan | nan                                        | pendientes físicos con antigüedad >= 14 días                               |       2000    | data/raw/backlog_mantenimiento.csv               | acelerar cola vencida y proteger SLA                      |
| backlog_critico_fisico    | conteo de componentes con deferral_risk_score >= 70 (mezclado con diferimiento) |             64 | data/processed/workshop_priority_table.csv | pendientes físicos críticos por edad/severidad o riesgo acumulado alto     |       1957    | data/raw/backlog_mantenimiento.csv               | qué pendientes físicos deben intervenirse antes           |
| riesgo_diferimiento_alto  | implícitamente mezclado con backlog crítico                                     |             64 | data/processed/workshop_priority_table.csv | casos con deferral_risk_score >= 70 (riesgo de aplazar, no backlog físico) |         64    | data/processed/workshop_priority_table.csv       | qué no debe diferirse por impacto operacional             |
| exposure_backlog_adjusted | n/a                                                                             |            nan | nan                                        | score compuesto 0-100 de exposición de backlog físico                      |         96.21 | data/processed/vw_depot_maintenance_pressure.csv | priorizar depósitos por exposición estructural de backlog |

## KPI oficial por decisión
| metric_id                       | category                           | definition                                                           | unit        |   value | decision_use                                        |
|:--------------------------------|:-----------------------------------|:---------------------------------------------------------------------|:------------|--------:|:----------------------------------------------------|
| backlog_physical_items_count    | backlog_fisico                     | pendientes reales abiertos                                           | count       | 2054    | dimensionamiento de cola real de taller             |
| backlog_overdue_items_count     | backlog_vencido                    | pendientes con antigüedad >=14 días                                  | count       | 2000    | escalado táctico para recuperar cumplimiento        |
| backlog_critical_physical_count | backlog_critico_por_edad_severidad | pendientes críticos por edad/severidad o riesgo acumulado            | count       | 1957    | secuenciación de intervención física prioritaria    |
| high_deferral_risk_cases_count  | riesgo_diferimiento                | casos con score de diferimiento >=70                                 | count       |   64    | límite de aplazamiento y ventana de entrada         |
| backlog_exposure_adjusted_mean  | exposure_backlog_adjusted          | exposición compuesta 0-100 (cantidad+edad+criticidad backlog físico) | score_0_100 |   96.21 | priorización de depósitos y rebalanceo de capacidad |

## Uso ejecutivo
- Dirección de taller: backlog físico/vencido/crítico por depósito.
- Dirección de operaciones: riesgo de diferimiento y exposición backlog-adjusted.
- Dirección de mantenimiento: combinación de ambos para secuencia de intervención.