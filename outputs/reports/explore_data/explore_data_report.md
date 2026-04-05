# Explore-Data Audit | Railway CBM

## Objetivo
Auditoría formal de calidad y readiness de datos previa a modelado, scoring, RUL, priorización y dashboard.

## Resumen por dataset
| tabla                          | grain                                                | candidate_key                                    | foreign_keys_esperadas                                                                                               |   n_filas |   n_columnas | cobertura_temporal       |   null_rate_promedio_pct |   duplicados_candidate_key |
|:-------------------------------|:-----------------------------------------------------|:-------------------------------------------------|:---------------------------------------------------------------------------------------------------------------------|----------:|-------------:|:-------------------------|-------------------------:|---------------------------:|
| alertas_operativas             | 1 fila por alerta operacional                        | alerta_id                                        | unidad_id->unidades.unidad_id, componente_id->componentes_criticos.componente_id                                     |    394102 |           10 | 2024-01-01 -> 2025-12-31 |                        0 |                          0 |
| asignacion_servicio            | 1 fila por unidad y fecha                            | fecha, unidad_id                                 | unidad_id->unidades.unidad_id                                                                                        |    105264 |            7 | 2024-01-01 -> 2025-12-31 |                        0 |                          0 |
| backlog_mantenimiento          | 1 fila por snapshot fecha-depósito-unidad-componente | fecha, deposito_id, unidad_id, componente_id     | unidad_id->unidades.unidad_id, component_id->componentes_criticos.componente_id, deposito_id->depositos.deposito_id  |     87266 |            8 | 2024-01-01 -> 2025-12-29 |                        0 |                      38997 |
| componentes_criticos           | 1 fila por componente crítico instalado              | componente_id                                    | unidad_id->unidades.unidad_id                                                                                        |      1152 |           12 | n/a                      |                        0 |                          0 |
| depositos                      | 1 fila por depósito                                  | deposito_id                                      |                                                                                                                      |        10 |            7 | n/a                      |                        0 |                          0 |
| disponibilidad_servicio        | 1 fila por unidad y fecha                            | fecha, unidad_id                                 | unidad_id->unidades.unidad_id, flota_id->flotas.flota_id                                                             |    105264 |           10 | 2024-01-01 -> 2025-12-31 |                        0 |                          0 |
| escenarios_mantenimiento       | 1 fila por fecha y escenario                         | fecha, escenario                                 |                                                                                                                      |      2193 |            6 | 2024-01-01 -> 2025-12-31 |                        0 |                          0 |
| eventos_mantenimiento          | 1 fila por evento de mantenimiento                   | mantenimiento_id                                 | unidad_id->unidades.unidad_id, componente_id->componentes_criticos.componente_id, deposito_id->depositos.deposito_id |     16971 |           15 | 2024-01-01 -> 2025-12-31 |                        0 |                          0 |
| fallas_historicas              | 1 fila por falla registrada                          | falla_id                                         | unidad_id->unidades.unidad_id, componente_id->componentes_criticos.componente_id                                     |     10427 |           10 | 2024-01-01 -> 2025-12-31 |                        0 |                          0 |
| flotas                         | 1 fila por flota                                     | flota_id                                         |                                                                                                                      |         6 |            9 | n/a                      |                        0 |                          0 |
| inspecciones_automaticas       | 1 fila por evento de inspección automática           | inspeccion_id                                    | unidad_id->unidades.unidad_id, componente_id->componentes_criticos.componente_id                                     |    253336 |           11 | 2024-01-01 -> 2025-12-31 |                        0 |                          0 |
| intervenciones_programadas     | 1 fila por intervención programada                   | intervencion_id                                  | unidad_id->unidades.unidad_id, componente_id->componentes_criticos.componente_id, deposito_id->depositos.deposito_id |     16955 |            9 | 2024-01-01 -> 2025-12-31 |                        0 |                          0 |
| parametros_operativos_contexto | 1 fila por fecha y línea                             | fecha, linea_servicio                            |                                                                                                                      |     11696 |            8 | 2024-01-01 -> 2025-12-31 |                        0 |                          0 |
| sensores_componentes           | 1 fila por timestamp-unidad-componente-sensor        | timestamp, unidad_id, componente_id, sensor_tipo | unidad_id->unidades.unidad_id, componente_id->componentes_criticos.componente_id                                     |   2457248 |           14 | 2024-01-01 -> 2025-12-31 |                        0 |                          0 |
| unidades                       | 1 fila por unidad                                    | unidad_id                                        | flota_id->flotas.flota_id, deposito_id->depositos.deposito_id                                                        |       144 |           10 | 2010-01-26 -> 2022-09-16 |                        0 |                          0 |

## Issues priorizados
- Total issues: 1
- Alta severidad: 1
- Media severidad: 0
- Baja severidad: 0

| severidad   | issue                       | tabla                 | detalle                                                                           | impacto                                    |
|:------------|:----------------------------|:----------------------|:----------------------------------------------------------------------------------|:-------------------------------------------|
| alta        | duplicados_en_candidate_key | backlog_mantenimiento | 38997 duplicados sobre key=['fecha', 'deposito_id', 'unidad_id', 'componente_id'] | Puede romper joins y agregaciones de marts |

## Recomendaciones de transformación analítica
- Consolidar un `component_day` con ventanas rolling y señales de estrés para soporte directo a scoring y RUL.
- Normalizar severidades categóricas a ordinales para modelos interpretables y comparables entre dominios.
- Mantener snapshots de backlog con frecuencia fija para análisis de presión de taller robusto.
- Controlar drift de sensores por familia de componente antes de ajustar thresholds de alerta.
- Versionar reglas de early warning y su precisión operacional por familia (wheel/brake/bogie/pantograph).

## Propuesta de joins oficiales
| left_table               | right_table                | join_condition                                                                                                                                            | purpose                     |
|:-------------------------|:---------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------|:----------------------------|
| componentes_criticos     | unidades                   | componentes_criticos.unidad_id = unidades.unidad_id                                                                                                       | jerarquía activo            |
| unidades                 | flotas                     | unidades.flota_id = flotas.flota_id                                                                                                                       | nivel flota                 |
| unidades                 | depositos                  | unidades.deposito_id = depositos.deposito_id                                                                                                              | capacidad y especialización |
| sensores_componentes     | componentes_criticos       | sensores_componentes.componente_id = componentes_criticos.componente_id                                                                                   | condición por activo        |
| inspecciones_automaticas | componentes_criticos       | inspecciones_automaticas.componente_id = componentes_criticos.componente_id                                                                               | defectos detectados         |
| fallas_historicas        | componentes_criticos       | fallas_historicas.componente_id = componentes_criticos.componente_id                                                                                      | historial de fallo          |
| eventos_mantenimiento    | componentes_criticos       | eventos_mantenimiento.componente_id = componentes_criticos.componente_id                                                                                  | historial de intervención   |
| alertas_operativas       | componentes_criticos       | alertas_operativas.componente_id = componentes_criticos.componente_id                                                                                     | early warning               |
| disponibilidad_servicio  | asignacion_servicio        | disponibilidad_servicio.fecha = asignacion_servicio.fecha AND disponibilidad_servicio.unidad_id = asignacion_servicio.unidad_id                           | impacto servicio            |
| backlog_mantenimiento    | intervenciones_programadas | backlog_mantenimiento.componente_id = intervenciones_programadas.componente_id AND backlog_mantenimiento.unidad_id = intervenciones_programadas.unidad_id | presión de taller           |

## Propuesta de marts analíticos
| mart_name            | grain          | core_content                                                 | main_use                   |
|:---------------------|:---------------|:-------------------------------------------------------------|:---------------------------|
| mart_component_day   | componente-dia | salud, degradación, alertas, fallas, mantenimiento           | scoring y RUL              |
| mart_unit_day        | unidad-dia     | riesgo agregado, indisponibilidad, backlog, impacto servicio | priorización operativa     |
| mart_depot_day       | deposito-dia   | saturación, carga correctiva/programada, riesgo pendiente    | planificación de taller    |
| mart_fleet_week      | flota-semana   | availability, MTBF/MTTR proxy, tendencia estratégica         | dirección de mantenimiento |
| mart_condition_value | global-periodo | alertas tempranas, correctivas evitables, valor CBM          | business case              |

## Impacto en scoring, RUL y dashboard
- Nulls o duplicados en llaves de componente/unidad afectan directamente calidad de features y estabilidad del ranking de riesgo.
- Incoherencias temporales en mantenimiento alteran `days_since_last_maintenance` y degradan priorización de taller.
- Señales fuera de rango generan falsos positivos de alerta y sesgo en estimaciones de health/failure risk.
- Fallas sin impacto asociado subestiman MTTR, indisponibilidad y valor de estrategias CBM.