# Metric Lineage

## Objetivo
Trazabilidad lógica desde raw hasta decisión ejecutiva.

## Lineage por Métrica
| metric_name                    | raw_inputs                                                                                                    | feature_or_mart_layer                                   | source_of_truth                               | intermediate_outputs               | final_consumption                            |
|:-------------------------------|:--------------------------------------------------------------------------------------------------------------|:--------------------------------------------------------|:----------------------------------------------|:-----------------------------------|:---------------------------------------------|
| health_score                   | data/raw/sensores_componentes.csv + inspecciones_automaticas.csv + componentes_criticos.csv                   | data/processed/component_day_features.csv               | data/processed/scoring_componentes.csv        | kpi_top_componentes_por_criticidad | dashboard: Vista Salud                       |
| prob_fallo_30d                 | data/raw/sensores_componentes.csv + fallas_historicas.csv                                                     | data/processed/component_day_features.csv               | data/processed/scoring_componentes.csv        | kpi_top_unidades_por_riesgo        | dashboard: Vista Salud/Priorización          |
| component_rul_estimate         | data/raw/sensores_componentes.csv + eventos_mantenimiento.csv + fallas_historicas.csv                         | data/processed/component_day_features.csv               | data/processed/component_rul_estimate.csv     | rul_distribution_before_after      | dashboard: priorización + decisión ejecutiva |
| backlog_fisico                 | data/raw/backlog_mantenimiento.csv                                                                            | sql/mart vw_depot_maintenance_pressure                  | data/processed/narrative_metrics_official.csv | kpi_backlog                        | dashboard: KPI cards + Vista Taller          |
| high_deferral_risk_cases_count | data/raw/intervenciones_programadas.csv + señales score                                                       | data/processed/workshop_priority_features.csv           | data/processed/workshop_priority_table.csv    | kpi_backlog_critico                | dashboard: KPI cards + Decisión Ejecutiva    |
| fleet_availability_pct         | data/raw/disponibilidad_servicio.csv                                                                          | data/processed/fleet_week_features.csv                  | data/processed/narrative_metrics_official.csv | kpi_fleet_availability             | dashboard: Header + KPI cards                |
| cbm_operational_savings_eur    | data/processed/fleet_week_features.csv + workshop_priority_table.csv + inspection_module_value_comparison.csv | data/processed/comparativo_estrategias_sensibilidad.csv | data/processed/comparativo_estrategias.csv    | comparativo_estrategias_escenarios | dashboard: Vista Estratégica                 |
| deferral_cost_delta_14d_eur    | data/processed/workshop_priority_table.csv                                                                    | data/processed/impacto_diferimiento_resumen.csv         | data/processed/narrative_metrics_official.csv | impacto_diferimiento_resumen       | dashboard: Trade-off diferimiento            |
| mean_depot_saturation_pct      | data/raw/depositos.csv + backlog_mantenimiento.csv + eventos_mantenimiento.csv                                | sql/vw_depot_maintenance_pressure                       | data/processed/narrative_metrics_official.csv | kpi_depot_saturation               | dashboard: KPI cards + Vista Taller          |

## Ownership por Output
- Reliability Analytics: scoring, RUL, risk drivers.
- Workshop Planning: priorización/scheduling, saturación, backlog operativo.
- Operations Analytics: disponibilidad e impacto en servicio.
- Finance Analytics (proxy): comparativo CBM y diferimiento.
- Analytics Governance: SSOT narrativo y contratos.