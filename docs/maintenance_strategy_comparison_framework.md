# Maintenance Strategy Comparison Framework

## Objetivo
Comparar estrategias de mantenimiento con separación explícita entre evidencia observada, hipótesis operativas y proxies económicos.

## 1) Outputs observados
| variable                 |         valor | tipo      | fuente                               |
|:-------------------------|--------------:|:----------|:-------------------------------------|
| availability_pct_base    |     90.4494   | observado | fleet_week_features                  |
| mtbf_base                |     16.1029   | observado | fleet_week_features                  |
| mttr_base                |      6.95152  | observado | fleet_week_features                  |
| downtime_h_base          | 190009        | observado | disponibilidad_servicio              |
| backlog_critical_base    |   1957        | observado | backlog_mantenimiento                |
| deferral_high_observed   |     64        | observado | workshop_priority_table              |
| failures_total           |  10427        | observado | fallas_historicas                    |
| corrective_events_base   |  10410        | observado | eventos_mantenimiento                |
| preventive_events_base   |   6561        | observado | eventos_mantenimiento                |
| inspection_quality_index |      0.327823 | observado | inspection_module_family_performance |
| ew_precision             |      0.4064   | observado | early_warning_practical_accuracy     |

## 2) Supuestos estructurales por estrategia
| estrategia          |   proactive_factor |   predictive_leverage |   preventive_intensity |   corrective_dependence |   enablement_cost_eur | hipotesis_operativa                                   |
|:--------------------|-------------------:|----------------------:|-----------------------:|------------------------:|----------------------:|:------------------------------------------------------|
| reactiva            |               0.16 |                  0.08 |                   0.22 |                    0.95 |               0       | mínima anticipación, alta dependencia correctiva      |
| preventiva_rigida   |               0.56 |                  0.28 |                   0.95 |                    0.62 |               2.4e+06 | intervención calendarizada alta, menor flexibilidad   |
| basada_en_condicion |               0.78 |                  0.74 |                   0.66 |                    0.48 |               1.3e+07 | anticipación selectiva, dependencia de señal temprana |

## 3) Hipótesis operativas por escenario
| scenario_profile   |   failure_stress |   capacity_factor |   detection_realization |   downtime_cost |   corrective_cost |   preventive_cost |   enablement_cost_mult | hipotesis                                                                  |
|:-------------------|-----------------:|------------------:|------------------------:|----------------:|------------------:|------------------:|-----------------------:|:---------------------------------------------------------------------------|
| conservador        |             1.12 |              0.9  |                    0.86 |            1.2  |              1.12 |              1.08 |                   1.35 | contexto exigente, menor efectividad de detección y mayor presión de coste |
| base               |             1    |              1    |                    1    |            1    |              1    |              1    |                   1    | condición operativa esperada                                               |
| agresivo           |             0.93 |              1.08 |                    1.12 |            0.92 |              0.94 |              0.96 |                   0.92 | mejor ejecución operativa y mayor calidad de señal                         |

## 4) Proxies económicos
| variable                        |   valor | tipo            | fuente     |
|:--------------------------------|--------:|:----------------|:-----------|
| cost_downtime_hour_base         |    1300 | proxy_economico | assumption |
| cost_corrective_event_base      |   16000 | proxy_economico | assumption |
| cost_preventive_event_base      |    8000 | proxy_economico | assumption |
| cost_backlog_critical_case_base |    2500 | proxy_economico | assumption |
| cost_deferral_case_base         |    5200 | proxy_economico | assumption |
| cost_service_impact_unit_base   |   42000 | proxy_economico | assumption |

## 5) Resultados por escenario (P10/P50/P90)
| scenario_profile   | estrategia          |   coste_total_p10 |   coste_total_p50 |   coste_total_p90 |   downtime_p50 |   correctivas_evitables_p50 |   horas_servicio_preservadas_p50 |   ahorro_neto_p50_vs_reactiva |   downside_ahorro_p10_vs_reactiva |   upside_ahorro_p90_vs_reactiva |   prob_ahorro_positivo |
|:-------------------|:--------------------|------------------:|------------------:|------------------:|---------------:|----------------------------:|---------------------------------:|------------------------------:|----------------------------------:|--------------------------------:|-----------------------:|
| agresivo           | basada_en_condicion |       3.31706e+08 |       3.77471e+08 |       4.32153e+08 |         123921 |                    2424.56  |                         66087.6  |                   1.22069e+07 |                      -2.5249e+06  |                     2.91028e+07 |               0.85048  |
| agresivo           | preventiva_rigida   |       3.0265e+08  |       3.50139e+08 |       4.07677e+08 |         128266 |                    2086.47  |                         61743.1  |                   3.98295e+07 |                       2.67505e+07 |                     5.48406e+07 |               1        |
| agresivo           | reactiva            |       3.31805e+08 |       3.89974e+08 |       4.6117e+08  |         159821 |                       0     |                         30187.5  |                   0           |                       0           |                     0           |               0        |
| base               | basada_en_condicion |       3.90207e+08 |       4.47097e+08 |       5.14526e+08 |         144083 |                    1809.71  |                         45926.5  |                   1.89004e+07 |                       1.86065e+06 |                     3.9066e+07  |               0.923182 |
| base               | preventiva_rigida   |       3.56092e+08 |       4.12801e+08 |       4.81566e+08 |         149144 |                    1446.5   |                         40864.9  |                   5.31121e+07 |                       3.75553e+07 |                     7.14569e+07 |               1        |
| base               | reactiva            |       3.95566e+08 |       4.65629e+08 |       5.53893e+08 |         185668 |                       0     |                          4340.88 |                   0           |                       0           |                     0           |               0        |
| conservador        | basada_en_condicion |       5.34417e+08 |       6.15069e+08 |       7.09251e+08 |         179474 |                     761.323 |                         10535.1  |                   2.25483e+07 |                 -465269           |                     4.91331e+07 |               0.893004 |
| conservador        | preventiva_rigida   |       4.73822e+08 |       5.54712e+08 |       6.52722e+08 |         185804 |                     353.283 |                          4204.75 |                   8.26245e+07 |                       6.13359e+07 |                     1.07636e+08 |               1        |
| conservador        | reactiva            |       5.39197e+08 |       6.37427e+08 |       7.59786e+08 |         231127 |                       0     |                             0    |                   0           |                       0           |                     0           |               0        |

## 6) Rango plausible de valor
| estrategia          |   coste_total_p10 |   coste_total_p50 |   coste_total_p90 |   ahorro_neto_p10_vs_reactiva |   ahorro_neto_p50_vs_reactiva |   ahorro_neto_p90_vs_reactiva |   downside_case |   upside_case |   prob_ahorro_positivo |
|:--------------------|------------------:|------------------:|------------------:|------------------------------:|------------------------------:|------------------------------:|----------------:|--------------:|-----------------------:|
| basada_en_condicion |       3.56035e+08 |       4.52218e+08 |       6.51965e+08 |              -628418          |                   1.72692e+07 |                   4.02971e+07 |    -2.21929e+07 |   7.48079e+07 |               0.888889 |
| preventiva_rigida   |       3.2781e+08  |       4.18929e+08 |       5.91512e+08 |                    3.2816e+07 |                   5.50097e+07 |                   9.28101e+07 |     1.45917e+07 |   1.35215e+08 |               1        |
| reactiva            |       3.61063e+08 |       4.73979e+08 |       6.83754e+08 |                    0          |                   0           |                   0           |     0           |   0           |               0        |

## 7) Reglas de interpretación
- `ahorro_neto_vs_reactiva > 0`: mejora económica frente a reactiva en ese escenario/sensibilidad.
- `downside_case < 0`: existe cola de riesgo económica donde la estrategia puede no compensar.
- `prob_ahorro_positivo`: robustez de la estrategia bajo incertidumbre.

## 8) Nota metodológica
No se afirma ganador universal: el resultado depende de capacidad, calidad de detección temprana y estructura de costes.