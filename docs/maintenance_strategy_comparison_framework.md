# Marco de Comparación de Estrategias de Mantenimiento

## Objetivo
Comparar estrategias de mantenimiento con separación explícita entre evidencia observada, hipótesis operativas y proxies económicos.

## 1) Datos observados
| variable                 |     valor | tipo      | fuente                               |
|:-------------------------|----------:|:----------|:-------------------------------------|
| availability_pct_base    |     95.75 | observado | fleet_week_features                  |
| mtbf_base                |  1,046.81 | observado | fleet_week_features                  |
| mttr_base                |      5.18 | observado | fleet_week_features                  |
| downtime_h_base          | 85,158.59 | observado | disponibilidad_servicio              |
| backlog_critical_base    |  1,955.00 | observado | backlog_mantenimiento                |
| deferral_high_observed   |     43.00 | observado | workshop_priority_table              |
| failures_total           |  3,021.00 | observado | fallas_historicas                    |
| corrective_events_base   |  3,016.00 | observado | eventos_mantenimiento                |
| preventive_events_base   |  6,568.00 | observado | eventos_mantenimiento                |
| inspection_quality_index |      0.09 | observado | inspection_module_family_performance |
| ew_precision             |      0.14 | observado | early_warning_practical_accuracy     |

## 2) Supuestos estructurales por estrategia
| estrategia          |   proactive_factor |   predictive_leverage |   preventive_intensity |   corrective_dependence |   enablement_cost_eur | hipotesis_operativa                                   |
|:--------------------|-------------------:|----------------------:|-----------------------:|------------------------:|----------------------:|:------------------------------------------------------|
| reactiva            |               0.16 |                  0.08 |                   0.22 |                    0.95 |                  0.00 | mínima anticipación, alta dependencia correctiva      |
| preventiva_rigida   |               0.56 |                  0.28 |                   0.95 |                    0.62 |          2,400,000.00 | intervención calendarizada alta, menor flexibilidad   |
| basada_en_condicion |               0.78 |                  0.74 |                   0.66 |                    0.48 |         13,000,000.00 | anticipación selectiva, dependencia de señal temprana |

## 3) Hipótesis operativas por escenario
| scenario_profile   |   failure_stress |   capacity_factor |   detection_realization |   downtime_cost |   corrective_cost |   preventive_cost |   enablement_cost_mult | hipotesis                                                                  |
|:-------------------|-----------------:|------------------:|------------------------:|----------------:|------------------:|------------------:|-----------------------:|:---------------------------------------------------------------------------|
| conservador        |             1.12 |              0.90 |                    0.86 |            1.20 |              1.12 |              1.08 |                   1.35 | contexto exigente, menor efectividad de detección y mayor presión de coste |
| base               |             1.00 |              1.00 |                    1.00 |            1.00 |              1.00 |              1.00 |                   1.00 | condición operativa esperada                                               |
| agresivo           |             0.93 |              1.08 |                    1.12 |            0.92 |              0.94 |              0.96 |                   0.92 | mejor ejecución operativa y mayor calidad de señal                         |

## 4) Proxies económicos
| variable                        |     valor | tipo            | fuente     |
|:--------------------------------|----------:|:----------------|:-----------|
| cost_downtime_hour_base         |  1,300.00 | proxy_economico | assumption |
| cost_corrective_event_base      | 16,000.00 | proxy_economico | assumption |
| cost_preventive_event_base      |  8,000.00 | proxy_economico | assumption |
| cost_backlog_critical_case_base |  2,500.00 | proxy_economico | assumption |
| cost_deferral_case_base         |  5,200.00 | proxy_economico | assumption |
| cost_service_impact_unit_base   | 42,000.00 | proxy_economico | assumption |

## 5) Resultados por escenario (P10/P50/P90)
| scenario_profile   | estrategia          |   coste_total_p10 |   coste_total_p50 |   coste_total_p90 |   downtime_p50 |   correctivas_evitables_p50 |   horas_servicio_preservadas_p50 |   ahorro_neto_p50_vs_reactiva |   downside_ahorro_p10_vs_reactiva |   upside_ahorro_p90_vs_reactiva |   prob_ahorro_positivo |
|:-------------------|:--------------------|------------------:|------------------:|------------------:|---------------:|----------------------------:|---------------------------------:|------------------------------:|----------------------------------:|--------------------------------:|-----------------------:|
| agresivo           | basada_en_condicion |    198,712,654.59 |    217,674,704.42 |    240,336,553.77 |      55,659.49 |                      692.32 |                        29,499.10 |                -49,858,715.32 |                    -55,578,344.04 |                  -43,646,189.70 |                   0.00 |
| agresivo           | preventiva_rigida   |    155,634,880.91 |    176,286,068.36 |    198,955,847.64 |      57,622.70 |                      593.69 |                        27,535.89 |                 -7,612,021.61 |                    -14,450,958.44 |                     -934,745.86 |                   0.08 |
| agresivo           | reactiva            |    143,750,115.47 |    167,996,915.22 |    194,782,895.45 |      71,678.52 |                        0.00 |                        13,480.07 |                          0.00 |                              0.00 |                            0.00 |                   0.00 |
| base               | basada_en_condicion |    226,186,565.93 |    250,619,377.74 |    279,777,809.18 |      64,636.83 |                      517.41 |                        20,521.76 |                -51,151,906.00 |                    -59,033,496.00 |                  -42,934,014.18 |                   0.00 |
| base               | preventiva_rigida   |    178,693,785.56 |    203,300,626.46 |    231,286,128.11 |      66,916.69 |                      411.36 |                        18,241.90 |                 -3,319,766.29 |                    -11,319,180.53 |                    4,480,014.83 |                   0.30 |
| base               | reactiva            |    170,388,593.75 |    199,151,074.46 |    232,660,702.06 |      83,239.58 |                        0.00 |                         1,919.02 |                          0.00 |                              0.00 |                            0.00 |                   0.00 |
| conservador        | basada_en_condicion |    302,206,093.66 |    339,725,096.08 |    382,564,561.87 |      80,436.95 |                      217.58 |                         4,721.65 |                -68,893,342.65 |                    -80,935,577.28 |                  -56,881,823.51 |                   0.00 |
| conservador        | preventiva_rigida   |    230,728,566.11 |    263,869,055.48 |    303,039,917.78 |      83,274.10 |                       98.81 |                         1,884.49 |                  6,056,240.16 |                     -3,784,449.59 |                   16,748,805.71 |                   0.78 |
| conservador        | reactiva            |    228,043,951.99 |    270,421,423.59 |    318,050,330.37 |     103,587.03 |                        0.00 |                             0.00 |                          0.00 |                              0.00 |                            0.00 |                   0.00 |

## 6) Rango plausible de valor
| estrategia          |   coste_total_p10 |   coste_total_p50 |   coste_total_p90 |   ahorro_neto_p10_vs_reactiva |   ahorro_neto_p50_vs_reactiva |   ahorro_neto_p90_vs_reactiva |   downside_case |    upside_case |   prob_ahorro_positivo |
|:--------------------|------------------:|------------------:|------------------:|------------------------------:|------------------------------:|------------------------------:|----------------:|---------------:|-----------------------:|
| basada_en_condicion |    209,322,168.02 |    252,594,604.49 |    356,084,798.44 |                -74,235,937.98 |                -53,777,290.47 |                -44,730,808.85 |  -92,226,128.76 | -34,808,335.85 |                   0.00 |
| preventiva_rigida   |    166,089,047.04 |    205,295,274.39 |    280,298,641.74 |                -11,923,746.49 |                 -2,940,648.66 |                 10,592,811.53 |  -19,817,978.96 |  28,579,053.19 |                   0.38 |
| reactiva            |    156,470,159.64 |    202,036,273.27 |    289,037,624.08 |                          0.00 |                          0.00 |                          0.00 |            0.00 |           0.00 |                   0.00 |

## 7) Reglas de interpretación
- `ahorro_neto_vs_reactiva > 0`: mejora económica frente a reactiva en ese escenario/sensibilidad.
- `downside_case < 0`: existe cola de riesgo económica donde la estrategia puede no compensar.
- `prob_ahorro_positivo`: robustez de la estrategia bajo incertidumbre.

## 8) Nota metodológica
No se afirma ganador universal: el resultado depende de capacidad, calidad de detección temprana y estructura de costes.
