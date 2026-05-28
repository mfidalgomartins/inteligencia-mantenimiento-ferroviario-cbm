# Maintenance Strategy Comparison Framework

## Objetivo
Comparar estrategias de mantenimiento con separación explícita entre evidencia observada, hipótesis operativas y proxies económicos.

## 1) Outputs observados
| variable                 |      valor | tipo      | fuente                               |
|:-------------------------|-----------:|:----------|:-------------------------------------|
| availability_pct_base    |      90.45 | observado | fleet_week_features                  |
| mtbf_base                |      16.10 | observado | fleet_week_features                  |
| mttr_base                |       6.95 | observado | fleet_week_features                  |
| downtime_h_base          | 190,009.00 | observado | disponibilidad_servicio              |
| backlog_critical_base    |   1,957.00 | observado | backlog_mantenimiento                |
| deferral_high_observed   |      64.00 | observado | workshop_priority_table              |
| failures_total           |  10,427.00 | observado | fallas_historicas                    |
| corrective_events_base   |  10,410.00 | observado | eventos_mantenimiento                |
| preventive_events_base   |   6,561.00 | observado | eventos_mantenimiento                |
| inspection_quality_index |       0.33 | observado | inspection_module_family_performance |
| ew_precision             |       0.41 | observado | early_warning_practical_accuracy     |

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
| agresivo           | basada_en_condicion |    331,705,859.87 |    377,470,828.39 |    432,152,590.92 |     123,921.39 |                    2,424.56 |                        66,087.62 |                 12,206,853.98 |                     -2,524,895.55 |                   29,102,809.99 |                   0.85 |
| agresivo           | preventiva_rigida   |    302,650,099.17 |    350,139,130.22 |    407,677,231.72 |     128,265.93 |                    2,086.47 |                        61,743.08 |                 39,829,481.30 |                     26,750,461.42 |                   54,840,602.82 |                   1.00 |
| agresivo           | reactiva            |    331,804,676.49 |    389,973,539.27 |    461,169,549.50 |     159,821.48 |                        0.00 |                        30,187.52 |                          0.00 |                              0.00 |                            0.00 |                   0.00 |
| base               | basada_en_condicion |    390,206,727.36 |    447,096,996.95 |    514,526,421.76 |     144,082.53 |                    1,809.71 |                        45,926.47 |                 18,900,374.66 |                      1,860,651.37 |                   39,066,039.48 |                   0.92 |
| base               | preventiva_rigida   |    356,092,180.84 |    412,800,775.30 |    481,566,123.96 |     149,144.10 |                    1,446.50 |                        40,864.91 |                 53,112,115.34 |                     37,555,253.61 |                   71,456,922.72 |                   1.00 |
| base               | reactiva            |    395,566,373.37 |    465,628,786.98 |    553,893,299.71 |     185,668.12 |                        0.00 |                         4,340.88 |                          0.00 |                              0.00 |                            0.00 |                   0.00 |
| conservador        | basada_en_condicion |    534,417,166.41 |    615,069,222.86 |    709,251,092.10 |     179,473.89 |                      761.32 |                        10,535.12 |                 22,548,327.53 |                       -465,268.93 |                   49,133,079.61 |                   0.89 |
| conservador        | preventiva_rigida   |    473,822,236.77 |    554,712,197.32 |    652,722,154.50 |     185,804.25 |                      353.28 |                         4,204.75 |                 82,624,481.94 |                     61,335,938.69 |                  107,636,336.97 |                   1.00 |
| conservador        | reactiva            |    539,197,060.04 |    637,427,255.31 |    759,786,159.71 |     231,127.20 |                        0.00 |                             0.00 |                          0.00 |                              0.00 |                            0.00 |                   0.00 |

## 6) Rango plausible de valor
| estrategia          |   coste_total_p10 |   coste_total_p50 |   coste_total_p90 |   ahorro_neto_p10_vs_reactiva |   ahorro_neto_p50_vs_reactiva |   ahorro_neto_p90_vs_reactiva |   downside_case |    upside_case |   prob_ahorro_positivo |
|:--------------------|------------------:|------------------:|------------------:|------------------------------:|------------------------------:|------------------------------:|----------------:|---------------:|-----------------------:|
| basada_en_condicion |    356,034,632.19 |    452,217,580.36 |    651,965,111.22 |                   -628,417.60 |                 17,269,169.29 |                 40,297,095.49 |  -22,192,890.11 |  74,807,910.20 |                   0.89 |
| preventiva_rigida   |    327,810,182.54 |    418,928,589.21 |    591,512,109.14 |                 32,816,004.51 |                 55,009,681.91 |                 92,810,122.57 |   14,591,666.54 | 135,214,560.77 |                   1.00 |
| reactiva            |    361,063,492.70 |    473,978,736.33 |    683,753,764.92 |                          0.00 |                          0.00 |                          0.00 |            0.00 |           0.00 |                   0.00 |

## 7) Reglas de interpretación
- `ahorro_neto_vs_reactiva > 0`: mejora económica frente a reactiva en ese escenario/sensibilidad.
- `downside_case < 0`: existe cola de riesgo económica donde la estrategia puede no compensar.
- `prob_ahorro_positivo`: robustez de la estrategia bajo incertidumbre.

## 8) Nota metodológica
No se afirma ganador universal: el resultado depende de capacidad, calidad de detección temprana y estructura de costes.