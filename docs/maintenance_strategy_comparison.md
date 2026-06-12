# Comparación de Estrategias de Mantenimiento

## Método
- Separación explícita entre evidencia observada, supuestos estructurales y proxies económicos.
- Escenarios operativos: conservador, base y agresivo.
- Sensibilidad multidimensional: coste de indisponibilidad, tasa de fallo, capacidad de taller, detección temprana, costes correctivo/preventivo.

## Resultado base (punto central)
| estrategia          |   fleet_availability |     mtbf |   mttr |   backlog_critico_fisico |   riesgo_diferimiento_alto |   correctivas_evitables |   fallas_repetitivas |   horas_indisponibilidad |   impacto_servicio_proxy |   utilizacion_taller |   intervencion_temprana_ratio |   intervencion_tardia_ratio |   coste_tecnico_proxy |   coste_economico_proxy |   coste_operativo_proxy |   coste_total_esperado |   service_hours_preserved |   ahorro_neto_vs_reactiva |   horas_servicio_preservadas_vs_reactiva |   ahorro_neto_p50_vs_reactiva |   ahorro_neto_p10_vs_reactiva |   ahorro_neto_p90_vs_reactiva |   downside_case |   prob_ahorro_positivo |   downside_case_ahorro_vs_reactiva |   rango_plausible_valor_min |   rango_plausible_valor_max |
|:--------------------|---------------------:|---------:|-------:|-------------------------:|---------------------------:|------------------------:|---------------------:|-------------------------:|-------------------------:|---------------------:|------------------------------:|----------------------------:|----------------------:|------------------------:|------------------------:|-----------------------:|--------------------------:|--------------------------:|-----------------------------------------:|------------------------------:|------------------------------:|------------------------------:|----------------:|-----------------------:|-----------------------------------:|----------------------------:|----------------------------:|
| basada_en_condicion |                96.78 | 1,254.49 |   4.71 |                 1,512.35 |                     135.37 |                  517.41 |               400.78 |                64,636.83 |                    38.50 |                43.68 |                          0.71 |                        0.29 |        160,441,754.04 |           85,644,889.47 |          246,086,643.51 |         246,086,643.51 |                 20,521.76 |            -48,700,525.36 |                                18,602.74 |                -53,777,290.47 |                -74,235,937.98 |                -44,730,808.85 |  -92,226,128.76 |                   0.00 |                     -74,235,937.98 |              -74,235,937.98 |              -44,730,808.85 |
| preventiva_rigida   |                96.67 | 1,264.51 |   4.92 |                 1,565.86 |                     134.87 |                  411.36 |               437.07 |                66,916.69 |                    38.96 |                50.68 |                          0.75 |                        0.25 |        112,215,865.90 |           88,627,867.36 |          200,843,733.26 |         200,843,733.26 |                 18,241.90 |             -3,457,615.12 |                                16,322.89 |                 -2,940,648.66 |                -11,923,746.49 |                 10,592,811.53 |  -19,817,978.96 |                   0.38 |                     -11,923,746.49 |              -11,923,746.49 |               10,592,811.53 |
| reactiva            |                95.86 | 1,093.65 |   5.29 |                 2,049.54 |                     144.58 |                    0.00 |               549.85 |                83,239.58 |                    42.22 |                42.29 |                          0.51 |                        0.49 |         87,401,296.62 |          109,984,821.53 |          197,386,118.15 |         197,386,118.15 |                  1,919.02 |                      0.00 |                                     0.00 |                          0.00 |                          0.00 |                          0.00 |            0.00 |                   0.00 |                               0.00 |                        0.00 |                        0.00 |

## Sensibilidad por escenario (P10/P50/P90)
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

## Rango plausible de valor
| estrategia          |   coste_total_p10 |   coste_total_p50 |   coste_total_p90 |   ahorro_neto_p10_vs_reactiva |   ahorro_neto_p50_vs_reactiva |   ahorro_neto_p90_vs_reactiva |   downside_case |    upside_case |   prob_ahorro_positivo |
|:--------------------|------------------:|------------------:|------------------:|------------------------------:|------------------------------:|------------------------------:|----------------:|---------------:|-----------------------:|
| basada_en_condicion |    209,322,168.02 |    252,594,604.49 |    356,084,798.44 |                -74,235,937.98 |                -53,777,290.47 |                -44,730,808.85 |  -92,226,128.76 | -34,808,335.85 |                   0.00 |
| preventiva_rigida   |    166,089,047.04 |    205,295,274.39 |    280,298,641.74 |                -11,923,746.49 |                 -2,940,648.66 |                 10,592,811.53 |  -19,817,978.96 |  28,579,053.19 |                   0.38 |
| reactiva            |    156,470,159.64 |    202,036,273.27 |    289,037,624.08 |                          0.00 |                          0.00 |                          0.00 |            0.00 |           0.00 |                   0.00 |

## Lectura ejecutiva
- En el punto base, CBM vs reactiva: disponibilidad +0.93 p.p.
- En el punto base, diferencial neto CBM vs reactiva: -48700525 EUR.
- Mejor estrategia por coste esperado (P50) en conservador: preventiva_rigida.
- Mejor estrategia por coste esperado (P50) en base: reactiva.
- Mejor estrategia por coste esperado (P50) en agresivo: reactiva.

## Limitaciones económicas
- El ahorro es proxy y depende de costes unitarios asumidos.
- El desempeño de CBM es sensible a la calidad de detección temprana y a la capacidad efectiva de taller.
- En escenarios conservadores, CBM puede perder ventaja frente a preventiva rígida si el coste de habilitación domina.

## Recomendación estratégica
Usar CBM donde la señal temprana y la capacidad de ejecución estén maduras; en contexto de baja madurez operativa,
aplicar transición híbrida con preventiva dirigida antes de escalar plenamente CBM.
