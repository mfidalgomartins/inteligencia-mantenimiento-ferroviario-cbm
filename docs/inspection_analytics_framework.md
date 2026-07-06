# Marco Analítico de Inspección Automática

## Taxonomía técnica
- Familias objetivo: `wheel`, `brake`, `bogie`, `pantograph`.
- `family` se deriva de metadatos técnicos de componente (`sistema_principal`+`subsistema`+`tipo_componente`) y se contrasta contra `familia_inspeccion` reportada.

## Métricas oficiales
- `inspection_coverage` = componentes inspeccionados / componentes monitorizados.
- `defect_detection_rate` = inspecciones con defecto detectado / total inspecciones.
- `pre_failure_detection_rate` = fallas con detección previa (0-30d) / total fallas.
- `false_alert_proxy` = 1 - (detecciones con falla posterior <=30d / total detecciones).
- `confidence_adjusted_detection_value` = cobertura × tasa previa a falla × confianza media previa a falla × (1 - falsa alerta).

## Coherencia temporal
- Cadena inspección -> alerta -> falla validada por marcas temporales por unidad/componente.
- Seguimiento inspección -> mantenimiento validado para detecciones severas (<=14 días).

## Limitaciones sintéticas
- La distribución de severidad/defectos depende del generador sintético y puede estar sesgada respecto a operación real.
- La cobertura del parque refleja una política de inspección programada; no equivale a una tasa de detección perfecta.
- `alert_chain_rate` mide trazabilidad del flujo sintético inspección-alerta, no precisión predictiva.
- Las tasas representan plausibilidad analítica y trazabilidad, no desempeño contractual real.

## Resultado agregado por familia
| family     |   inspection_coverage |   defect_detection_rate |   pre_failure_detection_rate |   false_alert_proxy |   confidence_adjusted_detection_value |
|:-----------|----------------------:|------------------------:|-----------------------------:|--------------------:|--------------------------------------:|
| bogie      |                     1 |                0.590474 |                     0.703704 |            0.892001 |                             0.06474   |
| brake      |                     1 |                0.665077 |                     0.751456 |            0.84409  |                             0.100365  |
| pantograph |                     1 |                0.666017 |                     0.748945 |            0.856801 |                             0.0920992 |
| wheel      |                     1 |                0.65877  |                     0.755507 |            0.847928 |                             0.0987808 |

## Controles de consistencia técnica
| check                               | result   | detail                                                                   |
|:------------------------------------|:---------|:-------------------------------------------------------------------------|
| rates_in_0_1                        | True     | all family rates within [0,1]                                            |
| coverage_not_above_one              | True     | max_coverage=1.0000                                                      |
| family_mapping_consistency          | True     | min_mapping_consistency=1.0000                                           |
| temporal_linkage_non_negative       | True     | inspection->alert->failure and inspection->maintenance non-negative lags |
| chain_rates_in_0_1                  | True     | chain/followthrough rates in [0,1]                                       |
| synthetic_detection_rates_plausible | True     | defect_rate_range=0.5905-0.6660; pre_failure_range=0.7037-0.7555         |
