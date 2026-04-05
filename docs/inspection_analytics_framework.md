# Inspection Analytics Framework

## Taxonomía técnica
- Familias objetivo: `wheel`, `brake`, `bogie`, `pantograph`.
- `family` se deriva de metadata técnica de componente (`sistema_principal`+`subsistema`+`tipo_componente`) y se contrasta contra `familia_inspeccion` reportada.

## Métricas oficiales
- `inspection_coverage` = componentes inspeccionados / componentes monitorizados.
- `defect_detection_rate` = inspecciones con defecto detectado / total inspecciones.
- `pre_failure_detection_rate` = fallas con detección previa (0-30d) / total fallas.
- `false_alert_proxy` = 1 - (detecciones con falla posterior <=30d / total detecciones).
- `confidence_adjusted_detection_value` = coverage × pre-failure rate × confianza media pre-falla × (1 - false alert).

## Coherencia temporal
- Cadena inspección -> alerta -> falla validada por timestamps por unidad/componente.
- Follow-through inspección -> mantenimiento validado para detecciones severas (<=14 días).

## Limitaciones sintéticas
- La distribución de severidad/defectos depende del generador sintético y puede estar sesgada respecto a operación real.
- Las tasas representan plausibilidad analítica y trazabilidad, no performance contractual real.

## Resultado agregado por familia
| family     |   inspection_coverage |   defect_detection_rate |   pre_failure_detection_rate |   false_alert_proxy |   confidence_adjusted_detection_value |
|:-----------|----------------------:|------------------------:|-----------------------------:|--------------------:|--------------------------------------:|
| bogie      |                     1 |                0.952178 |                     1        |            0.689567 |                              0.265084 |
| brake      |                     1 |                0.971297 |                     0.997669 |            0.575771 |                              0.362465 |
| pantograph |                     1 |                0.971407 |                     0.998104 |            0.593771 |                              0.34884  |
| wheel      |                     1 |                0.969144 |                     0.997372 |            0.608634 |                              0.334905 |

## Checks de consistencia técnica
| check                         | result   | detail                                                                   |
|:------------------------------|:---------|:-------------------------------------------------------------------------|
| rates_in_0_1                  | True     | all family rates within [0,1]                                            |
| coverage_not_above_one        | True     | max_coverage=1.0000                                                      |
| family_mapping_consistency    | True     | min_mapping_consistency=1.0000                                           |
| temporal_linkage_non_negative | True     | inspection->alert->failure and inspection->maintenance non-negative lags |
| chain_rates_in_0_1            | True     | chain/followthrough rates in [0,1]                                       |