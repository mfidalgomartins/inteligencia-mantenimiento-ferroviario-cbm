# Score Calibration Hardening

## Objetivo
Reducir saturación, evitar colapso de clases y recuperar discriminación operativa en scoring.

## Cambios aplicados
- Reescalado de señales base de choque/anomalía/backlog para evitar activación estructural.
- Recalibración de `component_failure_risk_score` con mezcla de percentiles global+familia.
- Umbrales de recomendación diferenciados por familia de componente.
- Reglas de early warning adaptativas por familia (riesgo/salud/RUL).
- Nuevos checks de saturación y colapso en validación y tests.

## Before vs After (resumen cuantitativo)
| metric                   |   mean_before |   mean_after |   p95_before |   p95_after |   delta_entropy_bin10 |
|:-------------------------|--------------:|-------------:|-------------:|------------:|----------------------:|
| health_score             |     29.9174   |    47.5337   |    37.88     |   56.298    |             0.0586024 |
| failure_risk             |      0.843454 |     0.373139 |     0.946125 |    0.828049 |             0.0859421 |
| impact_on_service_proxy  |     53.4705   |    53.4705   |    95.3937   |   95.3937   |             0         |
| unit_unavailability_risk |     84.0238   |    43.8962   |    94.5741   |   56.8336   |             0.0958102 |

## Saturation / Collapse Checks
| check                                    |   value_before |   value_after |      delta |
|:-----------------------------------------|---------------:|--------------:|-----------:|
| collapse_action_dominant_share           |       0.982639 |    0.392361   |  -0.590278 |
| collapse_driver_dominant_share           |       0.741319 |    0.490451   |  -0.250868 |
| rank_discrimination_top10_bottom10_ratio |     nan        |    4.79       | nan        |
| saturation_failure_risk_ge_0_90          |       0.255208 |    0.00607639 |  -0.249132 |
| saturation_impact_ge_95                  |       0.0625   |    0.0625     |   0        |
| saturation_unit_risk_ge_90               |       0.159722 |    0          |  -0.159722 |

## Interpretabilidad recuperada
- Las clases altas/críticas dejan de ser estructurales.
- `main_risk_driver` y `recommended_action_initial` muestran diversidad utilizable.
- La separación por ranking de riesgo vuelve a ser operativamente accionable.