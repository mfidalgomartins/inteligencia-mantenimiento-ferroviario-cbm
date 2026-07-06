# Gobierno de Métricas de Pendientes

## Objetivo
Separar formalmente pendientes físicos y riesgo de diferimiento para evitar indicadores híbridos mal rotulados.

## Taxonomía oficial
1. pendientes físicos: órdenes reales abiertas.
2. pendientes vencidos: pendientes físicos fuera de ventana operativa (>=14 días).
3. pendientes críticos por edad/severidad: vencimiento severo o riesgo acumulado alto.
4. riesgo de diferimiento: probabilidad de daño al aplazar una intervención (puntuación de decisión).
5. exposición ajustada de pendientes: exposición compuesta 0-100 de los pendientes físicos.

## Regla de gobierno obligatoria
- Nunca usar `deferral_risk_score` para reportar pendientes físicos.
- Nunca usar pendientes físicos para inferir automáticamente riesgo de diferimiento sin puntuación explícita.

## Indicador oficial por decisión
| metric_id                       | categoria                              | definicion                                                                  | unidad           |   valor | uso_decision                                        |
|:--------------------------------|:---------------------------------------|:----------------------------------------------------------------------------|:-----------------|--------:|:----------------------------------------------------|
| backlog_physical_items_count    | pendientes_fisicos                     | pendientes reales abiertos                                                  | conteo           | 2056    | dimensionamiento de cola real de taller             |
| backlog_overdue_items_count     | pendientes_vencidos                    | pendientes con antigüedad >=14 días                                         | conteo           | 2011    | escalado táctico para recuperar cumplimiento        |
| backlog_critical_physical_count | pendientes_criticos_por_edad_severidad | pendientes críticos por edad/severidad o riesgo acumulado                   | conteo           | 1955    | secuenciación de intervención física prioritaria    |
| high_deferral_risk_cases_count  | riesgo_diferimiento                    | casos con puntuación de diferimiento >=70                                   | conteo           |   43    | límite de aplazamiento y ventana de entrada         |
| backlog_exposure_adjusted_mean  | exposicion_pendientes_ajustada         | exposición compuesta 0-100 (cantidad+edad+criticidad de pendientes físicos) | puntuacion_0_100 |   95.83 | priorización de depósitos y rebalanceo de capacidad |

## Uso ejecutivo
- Dirección de taller: pendientes físicos/vencidos/críticos por depósito.
- Dirección de operaciones: riesgo de diferimiento y exposición ajustada de pendientes.
- Dirección de mantenimiento: combinación de ambos para secuencia de intervención.
