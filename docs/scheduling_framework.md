# Marco de Scheduling de Taller

## Objetivo
Reducir salida no ejecutable del plan de taller sin forzar programaciones irreales.

## Diagnóstico del colapso original
- Horizonte corto (21 días) con ventanas estrictas por caso.
- Concentración de carga en pocos depósitos especializados.
- Sin carry-over controlado ni capacidad flexible explícita.
- Un único estado de no ejecución (`pendiente_capacidad`) sin distinción de causa.

## Rediseño heurístico aplicado
1. Horizonte multiperiodo: 35 días.
2. Calendario de capacidad por depósito/día: capacidad regular + bolsa flexible.
3. Bucketización por criticidad (`P1..P4`) y cola priorizada con aging.
4. Carry-over controlado por bucket para programar fuera de ventana preferida cuando es viable.
5. Candidatos de depósito (top-N por fit técnico-operativo) para aliviar cuellos.
6. Nuevos estados operativos de salida para separar causa de no ejecución.

## Estados de salida
- `programada`: asignada dentro de ventana preferida.
- `programable_proxima_ventana`: asignada fuera de ventana preferida, dentro del horizonte extendido.
- `pendiente_repuesto`: no programable por riesgo de suministro de repuesto (proxy).
- `pendiente_capacidad`: no programable por falta de capacidad en horizonte.
- `pendiente_conflicto_operativo`: no programable por ventana operativa/conflicto de servicio.
- `escalar_decision`: requiere revisión técnica/manual (alto riesgo + conflicto/información insuficiente).

## Comparación de métricas
| metric                            |   baseline_greedy_21d |   heuristica_redisenada_35d |   delta_after_minus_before |
|:----------------------------------|----------------------:|----------------------------:|---------------------------:|
| total_casos                       |        1152           |              1152           |                0           |
| programadas_pct                   |          29.774       |                12.674       |              -17.1         |
| programables_proxima_ventana_pct  |           0           |                37.587       |               37.587       |
| pendientes_total_pct              |          70.226       |                49.74        |              -20.486       |
| pendiente_capacidad_pct           |          70.226       |                46.788       |              -23.438       |
| pendiente_repuesto_pct            |           0           |                 2.951       |                2.951       |
| pendiente_conflicto_operativo_pct |           0           |                 0           |                0           |
| escalar_decision_pct              |           0           |                 0           |                0           |
| actionable_pct                    |          29.774       |                50.26        |               20.486       |
| capacidad_utilizada_pct           |          16.753       |                45.807       |               29.054       |
| horas_taller_usadas               |        1748.01        |              3955.25        |             2207.23        |
| riesgo_residual_no_atendido_pct   |          69.025       |                44.856       |              -24.169       |
| valor_capturado_proxy             |           6.23653e+06 |                 7.27594e+06 |                1.03942e+06 |
| valor_no_capturado_proxy          |           8.76899e+06 |                 7.72957e+06 |               -1.03942e+06 |

## Cuellos de botella principales (baseline)
| deposito_id   |   casos |   horas_requeridas |   pendientes_capacidad |   pending_rate_pct |
|:--------------|--------:|-------------------:|-----------------------:|-------------------:|
| DEP01         |     864 |           4471.54  |                    660 |              76.39 |
| DEP10         |     144 |            830.311 |                     87 |              60.42 |
| DEP08         |     144 |            699.295 |                     62 |              43.06 |

## Distribución de estados del plan rediseñado
| estado_intervencion         |   share_pct |
|:----------------------------|------------:|
| pendiente_capacidad         |      46.788 |
| programable_proxima_ventana |      37.587 |
| programada                  |      12.674 |
| pendiente_repuesto          |       2.951 |

## Trade-offs introducidos
- Mayor capacidad de ejecución mediante carry-over y flexibilidad controlada.
- Posible reasignación de depósito con coste logístico implícito (no modelado en detalle).
- Mayor accionabilidad a cambio de complejidad heurística moderada.

## Limitaciones del enfoque heurístico
- No garantiza optimalidad global multiobjetivo.
- Modela repuestos y conflicto operativo con proxies, no con ERP real.
- No incorpora secuenciación fina de recursos técnicos por skill-hora.

## Cuándo usar optimización formal
- Si la red opera con saturación estructural persistente >85%.
- Si hay restricciones duras de SLA/seguridad en múltiples depósitos simultáneos.
- Si se necesita minimización explícita de coste+risk con constraints de recursos/repuestos.
- Si se requiere plan robusto multi-semana con replanificación automática.
