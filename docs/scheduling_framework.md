# Scheduling Framework

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

## Métricas Before/After
| metric                            |   baseline_greedy_21d |   heuristica_redisenada_35d |   delta_after_minus_before |
|:----------------------------------|----------------------:|----------------------------:|---------------------------:|
| total_casos                       |        1152           |              1152           |                0           |
| programadas_pct                   |          30.556       |                19.271       |              -11.285       |
| programables_proxima_ventana_pct  |           0           |                31.684       |               31.684       |
| pendientes_total_pct              |          69.444       |                49.045       |              -20.399       |
| pendiente_capacidad_pct           |          69.444       |                44.965       |              -24.479       |
| pendiente_repuesto_pct            |           0           |                 4.08        |                4.08        |
| pendiente_conflicto_operativo_pct |           0           |                 0           |                0           |
| escalar_decision_pct              |           0           |                 0           |                0           |
| actionable_pct                    |          30.556       |                50.955       |               20.399       |
| capacidad_utilizada_pct           |          17.471       |                46.748       |               29.277       |
| horas_taller_usadas               |        1822.96        |              4003.55        |             2180.6         |
| riesgo_residual_no_atendido_pct   |          66.915       |                44.253       |              -22.662       |
| valor_capturado_proxy             |           6.78625e+06 |                 7.92745e+06 |                1.14121e+06 |
| valor_no_capturado_proxy          |           8.99948e+06 |                 7.85827e+06 |               -1.14121e+06 |

## Cuellos de botella principales (baseline)
| deposito_id   |   casos |   horas_requeridas |   pendientes_capacidad |   pending_rate_pct |
|:--------------|--------:|-------------------:|-----------------------:|-------------------:|
| DEP01         |     864 |           4572.9   |                    652 |              75.46 |
| DEP10         |     144 |            882.446 |                     89 |              61.81 |
| DEP08         |     144 |            752.034 |                     59 |              40.97 |

## Distribución de estados (after)
| estado_intervencion         |   share_pct |
|:----------------------------|------------:|
| pendiente_capacidad         |      44.965 |
| programable_proxima_ventana |      31.684 |
| programada                  |      19.271 |
| pendiente_repuesto          |       4.08  |

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