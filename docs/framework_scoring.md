# Framework de Scoring

## Convención semántica
- `health`: alto = mejor.
- `deterioration` / `degradation` / `risk`: alto = peor.
- `maintenance_restoration`: alto = mayor recuperación.

## 1) Health score (0-100, alto=mejor)
`component_health_score` combina:
- salud base (`estimated_health_index`)
- penalización por `deterioration_index` y `degradation_velocity`
- penalización por `inspection_defect_score_recent` y backlog
- mitigación por `maintenance_restoration_index`

## 2) Probabilidad de fallo 30d (0-1, alto=peor)
Modelo logístico interpretable:
- entradas de deterioro, degradación, estrés, anomalías, backlog, repetitividad y restauración
- salida `prob_fallo_30d` acotada `[0.001, 0.985]`

## 3) Riesgo ajustado de negocio (0-100, alto=peor)
`riesgo_ajustado_negocio` integra:
- probabilidad de falla
- salud invertida
- riesgo agregado de indisponibilidad de unidad

## 4) RUL interpretable
- regresión lineal por componente en ventana de 60 días sobre `estimated_health_index`
- `component_rul_estimate` en días (1-365)
- `confidence_rul` derivada de calidad de ajuste y cobertura

## 5) Prioridad de taller
`intervention_priority_score` integra:
- riesgo técnico
- impacto de servicio
- urgencia
- ajuste de taller
- riesgo de diferimiento
