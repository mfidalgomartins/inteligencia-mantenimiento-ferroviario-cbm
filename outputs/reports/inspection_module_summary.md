# Módulo de Inspección Automática

## Valor observado
- Componentes con prioridad por inspección: 440
- Correctivas potencialmente evitables: 167.2
- Horas de indisponibilidad evitables: 869.4

## Familias más útiles
| family     |   fallas_con_deteccion_previa |   lead_time_medio_dias |   score_defecto_medio |   confianza_media |   fallas_totales |   coverage_pre_falla |
|:-----------|------------------------------:|-----------------------:|----------------------:|------------------:|-----------------:|---------------------:|
| wheel      |                          2546 |                14.9499 |               99.2343 |          0.856708 |             1522 |             1        |
| pantograph |                          1579 |                14.952  |               99.7695 |          0.858513 |             1582 |             0.998104 |
| brake      |                          1712 |                14.856  |               99.8295 |          0.85828  |             1716 |             0.997669 |
| bogie      |                          4572 |                14.9236 |               99.2229 |          0.855923 |             5607 |             0.815409 |

## Integración con scoring
Las señales de defectos y confianza alimentan `component_health_score`, `component_failure_risk_score`
y la priorización de inspección manual/intervención en taller.