# Gobierno de decisiones y modo sombra

El sistema produce recomendaciones trazables, pero no ejecuta órdenes de mantenimiento. Todas las decisiones se mantienen en modo sombra hasta superar la puerta de modelo y una transición operativa aprobada.

## Identidad y revisión

- `decision_id` es estable para fecha, unidad, componente y regla.
- Las revisiones humanas se incorporan mediante `data/raw/decision_approvals.csv`.
- Columnas exigidas: `decision_id`, `approval_status`, `reviewer_id`, `reviewed_at`, `comment`.
- Estados permitidos: `approved`, `rejected`, `escalated`.
- Una aprobación registrada no habilita ejecución automática mientras el modo sea `shadow`.

## Estado actual

| approval_status   |   decisions |
|:------------------|------------:|
| pending           |        1152 |

## Controles bloqueantes

| check                               | passed   |   observed | expected                              | publish_blocker   |
|:------------------------------------|:---------|-----------:|:--------------------------------------|:------------------|
| decision_id_unique                  | True     |          0 | 0 duplicate ids                       | True              |
| shadow_mode_enforced                | True     |          0 | 0 decisions outside shadow mode       | True              |
| automatic_execution_disabled        | True     |          0 | 0 automatically executable decisions  | True              |
| critical_decisions_require_approval | True     |       1117 | all material actions                  | True              |
| approved_decisions_have_reviewer    | True     |          0 | 0 approved decisions without reviewer | True              |
| review_event_identity_unique        | True     |          0 | 0 duplicate review events             | True              |
