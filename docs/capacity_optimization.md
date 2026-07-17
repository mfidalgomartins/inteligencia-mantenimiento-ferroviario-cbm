# Gobierno de capacidad y optimización

La heurística diaria sigue siendo la propuesta operativa base. La optimización formal sólo se activa cuando la utilización supera 85%, la saturación es persistente y existen casos pendientes por capacidad.

## Puerta de activación

| gate_name                    | formal_optimization_required   |   max_daily_utilization |   saturated_depot_day_share |   pending_capacity_cases |   saturation_trigger |   minimum_saturated_day_share | trigger_reason                             |
|:-----------------------------|:-------------------------------|------------------------:|----------------------------:|-------------------------:|---------------------:|------------------------------:|:-------------------------------------------|
| formal_capacity_optimization | True                           |                0.999957 |                    0.285714 |                      539 |                 0.85 |                           0.1 | persistent_saturation_and_pending_capacity |

## Pruebas de estrés

| scenario             |   demand_factor |   capacity_factor |   demand_hours |   available_hours |   load_ratio |   capacity_gap_hours | stress_breach   |
|:---------------------|----------------:|------------------:|---------------:|------------------:|-------------:|---------------------:|:----------------|
| baseline             |            1    |               1   |        6389.1  |           8634.55 |     0.739946 |             2245.45  | False           |
| demand_plus_15pct    |            1.15 |               1   |        7347.47 |           8634.55 |     0.850938 |             1287.08  | True            |
| capacity_minus_10pct |            1    |               0.9 |        6389.1  |           7771.1  |     0.822162 |             1381.99  | False           |
| combined_stress      |            1.15 |               0.9 |        7347.47 |           7771.1  |     0.945487 |              423.629 | True            |

## Resultado formal

El MILP depósito-semana asignó 85.5% de los casos respetando capacidad finita.
Su salida es una recomendación de capacidad en modo sombra; no sustituye la secuenciación diaria, la validación de repuestos ni la aprobación humana.
