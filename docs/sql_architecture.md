# SQL Architecture

## Dialecto elegido
Se utiliza **DuckDB SQL** por su capacidad analítica local, soporte de window functions, CTEs avanzados y ejecución reproducible sobre CSV/Parquet sin infraestructura adicional.

## Convención semántica transversal
- `health_*`: alto = mejor.
- `deterioration_*` / `degradation_*` / `risk_*`: alto = peor.
- `maintenance_restoration_*`: alto = mejor recuperación.
- Referencia oficial: `docs/health_degradation_semantics.md`.

## Capas
1. `staging`: tipado, normalización de fechas y estandarización semántica.
2. `integration`: consolidación de señales, fallas, mantenimiento, alertas y disponibilidad.
3. `analytical marts`: granularidades componente-día, unidad-día y flota-semana.
4. `kpi queries`: vistas listas para explotación ejecutiva.
5. `validation queries`: controles de calidad y coherencia analítica.

## Scripts y objetivo
- `01_staging_assets.sql`: activos maestros (flota, unidad, depósito, componente).
- `02_staging_sensors.sql`: sensórica e inspección automática.
- `03_staging_maintenance.sql`: fallas, mantenimiento, alertas, backlog, operación.
- `04_integrated_component_health.sql`: inputs de salud por componente-día.
- `05_integrated_failures_and_alerts.sql`: integración fallo-alerta + repetición estructural.
- `06_integrated_availability.sql`: disponibilidad operacional consolidada.
- `07_analytical_mart_component_day.sql`: mart componente-día + `vw_component_daily_health`.
- `08_analytical_mart_unit_day.sql`: mart unidad-día + `vw_unit_operational_risk` + `vw_depot_maintenance_pressure`.
- `09_analytical_mart_fleet_week.sql`: mart flota-semana.
- `10_kpi_queries.sql`: KPIs de riesgo, saturación, indisponibilidad, inspección automática y valor CBM.
- `11_validation_queries.sql`: checks de nulls, rangos y coherencia.

## Orden de ejecución
1. staging (`01` a `03`)
2. integration (`04` a `06`)
3. marts (`07` a `09`)
4. kpis (`10`)
5. validaciones (`11`)

## Runner
La ejecución está centralizada en [run_sql_layer.py](/Users/miguelfidalgo/Documents/sistema-inteligencia-mantenimiento-ferroviario/src/run_sql_layer.py), que:
- carga `data/raw/*.csv` en tablas `raw_*`
- ejecuta scripts en orden
- exporta marts, vistas KPI y validaciones a `data/processed/`
