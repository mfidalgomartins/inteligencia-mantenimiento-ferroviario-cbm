# Arquitectura SQL

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

## Contratos de grano y joins
- `mart_component_day`: una fila por `fecha, unidad_id, componente_id`.
- `mart_unit_day`: una fila por `fecha, unidad_id`.
- `mart_fleet_week`: una fila por `week_start, flota_id`.
- `vw_depot_maintenance_pressure`: una fila por `fecha, deposito_id`.
- Joins críticos:
  - sensores, inspecciones, mantenimiento, fallas y alertas se agregan antes de unirse al grano `fecha, unidad_id, componente_id`.
  - disponibilidad y asignación se unen al grano `fecha, unidad_id`.
  - contexto operativo se une por `fecha, linea_servicio`; `val_join_cardinality` vigila duplicados en esa clave.

## Validaciones SQL
- `val_row_counts`: confirma que los marts principales no quedan vacíos.
- `val_null_rates_critical`: nulls en claves y métricas críticas.
- `val_sensor_ranges`: rangos físicos básicos de temperatura, vibración y desgaste.
- `val_temporal_coherence`: fechas de mantenimiento e intervenciones sin join multiplicativo.
- `val_backlog_semantic_consistency`: coherencia de backlog físico, ratios y exposición.
- `val_semantic_health_deterioration`: balance obligatorio salud + deterioro = 100.
- `val_primary_key_uniqueness`: duplicados en claves naturales y granos de marts.
- `val_referential_integrity`: órfãos y inconsistencias unidad/componente/flota.
- `val_join_cardinality`: claves de join que pueden duplicar filas aguas abajo.
- `val_metric_ranges`: límites de métricas normalizadas `[0,1]` y `[0,100]`.
- `val_business_metric_coherence`: reconciliación de horas de servicio y proxies de fiabilidad/valor.

## Performance e índices sugeridos
DuckDB ejecuta esta capa en modo analítico local y no necesita índices persistentes para el volumen sintético actual. Si se porta a PostgreSQL, SQL Server o un warehouse con tablas físicas, priorizar índices/cluster keys en:
- `stg_sensores_componentes(fecha, unidad_id, componente_id)`
- `stg_inspecciones_automaticas(fecha, unidad_id, componente_id)`
- `stg_eventos_mantenimiento(fecha_inicio_dia, unidad_id, componente_id)`
- `stg_fallas_historicas(fecha_falla, unidad_id, componente_id)`
- `stg_alertas_operativas(fecha, unidad_id, componente_id)`
- `stg_disponibilidad_servicio(fecha, unidad_id)`
- `stg_asignacion_servicio(fecha, unidad_id)`
- `stg_backlog_mantenimiento(fecha, unidad_id)`
- `stg_parametros_operativos_contexto(fecha, linea_servicio)`

Para tablas grandes, materializar los CTEs diarios de sensores, inspecciones, mantenimiento y fallas antes de construir `mart_component_day`.

## Runner
La ejecución está centralizada en `src/run_sql_layer.py`, que:
- carga `data/raw/*.csv` en tablas `raw_*`
- ejecuta scripts en orden
- exporta marts, vistas KPI y validaciones a `data/processed/`
