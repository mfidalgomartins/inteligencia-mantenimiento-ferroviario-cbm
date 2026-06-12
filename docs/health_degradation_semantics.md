# Semántica Oficial de Salud, Deterioro y Degradación

## Objetivo
Mantener una convención de signos única entre SQL, Python, marts, scoring, dashboard y documentación.

## Taxonomía oficial

### 1) Health metrics (alto = mejor)
- `estimated_health_input_index` (SQL mart)
- `estimated_health_index` (feature layer)
- `component_health_score` / `health_score` (scoring)

### 2) Deterioration metrics (alto = peor)
- `deterioration_input_index` (SQL mart y features)
- `deterioration_index` (features y scoring)
- `degradation_velocity` (features, scoring y priorización)

### 3) Stress metrics (alto = peor)
- `operating_stress_index`
- `environment_stress_proxy`
- `shock_event_count`, `anomaly_count_30d`
- `backlog_exposure_flag`

### 4) Failure propensity metrics (alto = peor)
- `component_failure_risk_score` / `prob_fallo_30d`
- `unit_unavailability_risk_score`
- `predicted_unavailability_risk`
- `deferral_risk_score`

### 5) Maintenance restoration metrics (alto = mejor)
- `maintenance_restoration_index`

## Reglas obligatorias de signo y rango
- `health_*` en `[0,100]`, mayor = mejor.
- `deterioration_*` en `[0,100]`, mayor = peor.
- `degradation_velocity` en `[0,10]`, mayor = peor.
- `maintenance_restoration_index` en `[0,100]`, mayor = mayor recuperación.
- `prob_fallo_30d` en `[0,1]`, mayor = peor.
- `unit_unavailability_risk_score`, `intervention_priority_score`, `deferral_risk_score` en `[0,100]`, mayor = peor.
- Balance SQL obligatorio: `estimated_health_input_index + deterioration_input_index = 100` (tolerancia numérica).

## Reglas de cálculo clave
- `estimated_health_input_index` siempre representa salud; su complemento explícito es `deterioration_input_index`.
- `critical_components_at_risk` cuenta componentes con salud baja (`<=35`).
- `predicted_unavailability_risk` usa explícitamente `100 - aggregated_health_input`.
- `component_health_score` penaliza deterioro y degradación, y reconoce restauración reciente.
- `component_failure_risk_score` combina deterioro, estrés, anomalías, backlog, repetitividad y restauración.

## Fuentes oficiales
La definición semántica oficial se consume en:
- SQL marts y validación:
  - `sql/07_analytical_mart_component_day.sql`
  - `sql/08_analytical_mart_unit_day.sql`
  - `sql/10_kpi_queries.sql`
  - `sql/11_validation_queries.sql`
- Python:
  - `src/feature_engineering.py`
  - `src/risk_scoring.py`
  - `src/build_dashboard.py`
- Diccionarios/docs:
  - `docs/sql_metric_definitions.md`
  - `docs/feature_dictionary.md`
  - `docs/diccionario_metricas.md`
  - `docs/modeling_framework.md`

## Validaciones implementadas
- Test SQL: `val_semantic_health_deterioration`:
  - balance health+deterioration y rangos.
- Test de signos:
  - `rho(health_score, prob_fallo_30d) < 0`
  - `rho(deterioration_index, prob_fallo_30d) > 0`
  - `rho(maintenance_restoration_index, prob_fallo_30d) < 0`
- Test de monotonía:
  - riesgo medio creciente por terciles de deterioro.
- Test de saturación:
  - control de saturación en `impact_on_service_proxy`.

## Nota metodológica
El sistema sigue siendo interpretable y proxy (dato sintético). La semántica ya es consistente entre capas, pero la calibración final de umbrales/pesos debe hacerse con histórico real de operación y mantenimiento.
