# Semántica Oficial de Salud, Deterioro y Degradación

## Objetivo
Eliminar ambigüedad de signos entre SQL, Python, marts, scoring, dashboard y documentación.

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

## Variables corregidas (antes/después)
- `estimated_health_input_index`:
  - Antes: se consumía como “health” en unas capas y como “deterioro” en otras.
  - Ahora: siempre health (alto=mejor); su complemento explícito es `deterioration_input_index`.
- `critical_components_at_risk` (SQL unidad-día):
  - Antes: contaba componentes con health alto.
  - Ahora: cuenta componentes con health bajo (`<=35`).
- `predicted_unavailability_risk` (SQL unidad-día):
  - Antes: término de salud con signo invertido ambiguo.
  - Ahora: usa explícitamente `100 - aggregated_health_input`.
- `degradation_velocity` (features):
  - Antes: fórmula con doble ponderación accidental de `sensor_std`.
  - Ahora: fórmula corregida y estable, alineada con deterioro.
- `impact_on_service_proxy` (features unidad):
  - Antes: saturación artificial masiva en 100.
  - Ahora: normalización por percentiles (hours/cancelaciones/puntualidad) para evitar clipping degenerado.
- `component_health_score` (scoring):
  - Antes: mezcla parcial de health/degradation con semántica no explícita.
  - Ahora: health consolidado con penalización por deterioro/degradación y mitigación por `maintenance_restoration_index`.
- `component_failure_risk_score` (scoring):
  - Antes: dominado por degradación sin controles semánticos suficientes.
  - Ahora: logit con contribuciones explícitas de deterioro, estrés, anomalías, backlog, repetitividad y restauración (signo negativo).

## Single Source of Truth (SSOT)
La definición semántica oficial se consume en:
- SQL marts y validación:
  - `sql/07_analytical_mart_component_day.sql`
  - `sql/08_analytical_mart_unit_day.sql`
  - `sql/10_kpi_queries.sql`
  - `sql/11_validation_queries.sql`
- Python:
  - `src/feature_engineering.py`
  - `src/risk_scoring.py`
  - `src/validation.py`
  - `src/build_dashboard.py`
- Diccionarios/docs:
  - `docs/sql_metric_definitions.md`
  - `docs/feature_dictionary.md`
  - `docs/diccionario_metricas.md`
  - `docs/modeling_framework.md`

## Validaciones anti-regresión implementadas
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
