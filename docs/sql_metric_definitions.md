# Definiciones de Métricas SQL

## Convención semántica oficial
- `health_*`: mayor valor = mejor condición.
- `deterioration_*` / `degradation_*` / `risk_*`: mayor valor = peor condición.
- `maintenance_restoration_*`: mayor valor = mayor recuperación post-mantenimiento.

## Métricas núcleo en tablas analíticas/vistas

## Reglas de validación obligatorias
- Toda métrica normalizada `*_rate`, `*_ratio`, `*_coverage` y `*_proxy` debe mantenerse en `[0,1]` salvo definición explícita distinta.
- Toda métrica `*_score`, `*_index` acotada debe mantenerse en `[0,100]`.
- Las tablas analíticas deben respetar su grano oficial: componente-día, unidad-día y flota-semana.
- La integridad unidad-componente es obligatoria en sensores, inspecciones, mantenimiento, fallas, alertas y pendientes.
- Las reglas anteriores se controlan en `sql/11_validation_queries.sql` mediante `val_primary_key_uniqueness`, `val_referential_integrity`, `val_join_cardinality`, `val_metric_ranges` y `val_business_metric_coherence`.

### `estimated_health_input_index` (mart_component_day)
Índice base de salud por componente-día (0-100, alto=mejor).
- Construcción: complemento de `deterioration_input_index`.
- Regla: `estimated_health_input_index + deterioration_input_index = 100` (tolerancia numérica).

### `deterioration_input_index` (mart_component_day)
Índice base de deterioro por componente-día (0-100, alto=peor).
- Construcción: combinación acotada de `age_ratio`, `cycles_ratio`, `desgaste_proxy`, `vibracion_proxy`.
- Uso: entrada estructural de degradación y riesgo.

### `operating_stress_index`
Índice de estrés operacional (alto=peor).
- Construcción: carga operativa + congestión + desviación térmica.
- Uso: aceleración de degradación y puntuaciones relativas de riesgo.

### `predicted_unavailability_risk`
Riesgo aproximado de indisponibilidad por unidad (0-1, alto=peor).
- Construcción: salud agregada inversa + componentes críticos comprometidos + pendientes + no disponibilidad + sustituciones.
- Grano válido: `fecha, unidad_id`.

### `saturation_ratio`
Presión de capacidad de taller por depósito (alto=peor).
- Construcción: carga diaria equivalente / capacidad.
- Uso: identificar cuellos de botella y riesgo de diferimiento.
- Grano válido: `fecha, deposito_id`.

### `backlog_physical_items` / `backlog_overdue_items` / `backlog_critical_items`
Taxonomía oficial de pendientes físicos en tablas analíticas/vistas.
- `backlog_physical_items`: pendientes reales abiertos.
- `backlog_overdue_items`: pendientes físicos con `antiguedad_backlog_dias >= 14`.
- `backlog_critical_items`: pendientes físicos críticos por edad/severidad o riesgo acumulado alto.
- Uso: capacidad de taller, cola física y priorización táctica.

### `backlog_exposure_adjusted_score`
Exposición compuesta de pendientes físicos (0-100, alto=peor).
- Construcción: combinación acotada de cantidad, vencimiento, criticidad y riesgo acumulado físico.
- Uso: priorizar depósitos y unidades por presión estructural de pendientes.

### `deferral_risk_score` (capa de priorización)
Riesgo de aplazar una intervención (0-100, alto=peor).
- Construcción: señales técnicas + servicio + RUL + contexto de taller.
- Uso: decisión de diferimiento; no representa pendientes físicos.

### `alert_density_30d`
Densidad de alertas recientes por componente (alto=peor).

### `maintenance_frequency_180d`
Frecuencia de mantenimiento por componente en 180 días.
- Métrica descriptiva de historial de intervención (sin signo de “mejor/peor” por sí sola).

### `mtbf_proxy`
Tiempo medio aproximado entre fallas por flota-semana (alto=mejor).
- Fórmula: `sum(horas_disponibles) / sum(failures_count)`; si no hay fallas, se reportan las horas disponibles observadas.
- Grano válido: `week_start, flota_id`.

### `mttr_proxy`
Tiempo medio aproximado de reparación por flota-semana (alto=peor).
- Fórmula: `sum(failure_downtime_h) / sum(failures_count)`.
- Regla: si `failures_count = 0`, `mttr_proxy = 0`.

### `backlog_risk` (alias histórico)
Alias de compatibilidad para `backlog_physical_risk_accum`.
- Restricción: no usar como indicador ejecutivo principal; preferir la taxonomía oficial de pendientes.

### `net_operational_value_proxy`
Valor operativo neto aproximado del enfoque CBM.
- Alto=mejor valor económico-operativo frente a la base inicial.

### `inspection_coverage` (`kpi_inspeccion_automatica_por_familia`)
Cobertura de inspección automática por familia técnica.
- Definición: componentes inspeccionados / componentes monitorizados.
- Rango: [0,1], alto=mejor cobertura de monitorización.

### `defect_detection_rate` (`kpi_inspeccion_automatica_por_familia`)
Tasa de detección de defecto sobre inspecciones ejecutadas.
- Definición: inspecciones con `defecto_detectado=1` / total inspecciones.
- Rango: [0,1], métrica descriptiva (no implica por sí sola mejor/peor desempeño).

### `pre_failure_detection_rate` (`kpi_inspeccion_automatica_por_familia`)
Capacidad de detectar antes de falla dentro de ventana operativa.
- Definición: fallas con detección previa (0-30 días) / total fallas.
- Rango: [0,1], alto=mejor anticipación.

### `false_alert_proxy` (`kpi_inspeccion_automatica_por_familia`)
Estimación aproximada de falsas alertas de inspección.
- Definición: `1 - (detecciones con falla <=30d / total detecciones)`.
- Rango: [0,1], alto=peor calidad de señal.

### `confidence_adjusted_detection_value` (`kpi_inspeccion_automatica_por_familia`)
Valor compuesto de detección ajustado por cobertura, anticipación, confianza y falsas alertas.
- Definición: `inspection_coverage × pre_failure_detection_rate × avg_confidence_pre_failure × (1 - false_alert_proxy)`.
- Rango: [0,1], alto=mejor valor operativo de inspección.
