# SQL Metric Definitions

## Convención semántica oficial
- `health_*`: mayor valor = mejor condición.
- `deterioration_*` / `degradation_*` / `risk_*`: mayor valor = peor condición.
- `maintenance_restoration_*`: mayor valor = mayor recuperación post-mantenimiento.

## Métricas núcleo en marts/vistas

### `estimated_health_input_index` (mart_component_day)
Índice base de salud por componente-día (0-100, alto=mejor).
- Construcción: complemento de `deterioration_input_index`.
- Regla: `estimated_health_input_index + deterioration_input_index = 100` (tolerancia numérica).

### `deterioration_input_index` (mart_component_day)
Índice base de deterioro por componente-día (0-100, alto=peor).
- Construcción: combinación acotada de `age_ratio`, `cycles_ratio`, `desgaste_proxy`, `vibracion_proxy`.
- Uso: input estructural de degradación y riesgo.

### `operating_stress_index`
Índice de estrés operacional (alto=peor).
- Construcción: carga operativa + congestión + desviación térmica.
- Uso: aceleración de degradación y probabilidad de falla.

### `predicted_unavailability_risk`
Riesgo proxy de indisponibilidad por unidad (0-1, alto=peor).
- Construcción: salud agregada inversa + componentes críticos comprometidos + backlog + no disponibilidad + sustituciones.

### `saturation_ratio`
Presión de capacidad de taller por depósito (alto=peor).
- Construcción: carga diaria equivalente / capacidad.
- Uso: identificar cuellos de botella y riesgo de diferimiento.

### `backlog_physical_items` / `backlog_overdue_items` / `backlog_critical_items`
Taxonomía oficial de backlog físico en marts/vistas.
- `backlog_physical_items`: pendientes reales abiertos.
- `backlog_overdue_items`: pendientes físicos con `antiguedad_backlog_dias >= 14`.
- `backlog_critical_items`: pendientes físicos críticos por edad/severidad o riesgo acumulado alto.
- Uso: capacidad de taller, cola física y priorización táctica.

### `backlog_exposure_adjusted_score`
Exposición compuesta del backlog físico (0-100, alto=peor).
- Construcción: combinación acotada de cantidad, vencimiento, criticidad y riesgo acumulado físico.
- Uso: priorizar depósitos y unidades por presión estructural de backlog.

### `deferral_risk_score` (capa de priorización)
Riesgo de aplazar una intervención (0-100, alto=peor).
- Construcción: señales técnicas + servicio + RUL + contexto de taller.
- Uso: decisión de diferimiento; no representa backlog físico.

### `alert_density_30d`
Densidad de alertas recientes por componente (alto=peor).

### `maintenance_frequency_180d`
Frecuencia de mantenimiento por componente en 180 días.
- Métrica descriptiva de historial de intervención (sin signo de “mejor/peor” por sí sola).

### `mtbf_proxy`
Tiempo medio entre fallas por flota-semana (alto=mejor).

### `mttr_proxy`
Tiempo medio de reparación por flota-semana (alto=peor).

### `backlog_risk` (alias legacy)
Alias de compatibilidad para `backlog_physical_risk_accum`.
- Restricción: no usar como KPI ejecutivo principal; preferir taxonomía oficial de backlog.

### `net_operational_value_proxy`
Valor operativo neto proxy del enfoque CBM.
- Alto=mejor valor económico-operativo frente a baseline.

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
Proxy de falsas alertas de inspección.
- Definición: `1 - (detecciones con falla <=30d / total detecciones)`.
- Rango: [0,1], alto=peor calidad de señal.

### `confidence_adjusted_detection_value` (`kpi_inspeccion_automatica_por_familia`)
Valor compuesto de detección ajustado por cobertura, anticipación, confianza y falsas alertas.
- Definición: `inspection_coverage × pre_failure_detection_rate × avg_confidence_pre_failure × (1 - false_alert_proxy)`.
- Rango: [0,1], alto=mejor valor operativo de inspección.
