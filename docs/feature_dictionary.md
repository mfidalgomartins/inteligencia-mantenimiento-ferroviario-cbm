# Diccionario de Features

## component_day_features
- `sensor_mean`, `sensor_std`, `sensor_max`: señales observadas de condición por componente-día.
- `rolling_mean_7d`, `rolling_std_7d`, `rolling_slope`: comportamiento reciente y tendencia de degradación.
- `shock_event_count`: recuento 30d de picos anómalos de señal.
- `anomaly_count_30d`: intensidad de episodios fuera de patrón en 30 días.
- `inspection_defect_score_recent`, `defect_confidence_recent`: severidad/confianza reciente de inspección automática.
- `days_since_last_maintenance`, `days_since_last_failure`: tiempo desde intervención/falla previa.
- `maintenance_frequency_180d`: densidad de mantenimiento en 180 días.
- `repetitive_failure_flag`: indica historial de repetición de fallas.
- `age_ratio`, `cycles_ratio`: consumo relativo de vida útil por edad/ciclos.
- `deterioration_input_index`: deterioro estructural base del mart SQL (0-100, mayor es peor).
- `estimated_health_index`: índice de salud interpretable (0-100, mayor es mejor).
- `deterioration_index`: deterioro derivado de salud (0-100, mayor es peor).
- `degradation_velocity`: velocidad proxy de degradación acumulada.
- `maintenance_restoration_index`: efecto restaurativo tras mantenimiento reciente (0-100, mayor es mejor).
- `operating_stress_index`, `environment_stress_proxy`: estrés de operación y contexto externo.
- `alert_density`: concentración de alertas por componente.
- `backlog_exposure_flag`: exposición a backlog crítico en componente.

## unit_day_features
- `critical_components_at_risk`: número de componentes críticos comprometidos en unidad.
- `aggregated_health_score`: salud agregada de componentes de la unidad.
- `predicted_unavailability_risk`: riesgo proxy de indisponibilidad operacional.
- `maintenance_load_proxy`: presión combinada de mantenimiento y backlog.
- `service_exposure`: exposición operacional por carga y criticidad de servicio.
- `substitution_difficulty`: dificultad estimada de sustitución de material.
- `hours_lost_recent`: horas perdidas en ventana reciente.
- `impact_on_service_proxy`: impacto en cancelaciones/puntualidad.
- `fleet_dependency_flag`: unidad con dependencia alta para continuidad de servicio.

## fleet_week_features
- `availability_rate`, `mtbf_proxy`, `mttr_proxy`: fiabilidad/disponibilidad semanal por flota.
- `backlog_pressure`: presión de backlog agregada.
- `corrective_share`, `cbm_share`: mix de estrategia de intervención.
- `repetitive_failure_intensity`: intensidad de fallas repetitivas.
- `capacity_pressure_by_depot`: presión media de capacidad de depósitos que atienden la flota.

## workshop_priority_features
- `urgency_inputs`: urgencia técnica inmediata (salud, degradación y alertas).
- `service_impact_inputs`: impacto en servicio esperado por no intervenir.
- `technical_risk_inputs`: riesgo técnico consolidado.
- `workshop_efficiency_inputs`: ajuste de eficiencia según especialización/carga del depósito.
- `deferral_risk_inputs`: riesgo agregado de diferimiento para decisión táctica.

## Utilidad para CAF / entorno industrial ferroviario
Estas señales permiten pasar de una lógica de OTs aisladas a un esquema de priorización defendible en operación,
alineando salud de activo, disponibilidad de flota y saturación de taller con decisiones diarias de entrada a depósito.