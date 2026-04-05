# Diccionario de Métricas

## Convención de signos (obligatoria)
- `health_*`: mayor = mejor.
- `deterioration_*` / `degradation_*` / `risk_*`: mayor = peor.
- `maintenance_restoration_*`: mayor = mejor recuperación.

## Disponibilidad y fiabilidad
- `availability_rate`: proporción de horas disponibles sobre horas planificadas (alto=mejor).
- `fleet_availability`: disponibilidad agregada por estrategia (alto=mejor).
- `mtbf_proxy`: tiempo medio entre fallas (alto=mejor).
- `mttr_proxy`: tiempo medio de reparación (alto=peor).

## Salud, deterioro y riesgo técnico
- `component_health_score` (`health_score`): salud de componente (0-100, alto=mejor).
- `deterioration_input_index`: deterioro estructural base desde SQL mart (0-100, alto=peor).
- `deterioration_index`: deterioro derivado en feature/scoring (0-100, alto=peor).
- `degradation_velocity`: velocidad proxy de degradación (0-10, alto=peor).
- `maintenance_restoration_index`: recuperación por mantenimiento reciente (0-100, alto=mejor).
- `component_failure_risk_score` (`prob_fallo_30d`): probabilidad de falla en 30 días (0-1, alto=peor).
- `unit_unavailability_risk_score`: riesgo agregado de indisponibilidad por unidad (0-100, alto=peor).
- `riesgo_ajustado_negocio`: combinación de riesgo técnico y exposición operativa (0-100, alto=peor).
- `main_risk_driver`: señal dominante del riesgo (`degradacion`, `estres_operacion`, `anomalias`, `backlog`, `repetitividad`).

## Vida útil remanente
- `component_rul_estimate` (`rul_dias`): vida útil remanente estimada (1-365 días, alto=mejor margen).
- `confidence_rul` / `confidence_flag`: confianza del estimador RUL.

## Taller y backlog
- `intervention_priority_score` (`indice_prioridad`): prioridad integral de intervención (0-100, alto=más urgente).
- `deferral_risk_score`: riesgo acumulado de diferir intervención (0-100, alto=peor).
- `service_impact_score`: impacto esperado en servicio si no se interviene (0-100, alto=peor).
- `workshop_fit_score`: ajuste técnico-operativo del depósito (0-100, alto=mejor ajuste).
- `saturation_ratio`: presión de capacidad de taller por depósito (alto=peor).
- `backlog_physical_items`: número de pendientes físicos abiertos (alto=peor carga real).
- `backlog_overdue_items`: pendientes físicos fuera de ventana (>=14 días).
- `backlog_critical_items`: pendientes físicos críticos (edad/severidad o riesgo acumulado alto).
- `backlog_overdue_ratio`: `backlog_overdue_items / backlog_physical_items` (0-1).
- `backlog_critical_ratio`: `backlog_critical_items / backlog_physical_items` (0-1).
- `backlog_exposure_adjusted_score`: exposición compuesta backlog físico (0-100, alto=peor).
- `backlog_risk`: alias legacy de `backlog_physical_risk_accum`; mantener solo por compatibilidad histórica.
- `estado_intervencion` (scheduling): estado operativo de ejecutabilidad.
  `programada`, `programable_proxima_ventana`, `pendiente_repuesto`, `pendiente_capacidad`, `pendiente_conflicto_operativo`, `escalar_decision`.

## Regla de gobierno backlog vs diferimiento
- `backlog_*` describe pendiente físico real (cantidad, edad, criticidad).
- `deferral_risk_*` describe el riesgo de aplazar una intervención.
- Nunca usar `deferral_risk_score` para reportar backlog físico.
- Nunca usar backlog físico como sustituto directo de riesgo de diferimiento.

## Inspección automática y alerta temprana
- `inspection_defect_score_recent`: severidad reciente de defectos detectados (0-100, alto=peor).
- `defect_confidence_recent`: confianza de detección (0-1, alto=mejor confianza).
- `nivel_alerta`: `sin_alerta`, `preventiva`, `alta`, `critica`.
- `precision`, `recall` (early warning): calidad práctica de alerta temprana.

## Estrategia y valor
- `correctivas_evitables`: correctivas potencialmente evitables por enfoque CBM/inspección.
- `horas_indisponibilidad_evitables`: downtime potencialmente evitable.
- `coste_tecnico_proxy`, `coste_economico_proxy`, `coste_operativo_proxy`: capas de coste para comparación estratégica.
- `ahorro_operativo_proxy`: diferencia de coste entre reactiva y CBM.
