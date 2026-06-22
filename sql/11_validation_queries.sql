-- Objetivo: controles de calidad y coherencia para capa SQL
-- Convención: cada métrica *_issues / *_rows debe ser 0 salvo que se documente
-- como warning analítico no bloqueante.

CREATE OR REPLACE VIEW val_row_counts AS
SELECT 'mart_component_day' AS objeto, COUNT(*) AS n_rows FROM mart_component_day
UNION ALL
SELECT 'mart_unit_day', COUNT(*) FROM mart_unit_day
UNION ALL
SELECT 'mart_fleet_week', COUNT(*) FROM mart_fleet_week;

CREATE OR REPLACE VIEW val_null_rates_critical AS
SELECT
    'mart_component_day' AS objeto,
    AVG(CASE WHEN componente_id IS NULL THEN 1 ELSE 0 END) AS null_component_id,
    AVG(CASE WHEN fecha IS NULL THEN 1 ELSE 0 END) AS null_fecha,
    AVG(CASE WHEN estimated_health_input_index IS NULL THEN 1 ELSE 0 END) AS null_health_input
FROM mart_component_day
UNION ALL
SELECT
    'mart_unit_day',
    AVG(CASE WHEN unidad_id IS NULL THEN 1 ELSE 0 END),
    AVG(CASE WHEN fecha IS NULL THEN 1 ELSE 0 END),
    AVG(CASE WHEN predicted_unavailability_risk IS NULL THEN 1 ELSE 0 END)
FROM mart_unit_day;

CREATE OR REPLACE VIEW val_sensor_ranges AS
SELECT
    SUM(CASE WHEN temperatura_operacion < -20 OR temperatura_operacion > 170 THEN 1 ELSE 0 END) AS out_of_range_temp,
    SUM(CASE WHEN vibracion_proxy < 0 OR vibracion_proxy > 20 THEN 1 ELSE 0 END) AS out_of_range_vibration,
    SUM(CASE WHEN desgaste_proxy < 0 OR desgaste_proxy > 150 THEN 1 ELSE 0 END) AS out_of_range_wear
FROM stg_sensores_componentes;

CREATE OR REPLACE VIEW val_temporal_coherence AS
WITH maintenance AS (
    SELECT
        SUM(CASE WHEN fecha_inicio > fecha_fin THEN 1 ELSE 0 END) AS maintenance_negative_duration
    FROM stg_eventos_mantenimiento
),
interventions AS (
    SELECT
        SUM(CASE WHEN fecha_programada IS NULL THEN 1 ELSE 0 END) AS interventions_missing_date
    FROM stg_intervenciones_programadas
)
SELECT
    COALESCE(m.maintenance_negative_duration, 0) AS maintenance_negative_duration,
    COALESCE(i.interventions_missing_date, 0) AS interventions_missing_date
FROM maintenance m
CROSS JOIN interventions i;

CREATE OR REPLACE VIEW val_consistency_scores_actions AS
SELECT
    SUM(CASE WHEN predicted_unavailability_risk >= 0.8 AND backlog_physical_items = 0 THEN 1 ELSE 0 END) AS high_risk_without_backlog,
    SUM(CASE WHEN predicted_unavailability_risk < 0.2 AND horas_no_disponibles > 12 THEN 1 ELSE 0 END) AS low_risk_high_downtime
FROM mart_unit_day;

CREATE OR REPLACE VIEW val_backlog_semantic_consistency AS
SELECT
    SUM(CASE WHEN backlog_overdue_items > backlog_physical_items THEN 1 ELSE 0 END) AS overdue_gt_physical_rows,
    SUM(CASE WHEN backlog_critical_items > backlog_physical_items THEN 1 ELSE 0 END) AS critical_gt_physical_rows,
    SUM(CASE WHEN backlog_overdue_ratio < 0 OR backlog_overdue_ratio > 1 THEN 1 ELSE 0 END) AS overdue_ratio_out_of_bounds,
    SUM(CASE WHEN backlog_critical_ratio < 0 OR backlog_critical_ratio > 1 THEN 1 ELSE 0 END) AS critical_ratio_out_of_bounds,
    SUM(CASE WHEN backlog_exposure_adjusted_score < 0 OR backlog_exposure_adjusted_score > 100 THEN 1 ELSE 0 END) AS exposure_score_out_of_bounds
FROM mart_unit_day;

CREATE OR REPLACE VIEW val_semantic_health_deterioration AS
SELECT
    AVG(ABS((estimated_health_input_index + deterioration_input_index) - 100.0)) AS health_deterioration_balance_mae,
    SUM(CASE WHEN estimated_health_input_index < 0 OR estimated_health_input_index > 100 THEN 1 ELSE 0 END) AS health_out_of_range,
    SUM(CASE WHEN deterioration_input_index < 0 OR deterioration_input_index > 100 THEN 1 ELSE 0 END) AS deterioration_out_of_range
FROM mart_component_day;

CREATE OR REPLACE VIEW val_primary_key_uniqueness AS
WITH flotas AS (
    SELECT COUNT(*) AS n FROM (SELECT flota_id FROM stg_flotas GROUP BY flota_id HAVING COUNT(*) > 1)
),
depositos AS (
    SELECT COUNT(*) AS n FROM (SELECT deposito_id FROM stg_depositos GROUP BY deposito_id HAVING COUNT(*) > 1)
),
unidades AS (
    SELECT COUNT(*) AS n FROM (SELECT unidad_id FROM stg_unidades GROUP BY unidad_id HAVING COUNT(*) > 1)
),
componentes AS (
    SELECT COUNT(*) AS n FROM (SELECT componente_id FROM stg_componentes_criticos GROUP BY componente_id HAVING COUNT(*) > 1)
),
inspecciones AS (
    SELECT COUNT(*) AS n FROM (SELECT inspeccion_id FROM stg_inspecciones_automaticas GROUP BY inspeccion_id HAVING COUNT(*) > 1)
),
mantenimientos AS (
    SELECT COUNT(*) AS n FROM (SELECT mantenimiento_id FROM stg_eventos_mantenimiento GROUP BY mantenimiento_id HAVING COUNT(*) > 1)
),
fallas AS (
    SELECT COUNT(*) AS n FROM (SELECT falla_id FROM stg_fallas_historicas GROUP BY falla_id HAVING COUNT(*) > 1)
),
alertas AS (
    SELECT COUNT(*) AS n FROM (SELECT alerta_id FROM stg_alertas_operativas GROUP BY alerta_id HAVING COUNT(*) > 1)
),
intervenciones AS (
    SELECT COUNT(*) AS n FROM (SELECT intervencion_id FROM stg_intervenciones_programadas GROUP BY intervencion_id HAVING COUNT(*) > 1)
),
backlog AS (
    SELECT COUNT(*) AS n FROM (SELECT fecha, backlog_id FROM stg_backlog_mantenimiento GROUP BY fecha, backlog_id HAVING COUNT(*) > 1)
),
component_day AS (
    SELECT COUNT(*) AS n FROM (SELECT fecha, unidad_id, componente_id FROM mart_component_day GROUP BY fecha, unidad_id, componente_id HAVING COUNT(*) > 1)
),
unit_day AS (
    SELECT COUNT(*) AS n FROM (SELECT fecha, unidad_id FROM mart_unit_day GROUP BY fecha, unidad_id HAVING COUNT(*) > 1)
),
fleet_week AS (
    SELECT COUNT(*) AS n FROM (SELECT week_start, flota_id FROM mart_fleet_week GROUP BY week_start, flota_id HAVING COUNT(*) > 1)
)
SELECT
    (SELECT n FROM flotas) AS duplicate_flota_id,
    (SELECT n FROM depositos) AS duplicate_deposito_id,
    (SELECT n FROM unidades) AS duplicate_unidad_id,
    (SELECT n FROM componentes) AS duplicate_componente_id,
    (SELECT n FROM inspecciones) AS duplicate_inspeccion_id,
    (SELECT n FROM mantenimientos) AS duplicate_mantenimiento_id,
    (SELECT n FROM fallas) AS duplicate_falla_id,
    (SELECT n FROM alertas) AS duplicate_alerta_id,
    (SELECT n FROM intervenciones) AS duplicate_intervencion_id,
    (SELECT n FROM backlog) AS duplicate_backlog_snapshot_id,
    (SELECT n FROM component_day) AS duplicate_mart_component_day_grain,
    (SELECT n FROM unit_day) AS duplicate_mart_unit_day_grain,
    (SELECT n FROM fleet_week) AS duplicate_mart_fleet_week_grain;

CREATE OR REPLACE VIEW val_referential_integrity AS
SELECT
    (SELECT COUNT(*) FROM stg_unidades u LEFT JOIN stg_flotas f ON f.flota_id = u.flota_id WHERE f.flota_id IS NULL) AS orphan_unidades_flota,
    (SELECT COUNT(*) FROM stg_unidades u LEFT JOIN stg_depositos d ON d.deposito_id = u.deposito_id WHERE d.deposito_id IS NULL) AS orphan_unidades_deposito,
    (SELECT COUNT(*) FROM stg_componentes_criticos c LEFT JOIN stg_unidades u ON u.unidad_id = c.unidad_id WHERE u.unidad_id IS NULL) AS orphan_componentes_unidad,
    (SELECT COUNT(*) FROM stg_sensores_componentes s LEFT JOIN stg_componentes_criticos c ON c.componente_id = s.componente_id WHERE c.componente_id IS NULL OR c.unidad_id <> s.unidad_id) AS invalid_sensores_component_unit,
    (SELECT COUNT(*) FROM stg_inspecciones_automaticas i LEFT JOIN stg_componentes_criticos c ON c.componente_id = i.componente_id WHERE c.componente_id IS NULL OR c.unidad_id <> i.unidad_id) AS invalid_inspecciones_component_unit,
    (SELECT COUNT(*) FROM stg_eventos_mantenimiento m LEFT JOIN stg_componentes_criticos c ON c.componente_id = m.componente_id WHERE c.componente_id IS NULL OR c.unidad_id <> m.unidad_id) AS invalid_mantenimiento_component_unit,
    (SELECT COUNT(*) FROM stg_fallas_historicas f LEFT JOIN stg_componentes_criticos c ON c.componente_id = f.componente_id WHERE c.componente_id IS NULL OR c.unidad_id <> f.unidad_id) AS invalid_fallas_component_unit,
    (SELECT COUNT(*) FROM stg_alertas_operativas a LEFT JOIN stg_componentes_criticos c ON c.componente_id = a.componente_id WHERE c.componente_id IS NULL OR c.unidad_id <> a.unidad_id) AS invalid_alertas_component_unit,
    (SELECT COUNT(*) FROM stg_backlog_mantenimiento b LEFT JOIN stg_componentes_criticos c ON c.componente_id = b.componente_id WHERE c.componente_id IS NULL OR c.unidad_id <> b.unidad_id) AS invalid_backlog_component_unit,
    (SELECT COUNT(*) FROM stg_disponibilidad_servicio d LEFT JOIN stg_unidades u ON u.unidad_id = d.unidad_id WHERE u.unidad_id IS NULL OR u.flota_id <> d.flota_id) AS invalid_disponibilidad_unit_fleet,
    (SELECT COUNT(*) FROM stg_asignacion_servicio a LEFT JOIN stg_unidades u ON u.unidad_id = a.unidad_id WHERE u.unidad_id IS NULL) AS orphan_asignacion_unidad;

CREATE OR REPLACE VIEW val_join_cardinality AS
WITH parametros_linea_dia AS (
    SELECT COUNT(*) AS n
    FROM (
        SELECT fecha, linea_servicio
        FROM stg_parametros_operativos_contexto
        GROUP BY fecha, linea_servicio
        HAVING COUNT(*) > 1
    )
),
asignacion_unidad_dia AS (
    SELECT COUNT(*) AS n
    FROM (
        SELECT fecha, unidad_id
        FROM stg_asignacion_servicio
        GROUP BY fecha, unidad_id
        HAVING COUNT(*) > 1
    )
),
disponibilidad_unidad_dia AS (
    SELECT COUNT(*) AS n
    FROM (
        SELECT fecha, unidad_id
        FROM stg_disponibilidad_servicio
        GROUP BY fecha, unidad_id
        HAVING COUNT(*) > 1
    )
),
failures_alerts_component_dia AS (
    SELECT COUNT(*) AS n
    FROM (
        SELECT fecha, unidad_id, componente_id
        FROM int_failures_alerts
        GROUP BY fecha, unidad_id, componente_id
        HAVING COUNT(*) > 1
    )
),
depot_pressure_deposito_dia AS (
    SELECT COUNT(*) AS n
    FROM (
        SELECT fecha, deposito_id
        FROM vw_depot_maintenance_pressure
        GROUP BY fecha, deposito_id
        HAVING COUNT(*) > 1
    )
)
SELECT
    (SELECT n FROM parametros_linea_dia) AS duplicate_context_line_day_join_keys,
    (SELECT n FROM asignacion_unidad_dia) AS duplicate_assignment_unit_day_join_keys,
    (SELECT n FROM disponibilidad_unidad_dia) AS duplicate_availability_unit_day_join_keys,
    (SELECT n FROM failures_alerts_component_dia) AS duplicate_failures_alerts_component_day_join_keys,
    (SELECT n FROM depot_pressure_deposito_dia) AS duplicate_depot_pressure_day_grain;

CREATE OR REPLACE VIEW val_metric_ranges AS
SELECT
    (SELECT SUM(CASE WHEN predicted_unavailability_risk < 0 OR predicted_unavailability_risk > 1 THEN 1 ELSE 0 END) FROM mart_unit_day) AS unit_risk_out_of_range,
    (SELECT SUM(CASE WHEN availability_rate < 0 OR availability_rate > 1 THEN 1 ELSE 0 END) FROM mart_unit_day) AS availability_rate_out_of_range,
    (SELECT SUM(CASE WHEN backlog_exposure_adjusted_score < 0 OR backlog_exposure_adjusted_score > 100 THEN 1 ELSE 0 END) FROM mart_unit_day) AS backlog_exposure_out_of_range,
    (SELECT SUM(CASE WHEN saturation_ratio < 0 THEN 1 ELSE 0 END) FROM vw_depot_maintenance_pressure) AS negative_depot_saturation,
    (SELECT SUM(CASE WHEN inspection_coverage < 0 OR inspection_coverage > 1 THEN 1 ELSE 0 END) FROM kpi_inspeccion_automatica_por_familia) AS inspection_coverage_out_of_range,
    (SELECT SUM(CASE WHEN defect_detection_rate < 0 OR defect_detection_rate > 1 THEN 1 ELSE 0 END) FROM kpi_inspeccion_automatica_por_familia) AS defect_detection_rate_out_of_range,
    (SELECT SUM(CASE WHEN pre_failure_detection_rate < 0 OR pre_failure_detection_rate > 1 THEN 1 ELSE 0 END) FROM kpi_inspeccion_automatica_por_familia) AS pre_failure_detection_rate_out_of_range,
    (SELECT SUM(CASE WHEN false_alert_proxy < 0 OR false_alert_proxy > 1 THEN 1 ELSE 0 END) FROM kpi_inspeccion_automatica_por_familia) AS false_alert_proxy_out_of_range,
    (SELECT SUM(CASE WHEN confidence_adjusted_detection_value < 0 OR confidence_adjusted_detection_value > 1 THEN 1 ELSE 0 END) FROM kpi_inspeccion_automatica_por_familia) AS confidence_adjusted_detection_value_out_of_range;

CREATE OR REPLACE VIEW val_business_metric_coherence AS
SELECT
    (SELECT SUM(CASE WHEN horas_planificadas < 0 OR horas_disponibles < 0 OR horas_no_disponibles < 0 THEN 1 ELSE 0 END) FROM mart_unit_day) AS negative_service_hours,
    (SELECT SUM(CASE WHEN ABS((horas_disponibles + horas_no_disponibles) - horas_planificadas) > 1e-6 THEN 1 ELSE 0 END) FROM mart_unit_day) AS service_hours_not_reconciled,
    (SELECT SUM(CASE WHEN mtbf_proxy < 0 OR mttr_proxy < 0 THEN 1 ELSE 0 END) FROM mart_fleet_week) AS negative_reliability_proxy,
    (SELECT SUM(CASE WHEN failures_count = 0 AND mttr_proxy <> 0 THEN 1 ELSE 0 END) FROM mart_fleet_week) AS mttr_nonzero_without_failures,
    (SELECT SUM(CASE WHEN cbm_cost < 0 OR service_impact_cost_proxy < 0 THEN 1 ELSE 0 END) FROM vw_condition_based_value) AS negative_cbm_value_inputs;
