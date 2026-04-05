-- Objetivo: controles de calidad y coherencia para capa SQL

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
SELECT
    SUM(CASE WHEN fecha_inicio > fecha_fin THEN 1 ELSE 0 END) AS maintenance_negative_duration,
    SUM(CASE WHEN fecha_programada IS NULL THEN 1 ELSE 0 END) AS interventions_missing_date
FROM stg_eventos_mantenimiento m
LEFT JOIN stg_intervenciones_programadas i
    ON i.componente_id = m.componente_id
    AND i.unidad_id = m.unidad_id;

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
