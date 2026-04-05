-- Objetivo: mart analítico componente-día y vista obligatoria de salud

CREATE OR REPLACE TABLE mart_component_day AS
WITH base AS (
    SELECT
        h.fecha,
        h.unidad_id,
        h.componente_id,
        h.sistema_principal,
        h.subsistema,
        h.tipo_componente,
        h.criticidad_componente,
        h.age_ratio,
        h.cycles_ratio,
        h.sensor_mean,
        h.sensor_std,
        h.sensor_max,
        h.temperatura_operacion,
        h.vibracion_proxy,
        h.presion_proxy,
        h.desgaste_proxy,
        h.corriente_proxy,
        h.ruido_proxy,
        h.velocidad_operativa,
        h.carga_operativa,
        h.ambiente_externo_proxy,
        h.inspection_defect_score,
        h.inspection_confidence,
        h.inspection_defect_flag,
        h.maintenance_events,
        h.maintenance_hours,
        h.corrective_share,
        h.cbm_share,
        h.failures_count,
        h.failure_downtime_h,
        h.failure_severity_avg,
        h.repetitive_failure_flag,
        h.temperatura_ambiente,
        h.humedad,
        h.intensidad_servicio,
        h.nivel_congestion_operativa_proxy,
        COALESCE(fa.alerts_count, 0) AS alerts_count,
        COALESCE(fa.early_alerts_count, 0) AS early_alerts_count,
        COALESCE(fa.critical_alerts_count, 0) AS critical_alerts_count,
        COALESCE(fa.alerts_attended_rate, 0) AS alerts_attended_rate
    FROM int_component_daily_health_inputs h
    LEFT JOIN int_failures_alerts fa
        ON fa.fecha = h.fecha
        AND fa.unidad_id = h.unidad_id
        AND fa.componente_id = h.componente_id
)
SELECT
    b.fecha,
    b.unidad_id,
    b.componente_id,
    b.sistema_principal,
    b.subsistema,
    b.tipo_componente,
    b.criticidad_componente,
    b.age_ratio,
    b.cycles_ratio,
    b.sensor_mean,
    b.sensor_std,
    b.sensor_max,
    b.temperatura_operacion,
    b.vibracion_proxy,
    b.presion_proxy,
    b.desgaste_proxy,
    b.corriente_proxy,
    b.ruido_proxy,
    b.velocidad_operativa,
    b.carga_operativa,
    b.ambiente_externo_proxy,
    b.inspection_defect_score,
    b.inspection_confidence,
    b.inspection_defect_flag,
    b.maintenance_events,
    b.maintenance_hours,
    b.corrective_share,
    b.cbm_share,
    b.failures_count,
    b.failure_downtime_h,
    b.failure_severity_avg,
    b.repetitive_failure_flag,
    b.alerts_count,
    b.early_alerts_count,
    b.critical_alerts_count,
    b.alerts_attended_rate,
    AVG(b.sensor_mean) OVER (
        PARTITION BY b.componente_id
        ORDER BY b.fecha
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_mean_7d,
    STDDEV_SAMP(b.sensor_mean) OVER (
        PARTITION BY b.componente_id
        ORDER BY b.fecha
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_std_7d,
    b.sensor_mean - LAG(b.sensor_mean, 7) OVER (
        PARTITION BY b.componente_id
        ORDER BY b.fecha
    ) AS rolling_slope,
    ABS(
        b.sensor_mean - LAG(b.sensor_mean, 7) OVER (
            PARTITION BY b.componente_id
            ORDER BY b.fecha
        )
    ) AS rolling_abs_change_7d,
    SUM(CASE WHEN b.alerts_count > 0 THEN 1 ELSE 0 END) OVER (
        PARTITION BY b.componente_id
        ORDER BY b.fecha
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS alert_density_30d,
    SUM(b.failures_count) OVER (
        PARTITION BY b.componente_id
        ORDER BY b.fecha
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS failures_30d,
    SUM(b.maintenance_events) OVER (
        PARTITION BY b.componente_id
        ORDER BY b.fecha
        ROWS BETWEEN 179 PRECEDING AND CURRENT ROW
    ) AS maintenance_frequency_180d,
    (
        b.carga_operativa * 0.45
        + b.nivel_congestion_operativa_proxy * 0.35
        + ABS(b.temperatura_ambiente - 22) / 30 * 0.20
    ) AS operating_stress_index,
    LEAST(
        100.0,
        GREATEST(
            0.0,
            (
                LEAST(COALESCE(b.age_ratio, 0), 1.5) / 1.5 * 30.0
                + LEAST(COALESCE(b.cycles_ratio, 0), 1.5) / 1.5 * 25.0
                + LEAST(COALESCE(b.desgaste_proxy, 0), 140.0) / 140.0 * 30.0
                + LEAST(COALESCE(b.vibracion_proxy, 0), 12.0) / 12.0 * 15.0
            )
        )
    ) AS deterioration_input_index,
    100.0 - LEAST(
        100.0,
        GREATEST(
            0.0,
            (
                LEAST(COALESCE(b.age_ratio, 0), 1.5) / 1.5 * 30.0
                + LEAST(COALESCE(b.cycles_ratio, 0), 1.5) / 1.5 * 25.0
                + LEAST(COALESCE(b.desgaste_proxy, 0), 140.0) / 140.0 * 30.0
                + LEAST(COALESCE(b.vibracion_proxy, 0), 12.0) / 12.0 * 15.0
            )
        )
    ) AS estimated_health_input_index
FROM base b;

CREATE OR REPLACE VIEW vw_component_daily_health AS
SELECT
    m.fecha,
    m.unidad_id,
    m.componente_id,
    m.sistema_principal,
    m.subsistema,
    m.tipo_componente,
    m.sensor_mean,
    m.sensor_std,
    m.sensor_max,
    m.rolling_mean_7d,
    COALESCE(m.rolling_std_7d, 0) AS rolling_std_7d,
    COALESCE(m.rolling_slope, 0) AS rolling_slope,
    COALESCE(m.rolling_abs_change_7d, 0) AS rolling_abs_change_7d,
    m.operating_stress_index,
    m.deterioration_input_index,
    m.inspection_defect_score,
    m.inspection_confidence,
    m.inspection_defect_flag,
    m.failures_30d,
    m.maintenance_frequency_180d,
    m.alert_density_30d,
    m.critical_alerts_count,
    m.estimated_health_input_index,
    CASE WHEN m.critical_alerts_count > 0 THEN 1 ELSE 0 END AS critical_alert_flag,
    CASE WHEN m.failures_30d > 0 THEN 1 ELSE 0 END AS failure_history_flag
FROM mart_component_day m;
