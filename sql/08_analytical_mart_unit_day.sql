-- Objetivo: mart unidad-día y vista obligatoria de riesgo operacional
-- Gobierno de backlog:
-- - backlog_* físico = pendiente real de mantenimiento (no score de diferimiento)
-- - deferral_* = riesgo de aplazamiento en capa de priorización (fuera de este mart)

CREATE OR REPLACE TABLE mart_unit_day AS
WITH component_rollup AS (
    SELECT
        c.fecha,
        c.unidad_id,
        COUNT(*) AS component_count,
        SUM(CASE WHEN c.estimated_health_input_index <= 35 THEN 1 ELSE 0 END) AS critical_components_at_risk,
        AVG(c.estimated_health_input_index) AS aggregated_health_input,
        AVG(c.deterioration_input_index) AS aggregated_deterioration_input,
        SUM(c.failures_count) AS failures_count,
        SUM(c.failure_downtime_h) AS failure_downtime_h,
        SUM(c.alerts_count) AS alerts_count,
        SUM(c.early_alerts_count) AS early_alerts_count,
        SUM(c.critical_alerts_count) AS critical_alerts_count,
        SUM(c.maintenance_hours) AS maintenance_hours,
        AVG(c.operating_stress_index) AS operating_stress_index,
        AVG(c.repetitive_failure_flag) AS repetitive_failure_intensity
    FROM mart_component_day c
    GROUP BY c.fecha, c.unidad_id
)
SELECT
    ua.fecha,
    ua.unidad_id,
    ua.flota_id,
    ua.linea_servicio,
    cr.component_count,
    cr.critical_components_at_risk,
    cr.aggregated_health_input,
    cr.aggregated_deterioration_input,
    cr.failures_count,
    cr.failure_downtime_h,
    cr.alerts_count,
    cr.early_alerts_count,
    cr.critical_alerts_count,
    cr.maintenance_hours,
    cr.operating_stress_index,
    cr.repetitive_failure_intensity,
    ua.horas_planificadas,
    ua.horas_disponibles,
    ua.horas_no_disponibles,
    ua.cancelaciones_proxy,
    ua.puntualidad_impactada_proxy,
    ua.servicio_planificado,
    ua.servicio_realizado,
    ua.sustitucion_requerida_flag,
    ua.backlog_physical_items,
    ua.backlog_physical_risk_accum,
    ua.backlog_overdue_items,
    ua.backlog_critical_items,
    ua.backlog_overdue_ratio,
    ua.backlog_critical_ratio,
    CASE
        WHEN ua.backlog_physical_items > 0 THEN
            LEAST(
                100.0,
                (
                    ua.backlog_overdue_items * 0.75
                    + ua.backlog_critical_items * 1.20
                    + ua.backlog_physical_risk_accum / 90.0
                ) / ua.backlog_physical_items * 28.0
            )
        ELSE 0
    END AS backlog_exposure_adjusted_score,
    -- Compatibilidad legacy (mantener mientras haya consumidores antiguos).
    ua.backlog_risk,
    ua.backlog_items,
    ua.availability_rate,
    (
        (100 - cr.aggregated_health_input) / 100 * 0.30
        + LEAST(cr.critical_components_at_risk, 8) / 8 * 0.20
        + LEAST(ua.backlog_critical_ratio, 1.0) * 0.10
        + LEAST(
            CASE
                WHEN ua.backlog_physical_items > 0 THEN
                    (
                        ua.backlog_overdue_items * 0.75
                        + ua.backlog_critical_items * 1.20
                        + ua.backlog_physical_risk_accum / 90.0
                    ) / ua.backlog_physical_items * 28.0
                ELSE 0
            END,
            100.0
        ) / 100 * 0.05
        + LEAST(ua.horas_no_disponibles, 24) / 24 * 0.20
        + LEAST(ua.sustitucion_requerida_flag, 1) * 0.15
    ) AS predicted_unavailability_risk
FROM int_unit_availability ua
LEFT JOIN component_rollup cr
    ON cr.fecha = ua.fecha
    AND cr.unidad_id = ua.unidad_id;

CREATE OR REPLACE VIEW vw_unit_operational_risk AS
SELECT
    m.fecha,
    m.unidad_id,
    m.flota_id,
    m.linea_servicio,
    m.critical_components_at_risk,
    m.predicted_unavailability_risk,
    m.horas_no_disponibles AS expected_hours_out_of_service,
    m.backlog_physical_items AS associated_backlog_physical_items,
    m.backlog_overdue_items AS associated_backlog_overdue_items,
    m.backlog_critical_items AS associated_backlog_critical_items,
    m.backlog_exposure_adjusted_score AS associated_backlog_exposure_adjusted_score,
    -- Compatibilidad legacy.
    m.backlog_items AS associated_backlog_items,
    m.backlog_risk AS associated_backlog_risk,
    m.cancelaciones_proxy,
    m.sustitucion_requerida_flag,
    CASE
        WHEN m.predicted_unavailability_risk >= 0.75 THEN 'alto'
        WHEN m.predicted_unavailability_risk >= 0.50 THEN 'medio'
        ELSE 'bajo'
    END AS operational_risk_tier
FROM mart_unit_day m;

CREATE OR REPLACE VIEW vw_depot_maintenance_pressure AS
WITH depot_day AS (
    SELECT
        u.fecha,
        un.deposito_id,
        SUM(u.backlog_physical_items) AS backlog_physical_items,
        SUM(u.backlog_physical_risk_accum) AS backlog_physical_risk_accum,
        SUM(u.backlog_overdue_items) AS backlog_overdue_items,
        SUM(u.backlog_critical_items) AS backlog_critical_items,
        AVG(u.backlog_overdue_ratio) AS backlog_overdue_ratio,
        AVG(u.backlog_critical_ratio) AS backlog_critical_ratio,
        AVG(u.backlog_exposure_adjusted_score) AS backlog_exposure_adjusted_score,
        SUM(u.maintenance_hours) AS maintenance_hours,
        AVG(u.predicted_unavailability_risk) AS avg_unit_risk,
        SUM(u.horas_no_disponibles) AS total_unavailable_hours,
        SUM(u.cancelaciones_proxy) AS total_cancelaciones,
        SUM(u.sustitucion_requerida_flag) AS substitutions_required
    FROM mart_unit_day u
    INNER JOIN stg_unidades un
        ON un.unidad_id = u.unidad_id
    GROUP BY u.fecha, un.deposito_id
),
mix AS (
    SELECT
        m.fecha_inicio_dia AS fecha,
        m.deposito_id,
        AVG(m.correctiva_flag) AS corrective_share,
        AVG(m.programada_flag) AS programmed_share
    FROM stg_eventos_mantenimiento m
    GROUP BY m.fecha_inicio_dia, m.deposito_id
)
SELECT
    d.fecha,
    d.deposito_id,
    dep.nombre_deposito,
    dep.capacidad_taller,
    d.backlog_physical_items,
    d.backlog_physical_risk_accum,
    d.backlog_overdue_items,
    d.backlog_critical_items,
    d.backlog_overdue_ratio,
    d.backlog_critical_ratio,
    d.backlog_exposure_adjusted_score,
    -- Compatibilidad legacy.
    d.backlog_physical_items AS backlog_items,
    d.backlog_physical_risk_accum AS backlog_risk,
    d.maintenance_hours,
    CASE
        WHEN dep.capacidad_taller > 0 THEN
            (
                d.maintenance_hours
                + d.backlog_physical_items * 0.20
                + d.backlog_overdue_items * 0.35
                + d.backlog_critical_items * 0.75
                + d.backlog_exposure_adjusted_score * 0.12
                + d.total_unavailable_hours * 0.05
            ) / dep.capacidad_taller
        ELSE 0
    END AS saturation_ratio,
    d.avg_unit_risk,
    d.total_unavailable_hours,
    d.total_cancelaciones,
    d.substitutions_required,
    COALESCE(mx.corrective_share, 0) AS corrective_share,
    COALESCE(mx.programmed_share, 0) AS programmed_share
FROM depot_day d
INNER JOIN stg_depositos dep
    ON dep.deposito_id = d.deposito_id
LEFT JOIN mix mx
    ON mx.fecha = d.fecha
    AND mx.deposito_id = d.deposito_id;
