-- Objetivo: mart flota-semana para seguimiento estratégico

CREATE OR REPLACE TABLE mart_fleet_week AS
WITH weekly AS (
    SELECT
        DATE_TRUNC('week', u.fecha) AS week_start,
        u.flota_id,
        AVG(u.availability_rate) AS availability_rate,
        SUM(u.failures_count) AS failures_count,
        SUM(u.failure_downtime_h) AS downtime_h,
        SUM(u.maintenance_hours) AS maintenance_hours,
        AVG(u.repetitive_failure_intensity) AS repetitive_failure_intensity,
        AVG(u.predicted_unavailability_risk) AS avg_unavailability_risk,
        SUM(u.backlog_physical_items) AS backlog_physical_items,
        SUM(u.backlog_physical_risk_accum) AS backlog_physical_risk_accum,
        SUM(u.backlog_overdue_items) AS backlog_overdue_items,
        SUM(u.backlog_critical_items) AS backlog_critical_items,
        AVG(u.backlog_exposure_adjusted_score) AS backlog_exposure_adjusted_score
    FROM mart_unit_day u
    GROUP BY DATE_TRUNC('week', u.fecha), u.flota_id
),
maintenance_mix AS (
    SELECT
        DATE_TRUNC('week', m.fecha_inicio_dia) AS week_start,
        un.flota_id,
        AVG(m.correctiva_flag) AS corrective_share,
        AVG(m.basada_en_condicion_flag) AS cbm_share,
        AVG(m.programada_flag) AS programmed_share
    FROM stg_eventos_mantenimiento m
    INNER JOIN stg_unidades un
        ON un.unidad_id = m.unidad_id
    GROUP BY DATE_TRUNC('week', m.fecha_inicio_dia), un.flota_id
)
SELECT
    w.week_start,
    w.flota_id,
    w.availability_rate,
    CASE
        WHEN w.failures_count > 0 THEN (7 * 24) / w.failures_count
        ELSE 7 * 24
    END AS mtbf_proxy,
    CASE
        WHEN w.failures_count > 0 THEN w.downtime_h / w.failures_count
        ELSE 0
    END AS mttr_proxy,
    w.backlog_physical_items,
    w.backlog_physical_risk_accum,
    w.backlog_overdue_items,
    w.backlog_critical_items,
    w.backlog_exposure_adjusted_score,
    -- Compatibilidad legacy.
    w.backlog_physical_items AS backlog_items,
    w.backlog_physical_risk_accum AS backlog_risk,
    w.downtime_h,
    w.maintenance_hours,
    w.repetitive_failure_intensity,
    w.avg_unavailability_risk,
    COALESCE(mm.corrective_share, 0) AS corrective_share,
    COALESCE(mm.cbm_share, 0) AS cbm_share,
    COALESCE(mm.programmed_share, 0) AS programmed_share
FROM weekly w
LEFT JOIN maintenance_mix mm
    ON mm.week_start = w.week_start
    AND mm.flota_id = w.flota_id;
