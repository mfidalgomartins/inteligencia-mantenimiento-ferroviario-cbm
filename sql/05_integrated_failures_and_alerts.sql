-- Objetivo: integrar alertas con fallas para patrones de repetición y valor temprano

CREATE OR REPLACE VIEW int_failures_alerts AS
WITH alerts_day AS (
    SELECT
        a.fecha,
        a.unidad_id,
        a.componente_id,
        COUNT(a.alerta_id) AS alerts_count,
        SUM(CASE WHEN a.alerta_temprana_flag = 1 THEN 1 ELSE 0 END) AS early_alerts_count,
        SUM(CASE WHEN a.severidad = 'critica' THEN 1 ELSE 0 END) AS critical_alerts_count,
        AVG(CASE WHEN a.atendida_flag = 1 THEN 1.0 ELSE 0.0 END) AS alerts_attended_rate
    FROM stg_alertas_operativas a
    GROUP BY a.fecha, a.unidad_id, a.componente_id
),
failures_day AS (
    SELECT
        f.fecha_falla AS fecha,
        f.unidad_id,
        f.componente_id,
        COUNT(f.falla_id) AS failures_count,
        SUM(f.tiempo_fuera_servicio_horas) AS downtime_h,
        AVG(f.severidad_falla) AS avg_failure_severity,
        MAX(f.repetitiva_flag) AS repetitive_failure_flag
    FROM stg_fallas_historicas f
    GROUP BY f.fecha_falla, f.unidad_id, f.componente_id
)
SELECT
    COALESCE(ad.fecha, fd.fecha) AS fecha,
    COALESCE(ad.unidad_id, fd.unidad_id) AS unidad_id,
    COALESCE(ad.componente_id, fd.componente_id) AS componente_id,
    COALESCE(ad.alerts_count, 0) AS alerts_count,
    COALESCE(ad.early_alerts_count, 0) AS early_alerts_count,
    COALESCE(ad.critical_alerts_count, 0) AS critical_alerts_count,
    COALESCE(ad.alerts_attended_rate, 0) AS alerts_attended_rate,
    COALESCE(fd.failures_count, 0) AS failures_count,
    COALESCE(fd.downtime_h, 0) AS downtime_h,
    COALESCE(fd.avg_failure_severity, 0) AS avg_failure_severity,
    COALESCE(fd.repetitive_failure_flag, 0) AS repetitive_failure_flag
FROM alerts_day ad
FULL OUTER JOIN failures_day fd
    ON ad.fecha = fd.fecha
    AND ad.unidad_id = fd.unidad_id
    AND ad.componente_id = fd.componente_id;

CREATE OR REPLACE VIEW vw_failure_repetition_patterns AS
SELECT
    c.subsistema,
    c.tipo_componente,
    f.modo_falla,
    f.unidad_id,
    f.componente_id,
    COUNT(f.falla_id) AS failure_events,
    SUM(f.repetitiva_flag) AS repetitive_events,
    SUM(f.tiempo_fuera_servicio_horas) AS downtime_total_h,
    AVG(f.severidad_falla) AS severity_mean,
    CASE
        WHEN COUNT(f.falla_id) >= 6 AND SUM(f.repetitiva_flag) >= 3 THEN 'alto_riesgo_estructural'
        WHEN COUNT(f.falla_id) >= 3 THEN 'riesgo_medio'
        ELSE 'riesgo_bajo'
    END AS structural_risk_segment
FROM stg_fallas_historicas f
INNER JOIN stg_componentes_criticos c
    ON c.componente_id = f.componente_id
GROUP BY
    c.subsistema,
    c.tipo_componente,
    f.modo_falla,
    f.unidad_id,
    f.componente_id;
