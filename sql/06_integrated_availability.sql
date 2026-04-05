-- Objetivo: unificar desempeño de disponibilidad con operación y presión de mantenimiento

CREATE OR REPLACE VIEW int_unit_availability AS
WITH backlog_day AS (
    SELECT
        b.fecha,
        b.unidad_id,
        COUNT(*) AS backlog_physical_items,
        SUM(b.riesgo_acumulado) AS backlog_physical_risk_accum,
        SUM(CASE WHEN b.antiguedad_backlog_dias >= 14 THEN 1 ELSE 0 END) AS backlog_overdue_items,
        SUM(
            CASE
                WHEN b.antiguedad_backlog_dias >= 21 AND LOWER(COALESCE(b.severidad_pendiente, 'baja')) IN ('alta', 'critica') THEN 1
                WHEN COALESCE(b.riesgo_acumulado, 0) >= 70 THEN 1
                ELSE 0
            END
        ) AS backlog_critical_items
    FROM stg_backlog_mantenimiento b
    GROUP BY b.fecha, b.unidad_id
),
maintenance_day AS (
    SELECT
        m.fecha_inicio_dia AS fecha,
        m.unidad_id,
        SUM(m.horas_taller) AS maintenance_hours,
        AVG(m.correctiva_flag) AS maintenance_corrective_share
    FROM stg_eventos_mantenimiento m
    GROUP BY m.fecha_inicio_dia, m.unidad_id
)
SELECT
    d.fecha,
    d.unidad_id,
    d.flota_id,
    d.linea_servicio,
    d.horas_planificadas,
    d.horas_disponibles,
    d.horas_no_disponibles,
    d.cancelaciones_proxy,
    d.puntualidad_impactada_proxy,
    a.servicio_planificado,
    a.servicio_realizado,
    a.sustitucion_requerida_flag,
    COALESCE(bd.backlog_physical_items, 0) AS backlog_physical_items,
    COALESCE(bd.backlog_physical_risk_accum, 0) AS backlog_physical_risk_accum,
    COALESCE(bd.backlog_overdue_items, 0) AS backlog_overdue_items,
    COALESCE(bd.backlog_critical_items, 0) AS backlog_critical_items,
    CASE
        WHEN COALESCE(bd.backlog_physical_items, 0) > 0 THEN COALESCE(bd.backlog_overdue_items, 0) / bd.backlog_physical_items
        ELSE 0
    END AS backlog_overdue_ratio,
    CASE
        WHEN COALESCE(bd.backlog_physical_items, 0) > 0 THEN COALESCE(bd.backlog_critical_items, 0) / bd.backlog_physical_items
        ELSE 0
    END AS backlog_critical_ratio,
    -- Compatibilidad legacy (mantener hasta retirar consumidores previos).
    COALESCE(bd.backlog_physical_risk_accum, 0) AS backlog_risk,
    COALESCE(bd.backlog_physical_items, 0) AS backlog_items,
    COALESCE(md.maintenance_hours, 0) AS maintenance_hours,
    COALESCE(md.maintenance_corrective_share, 0) AS maintenance_corrective_share,
    CASE
        WHEN d.horas_planificadas > 0 THEN d.horas_disponibles / d.horas_planificadas
        ELSE 0
    END AS availability_rate
FROM stg_disponibilidad_servicio d
LEFT JOIN stg_asignacion_servicio a
    ON a.fecha = d.fecha
    AND a.unidad_id = d.unidad_id
LEFT JOIN backlog_day bd
    ON bd.fecha = d.fecha
    AND bd.unidad_id = d.unidad_id
LEFT JOIN maintenance_day md
    ON md.fecha = d.fecha
    AND md.unidad_id = d.unidad_id;
