-- Objetivo: integrar sensores + activos + contexto + inspecciones + mantenimiento + fallas

CREATE OR REPLACE VIEW int_component_daily_health_inputs AS
WITH sensor_day AS (
    SELECT
        s.fecha,
        s.unidad_id,
        s.componente_id,
        AVG(s.valor_sensor) AS sensor_mean,
        STDDEV_SAMP(s.valor_sensor) AS sensor_std,
        MAX(s.valor_sensor) AS sensor_max,
        AVG(s.temperatura_operacion) AS temperatura_operacion,
        AVG(s.vibracion_proxy) AS vibracion_proxy,
        AVG(s.presion_proxy) AS presion_proxy,
        AVG(s.desgaste_proxy) AS desgaste_proxy,
        AVG(s.corriente_proxy) AS corriente_proxy,
        AVG(s.ruido_proxy) AS ruido_proxy,
        AVG(s.velocidad_operativa) AS velocidad_operativa,
        AVG(s.carga_operativa) AS carga_operativa,
        AVG(s.ambiente_externo_proxy) AS ambiente_externo_proxy
    FROM stg_sensores_componentes s
    GROUP BY s.fecha, s.unidad_id, s.componente_id
),
inspection_day AS (
    SELECT
        i.fecha,
        i.unidad_id,
        i.componente_id,
        MAX(i.score_defecto) AS inspection_defect_score,
        AVG(i.confianza_deteccion) AS inspection_confidence,
        MAX(i.defecto_detectado) AS inspection_defect_flag
    FROM stg_inspecciones_automaticas i
    GROUP BY i.fecha, i.unidad_id, i.componente_id
),
maintenance_day AS (
    SELECT
        m.fecha_inicio_dia AS fecha,
        m.unidad_id,
        m.componente_id,
        COUNT(m.mantenimiento_id) AS maintenance_events,
        SUM(m.horas_taller) AS maintenance_hours,
        AVG(m.correctiva_flag) AS corrective_share,
        AVG(m.basada_en_condicion_flag) AS cbm_share
    FROM stg_eventos_mantenimiento m
    GROUP BY m.fecha_inicio_dia, m.unidad_id, m.componente_id
),
failure_day AS (
    SELECT
        f.fecha_falla AS fecha,
        f.unidad_id,
        f.componente_id,
        COUNT(f.falla_id) AS failures_count,
        SUM(f.tiempo_fuera_servicio_horas) AS failure_downtime_h,
        AVG(f.severidad_falla) AS failure_severity_avg,
        MAX(f.repetitiva_flag) AS repetitive_failure_flag
    FROM stg_fallas_historicas f
    GROUP BY f.fecha_falla, f.unidad_id, f.componente_id
)
SELECT
    sd.fecha,
    sd.unidad_id,
    sd.componente_id,
    c.sistema_principal,
    c.subsistema,
    c.tipo_componente,
    c.criticidad_componente,
    c.age_ratio,
    c.cycles_ratio,
    sd.sensor_mean,
    COALESCE(sd.sensor_std, 0) AS sensor_std,
    sd.sensor_max,
    sd.temperatura_operacion,
    sd.vibracion_proxy,
    sd.presion_proxy,
    sd.desgaste_proxy,
    sd.corriente_proxy,
    sd.ruido_proxy,
    sd.velocidad_operativa,
    sd.carga_operativa,
    sd.ambiente_externo_proxy,
    COALESCE(id.inspection_defect_score, 0) AS inspection_defect_score,
    COALESCE(id.inspection_confidence, 0) AS inspection_confidence,
    COALESCE(id.inspection_defect_flag, 0) AS inspection_defect_flag,
    COALESCE(md.maintenance_events, 0) AS maintenance_events,
    COALESCE(md.maintenance_hours, 0) AS maintenance_hours,
    COALESCE(md.corrective_share, 0) AS corrective_share,
    COALESCE(md.cbm_share, 0) AS cbm_share,
    COALESCE(fd.failures_count, 0) AS failures_count,
    COALESCE(fd.failure_downtime_h, 0) AS failure_downtime_h,
    COALESCE(fd.failure_severity_avg, 0) AS failure_severity_avg,
    COALESCE(fd.repetitive_failure_flag, 0) AS repetitive_failure_flag,
    p.temperatura_ambiente,
    p.humedad,
    p.intensidad_servicio,
    p.nivel_congestion_operativa_proxy
FROM sensor_day sd
INNER JOIN stg_componentes_criticos c
    ON c.componente_id = sd.componente_id
LEFT JOIN inspection_day id
    ON id.fecha = sd.fecha
    AND id.unidad_id = sd.unidad_id
    AND id.componente_id = sd.componente_id
LEFT JOIN maintenance_day md
    ON md.fecha = sd.fecha
    AND md.unidad_id = sd.unidad_id
    AND md.componente_id = sd.componente_id
LEFT JOIN failure_day fd
    ON fd.fecha = sd.fecha
    AND fd.unidad_id = sd.unidad_id
    AND fd.componente_id = sd.componente_id
LEFT JOIN stg_unidades u
    ON u.unidad_id = sd.unidad_id
LEFT JOIN stg_parametros_operativos_contexto p
    ON p.fecha = sd.fecha
    AND p.linea_servicio = u.linea_servicio;
