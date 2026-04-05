DROP TABLE IF EXISTS mart_componentes_diario;
DROP TABLE IF EXISTS mart_unidad_diario;
DROP TABLE IF EXISTS mart_deposito_diario;
DROP TABLE IF EXISTS mart_backlog_actual;

CREATE TABLE mart_componentes_diario AS
WITH fallas_diarias AS (
  SELECT
    fecha_fallo AS fecha,
    instancia_id,
    COUNT(*) AS n_fallas_dia,
    SUM(minutos_fuera_servicio) AS downtime_falla_dia
  FROM stg_fallas
  GROUP BY 1,2
),
inspecciones AS (
  SELECT
    fecha,
    instancia_id,
    AVG(desgaste_detectado_pct) AS desgaste_detectado_pct,
    AVG(hallazgo_vision_score) AS hallazgo_vision_score,
    AVG(anomalia_geometrica_mm) AS anomalia_geometrica_mm,
    AVG(confianza_modelo) AS confianza_modelo
  FROM stg_inspeccion_automatica
  GROUP BY 1,2
),
ordenes_abiertas AS (
  SELECT
    instancia_id,
    COUNT(*) AS ordenes_abiertas,
    SUM(horas_trabajo) AS horas_backlog
  FROM stg_ordenes_trabajo
  WHERE estado = 'abierta'
  GROUP BY 1
),
base AS (
  SELECT
    s.fecha,
    i.instancia_id,
    i.unidad_id,
    u.deposito_base_id AS deposito_id,
    i.componente_id,
    c.nombre_componente,
    c.subsistema_id,
    sub.nombre_subsistema,
    sub.criticidad_operativa,
    sub.peso_disponibilidad,
    c.estrategia_mantenimiento_base,
    c.umbral_temperatura_c,
    c.umbral_vibracion_rms,
    c.umbral_corriente_a,
    i.indice_salud_inicial,
    i.tasa_degradacion_base,
    i.factor_estres_operativo,
    s.temperatura_media_c,
    s.vibracion_rms_mm_s,
    s.corriente_a,
    s.presion_bar,
    s.humedad_pct,
    s.energia_kwh,
    COALESCE(ins.desgaste_detectado_pct, 0.0) AS desgaste_detectado_pct,
    COALESCE(ins.hallazgo_vision_score, 0.0) AS hallazgo_vision_score,
    COALESCE(ins.anomalia_geometrica_mm, 0.0) AS anomalia_geometrica_mm,
    COALESCE(ins.confianza_modelo, 0.0) AS confianza_modelo,
    op.km_dia,
    op.horas_servicio,
    op.retraso_minutos,
    op.cancelaciones,
    op.disponibilidad_flag,
    COALESCE(f.n_fallas_dia, 0) AS n_fallas_dia,
    COALESCE(f.downtime_falla_dia, 0.0) AS downtime_falla_dia,
    COALESCE(ob.ordenes_abiertas, 0) AS ordenes_abiertas,
    COALESCE(ob.horas_backlog, 0.0) AS horas_backlog
  FROM stg_sensor_diario s
  INNER JOIN stg_instancia i ON s.instancia_id = i.instancia_id
  INNER JOIN stg_unidad u ON i.unidad_id = u.unidad_id
  INNER JOIN stg_componente c ON i.componente_id = c.componente_id
  INNER JOIN stg_subsistema sub ON c.subsistema_id = sub.subsistema_id
  INNER JOIN stg_operacion_diaria op ON op.fecha = s.fecha AND op.unidad_id = i.unidad_id
  LEFT JOIN fallas_diarias f ON f.fecha = s.fecha AND f.instancia_id = s.instancia_id
  LEFT JOIN inspecciones ins ON ins.fecha = s.fecha AND ins.instancia_id = s.instancia_id
  LEFT JOIN ordenes_abiertas ob ON ob.instancia_id = s.instancia_id
)
SELECT
  *,
  temperatura_media_c / NULLIF(umbral_temperatura_c, 0) AS ratio_temp_umbral,
  vibracion_rms_mm_s / NULLIF(umbral_vibracion_rms, 0) AS ratio_vibr_umbral,
  corriente_a / NULLIF(umbral_corriente_a, 0) AS ratio_corriente_umbral,
  AVG(temperatura_media_c) OVER (
    PARTITION BY instancia_id
    ORDER BY fecha
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS temp_media_7d,
  AVG(vibracion_rms_mm_s) OVER (
    PARTITION BY instancia_id
    ORDER BY fecha
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS vibr_media_7d,
  AVG(corriente_a) OVER (
    PARTITION BY instancia_id
    ORDER BY fecha
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS corriente_media_7d,
  AVG(km_dia) OVER (
    PARTITION BY unidad_id
    ORDER BY fecha
    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
  ) AS km_media_30d_unidad,
  SUM(n_fallas_dia) OVER (
    PARTITION BY instancia_id
    ORDER BY fecha
    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
  ) AS fallas_30d,
  SUM(downtime_falla_dia) OVER (
    PARTITION BY instancia_id
    ORDER BY fecha
    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
  ) AS downtime_30d,
  temperatura_media_c - LAG(temperatura_media_c, 14) OVER (
    PARTITION BY instancia_id ORDER BY fecha
  ) AS delta_temp_14d,
  vibracion_rms_mm_s - LAG(vibracion_rms_mm_s, 14) OVER (
    PARTITION BY instancia_id ORDER BY fecha
  ) AS delta_vibr_14d,
  corriente_a - LAG(corriente_a, 14) OVER (
    PARTITION BY instancia_id ORDER BY fecha
  ) AS delta_corriente_14d
FROM base;

CREATE TABLE mart_unidad_diario AS
SELECT
  fecha,
  unidad_id,
  deposito_id,
  AVG(ratio_temp_umbral) AS ratio_temp_umbral_prom,
  AVG(ratio_vibr_umbral) AS ratio_vibr_umbral_prom,
  AVG(ratio_corriente_umbral) AS ratio_corriente_umbral_prom,
  SUM(n_fallas_dia) AS n_fallas_componentes_dia,
  SUM(downtime_falla_dia) AS downtime_componentes_dia,
  AVG(disponibilidad_flag) AS disponibilidad_flag_prom,
  AVG(km_dia) AS km_promedio_dia,
  SUM(cancelaciones) AS cancelaciones_dia,
  AVG(retraso_minutos) AS retraso_promedio_min
FROM mart_componentes_diario
GROUP BY 1,2,3;

CREATE TABLE mart_deposito_diario AS
WITH base AS (
  SELECT
    u.fecha,
    u.deposito_id,
    AVG(u.disponibilidad_flag_prom) AS disponibilidad_media,
    SUM(u.n_fallas_componentes_dia) AS fallas_dia,
    SUM(u.downtime_componentes_dia) AS downtime_dia,
    AVG(u.km_promedio_dia) AS km_medio_unidad,
    AVG(u.retraso_promedio_min) AS retraso_medio,
    SUM(u.cancelaciones_dia) AS cancelaciones,
    COUNT(DISTINCT u.unidad_id) AS unidades_reportando
  FROM mart_unidad_diario u
  GROUP BY 1,2
)
SELECT
  b.*,
  d.trenes_requeridos,
  d.unidades_disponibles,
  d.brecha_servicio,
  CASE
    WHEN d.trenes_requeridos > 0 THEN CAST(d.brecha_servicio AS REAL) / d.trenes_requeridos
    ELSE 0
  END AS ratio_brecha_servicio
FROM base b
LEFT JOIN stg_demanda_servicio d
  ON d.fecha = b.fecha AND d.deposito_id = b.deposito_id;

CREATE TABLE mart_backlog_actual AS
SELECT
  o.deposito_id,
  o.unidad_id,
  o.instancia_id,
  i.componente_id,
  c.nombre_componente,
  c.subsistema_id,
  sub.nombre_subsistema,
  sub.criticidad_operativa,
  o.tipo_orden,
  o.prioridad_original,
  o.fecha_apertura,
  o.horas_trabajo,
  o.repuestos_eur,
  CAST(julianday('2025-12-31') - julianday(o.fecha_apertura) AS INTEGER) AS edad_backlog_dias
FROM stg_ordenes_trabajo o
INNER JOIN stg_instancia i ON o.instancia_id = i.instancia_id
INNER JOIN stg_componente c ON i.componente_id = c.componente_id
INNER JOIN stg_subsistema sub ON c.subsistema_id = sub.subsistema_id
WHERE o.estado = 'abierta';
