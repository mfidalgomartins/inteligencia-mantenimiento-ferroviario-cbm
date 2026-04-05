DROP VIEW IF EXISTS vw_kpi_disponibilidad_mensual;
DROP VIEW IF EXISTS vw_kpi_riesgo_subsistema;
DROP VIEW IF EXISTS vw_kpi_backlog_deposito;
DROP VIEW IF EXISTS vw_kpi_ratio_correctivo_preventivo;
DROP VIEW IF EXISTS vw_kpi_impacto_servicio_deposito;

CREATE VIEW vw_kpi_disponibilidad_mensual AS
SELECT
  substr(fecha, 1, 7) AS anio_mes,
  ROUND(AVG(disponibilidad_media) * 100, 2) AS disponibilidad_pct,
  ROUND(AVG(retraso_medio), 2) AS retraso_promedio_min,
  ROUND(AVG(ratio_brecha_servicio) * 100, 2) AS brecha_servicio_pct
FROM mart_deposito_diario
GROUP BY 1
ORDER BY 1;

CREATE VIEW vw_kpi_riesgo_subsistema AS
WITH ultima_fecha AS (
  SELECT MAX(fecha) AS max_fecha FROM mart_componentes_diario
)
SELECT
  m.nombre_subsistema,
  ROUND(AVG(m.ratio_temp_umbral), 4) AS ratio_temp,
  ROUND(AVG(m.ratio_vibr_umbral), 4) AS ratio_vibr,
  ROUND(AVG(m.ratio_corriente_umbral), 4) AS ratio_corriente,
  ROUND(AVG(COALESCE(m.fallas_30d, 0)), 2) AS fallas_30d,
  ROUND(AVG(COALESCE(m.downtime_30d, 0)), 2) AS downtime_30d,
  ROUND(
    AVG(
      (m.ratio_temp_umbral * 0.25)
      + (m.ratio_vibr_umbral * 0.30)
      + (m.ratio_corriente_umbral * 0.20)
      + (COALESCE(m.fallas_30d, 0) / 4.0 * 0.15)
      + (m.criticidad_operativa / 5.0 * 0.10)
    ),
    4
  ) AS indice_riesgo_proxy
FROM mart_componentes_diario m
CROSS JOIN ultima_fecha u
WHERE m.fecha = u.max_fecha
GROUP BY 1
ORDER BY indice_riesgo_proxy DESC;

CREATE VIEW vw_kpi_backlog_deposito AS
SELECT
  deposito_id,
  COUNT(*) AS n_ordenes_abiertas,
  ROUND(SUM(horas_trabajo), 2) AS horas_backlog,
  ROUND(AVG(prioridad_original), 2) AS prioridad_media,
  ROUND(AVG(edad_backlog_dias), 2) AS edad_backlog_media_dias,
  ROUND(SUM(repuestos_eur), 2) AS repuestos_backlog_eur
FROM mart_backlog_actual
GROUP BY 1
ORDER BY n_ordenes_abiertas DESC;

CREATE VIEW vw_kpi_ratio_correctivo_preventivo AS
SELECT
  tipo_orden,
  COUNT(*) AS n_ordenes,
  ROUND(SUM(horas_trabajo), 2) AS horas_totales,
  ROUND(SUM(repuestos_eur), 2) AS repuestos_totales_eur
FROM stg_ordenes_trabajo
GROUP BY 1;

CREATE VIEW vw_kpi_impacto_servicio_deposito AS
SELECT
  deposito_id,
  ROUND(AVG(disponibilidad_media) * 100, 2) AS disponibilidad_pct,
  ROUND(AVG(retraso_medio), 2) AS retraso_min_medio,
  ROUND(SUM(cancelaciones), 2) AS cancelaciones_totales,
  ROUND(AVG(ratio_brecha_servicio) * 100, 2) AS brecha_servicio_pct
FROM mart_deposito_diario
GROUP BY 1
ORDER BY disponibilidad_pct ASC;
