DROP VIEW IF EXISTS stg_unidad;
DROP VIEW IF EXISTS stg_componente;
DROP VIEW IF EXISTS stg_subsistema;
DROP VIEW IF EXISTS stg_instancia;
DROP VIEW IF EXISTS stg_operacion_diaria;
DROP VIEW IF EXISTS stg_sensor_diario;
DROP VIEW IF EXISTS stg_inspeccion_automatica;
DROP VIEW IF EXISTS stg_fallas;
DROP VIEW IF EXISTS stg_ordenes_trabajo;
DROP VIEW IF EXISTS stg_demanda_servicio;

CREATE VIEW stg_unidad AS
SELECT
  unidad_id,
  serie,
  modelo,
  fecha_entrada_servicio,
  deposito_base_id,
  CAST(km_acumulados AS REAL) AS km_acumulados,
  CAST(edad_anios AS REAL) AS edad_anios,
  configuracion
FROM raw_dim_unidad;

CREATE VIEW stg_componente AS
SELECT
  componente_id,
  nombre_componente,
  subsistema_id,
  estrategia_mantenimiento_base,
  CAST(umbral_temperatura_c AS REAL) AS umbral_temperatura_c,
  CAST(umbral_vibracion_rms AS REAL) AS umbral_vibracion_rms,
  CAST(umbral_corriente_a AS REAL) AS umbral_corriente_a
FROM raw_dim_componente;

CREATE VIEW stg_subsistema AS
SELECT
  subsistema_id,
  nombre_subsistema,
  CAST(criticidad_operativa AS REAL) AS criticidad_operativa,
  CAST(peso_disponibilidad AS REAL) AS peso_disponibilidad
FROM raw_dim_subsistema;

CREATE VIEW stg_instancia AS
SELECT
  instancia_id,
  unidad_id,
  componente_id,
  fecha_instalacion,
  CAST(horas_operacion_acumuladas AS REAL) AS horas_operacion_acumuladas,
  CAST(ciclos_acumulados AS REAL) AS ciclos_acumulados,
  CAST(indice_salud_inicial AS REAL) AS indice_salud_inicial,
  CAST(tasa_degradacion_base AS REAL) AS tasa_degradacion_base,
  CAST(factor_estres_operativo AS REAL) AS factor_estres_operativo
FROM raw_bridge_unidad_componente;

CREATE VIEW stg_operacion_diaria AS
SELECT
  fecha,
  unidad_id,
  deposito_id,
  CAST(km_dia AS REAL) AS km_dia,
  CAST(horas_servicio AS REAL) AS horas_servicio,
  CAST(incidencias_servicio AS INTEGER) AS incidencias_servicio,
  CAST(retraso_minutos AS REAL) AS retraso_minutos,
  CAST(cancelaciones AS INTEGER) AS cancelaciones,
  CAST(disponibilidad_flag AS INTEGER) AS disponibilidad_flag,
  CAST(fallas_dia AS INTEGER) AS fallas_dia,
  CAST(downtime_min AS REAL) AS downtime_min
FROM raw_fact_operacion_diaria;

CREATE VIEW stg_sensor_diario AS
SELECT
  fecha,
  instancia_id,
  CAST(temperatura_media_c AS REAL) AS temperatura_media_c,
  CAST(vibracion_rms_mm_s AS REAL) AS vibracion_rms_mm_s,
  CAST(corriente_a AS REAL) AS corriente_a,
  CAST(presion_bar AS REAL) AS presion_bar,
  CAST(humedad_pct AS REAL) AS humedad_pct,
  CAST(energia_kwh AS REAL) AS energia_kwh
FROM raw_fact_sensor_diario;

CREATE VIEW stg_inspeccion_automatica AS
SELECT
  fecha,
  unidad_id,
  instancia_id,
  CAST(hallazgo_vision_score AS REAL) AS hallazgo_vision_score,
  CAST(desgaste_detectado_pct AS REAL) AS desgaste_detectado_pct,
  CAST(anomalia_geometrica_mm AS REAL) AS anomalia_geometrica_mm,
  CAST(confianza_modelo AS REAL) AS confianza_modelo
FROM raw_fact_inspeccion_automatica;

CREATE VIEW stg_fallas AS
SELECT
  fallo_id,
  fecha_fallo,
  unidad_id,
  instancia_id,
  CAST(severidad AS INTEGER) AS severidad,
  CAST(minutos_fuera_servicio AS REAL) AS minutos_fuera_servicio,
  CAST(costo_correctivo_eur AS REAL) AS costo_correctivo_eur,
  causa_raiz
FROM raw_fact_fallas;

CREATE VIEW stg_ordenes_trabajo AS
SELECT
  orden_id,
  fecha_apertura,
  NULLIF(fecha_cierre, '') AS fecha_cierre,
  unidad_id,
  instancia_id,
  deposito_id,
  tipo_orden,
  CAST(prioridad_original AS INTEGER) AS prioridad_original,
  CAST(horas_trabajo AS REAL) AS horas_trabajo,
  CAST(repuestos_eur AS REAL) AS repuestos_eur,
  estado
FROM raw_fact_ordenes_trabajo;

CREATE VIEW stg_demanda_servicio AS
SELECT
  fecha,
  deposito_id,
  CAST(trenes_requeridos AS INTEGER) AS trenes_requeridos,
  CAST(unidades_disponibles AS INTEGER) AS unidades_disponibles,
  CAST(brecha_servicio AS INTEGER) AS brecha_servicio
FROM raw_fact_demanda_servicio;
