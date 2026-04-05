-- Objetivo: estandarizar señales sensóricas e inspecciones automáticas

CREATE OR REPLACE VIEW stg_sensores_componentes AS
SELECT
    CAST(timestamp AS TIMESTAMP) AS ts,
    CAST(DATE_TRUNC('day', CAST(timestamp AS TIMESTAMP)) AS DATE) AS fecha,
    CAST(unidad_id AS VARCHAR) AS unidad_id,
    CAST(componente_id AS VARCHAR) AS componente_id,
    CAST(sensor_tipo AS VARCHAR) AS sensor_tipo,
    CAST(valor_sensor AS DOUBLE) AS valor_sensor,
    CAST(temperatura_operacion AS DOUBLE) AS temperatura_operacion,
    CAST(vibracion_proxy AS DOUBLE) AS vibracion_proxy,
    CAST(presion_proxy AS DOUBLE) AS presion_proxy,
    CAST(desgaste_proxy AS DOUBLE) AS desgaste_proxy,
    CAST(corriente_proxy AS DOUBLE) AS corriente_proxy,
    CAST(ruido_proxy AS DOUBLE) AS ruido_proxy,
    CAST(velocidad_operativa AS DOUBLE) AS velocidad_operativa,
    CAST(carga_operativa AS DOUBLE) AS carga_operativa,
    CAST(ambiente_externo_proxy AS DOUBLE) AS ambiente_externo_proxy
FROM raw_sensores_componentes;

CREATE OR REPLACE VIEW stg_inspecciones_automaticas AS
SELECT
    CAST(inspeccion_id AS VARCHAR) AS inspeccion_id,
    CAST(timestamp AS TIMESTAMP) AS ts,
    CAST(DATE_TRUNC('day', CAST(timestamp AS TIMESTAMP)) AS DATE) AS fecha,
    CAST(unidad_id AS VARCHAR) AS unidad_id,
    CAST(componente_id AS VARCHAR) AS componente_id,
    CAST(familia_inspeccion AS VARCHAR) AS familia_inspeccion,
    CAST(detector_origen AS VARCHAR) AS detector_origen,
    CAST(severidad_hallazgo AS VARCHAR) AS severidad_hallazgo,
    CAST(score_defecto AS DOUBLE) AS score_defecto,
    CAST(defecto_detectado AS INTEGER) AS defecto_detectado,
    CAST(confianza_deteccion AS DOUBLE) AS confianza_deteccion,
    CAST(recomendacion_inicial AS VARCHAR) AS recomendacion_inicial
FROM raw_inspecciones_automaticas;
