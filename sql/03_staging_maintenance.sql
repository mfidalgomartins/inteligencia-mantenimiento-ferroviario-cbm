-- Objetivo: estandarizar fallas, mantenimiento, alertas, backlog y operación

CREATE OR REPLACE VIEW stg_fallas_historicas AS
SELECT
    CAST(falla_id AS VARCHAR) AS falla_id,
    CAST(unidad_id AS VARCHAR) AS unidad_id,
    CAST(componente_id AS VARCHAR) AS componente_id,
    CAST(fecha_falla AS DATE) AS fecha_falla,
    CAST(modo_falla AS VARCHAR) AS modo_falla,
    CAST(severidad_falla AS INTEGER) AS severidad_falla,
    CAST(impacto_en_servicio AS VARCHAR) AS impacto_en_servicio,
    CAST(tiempo_fuera_servicio_horas AS DOUBLE) AS tiempo_fuera_servicio_horas,
    CAST(causa_raiz_proxy AS VARCHAR) AS causa_raiz_proxy,
    CAST(repetitiva_flag AS INTEGER) AS repetitiva_flag
FROM raw_fallas_historicas;

CREATE OR REPLACE VIEW stg_eventos_mantenimiento AS
SELECT
    CAST(mantenimiento_id AS VARCHAR) AS mantenimiento_id,
    CAST(unidad_id AS VARCHAR) AS unidad_id,
    CAST(componente_id AS VARCHAR) AS componente_id,
    CAST(deposito_id AS VARCHAR) AS deposito_id,
    CAST(fecha_inicio AS TIMESTAMP) AS fecha_inicio,
    CAST(fecha_fin AS TIMESTAMP) AS fecha_fin,
    CAST(tipo_mantenimiento AS VARCHAR) AS tipo_mantenimiento,
    CAST(motivo_intervencion AS VARCHAR) AS motivo_intervencion,
    CAST(correctiva_flag AS INTEGER) AS correctiva_flag,
    CAST(basada_en_condicion_flag AS INTEGER) AS basada_en_condicion_flag,
    CAST(programada_flag AS INTEGER) AS programada_flag,
    CAST(horas_taller AS DOUBLE) AS horas_taller,
    CAST(coste_mano_obra_proxy AS DOUBLE) AS coste_mano_obra_proxy,
    CAST(coste_material_proxy AS DOUBLE) AS coste_material_proxy,
    CAST(resultado_intervencion AS VARCHAR) AS resultado_intervencion,
    CAST(DATE_TRUNC('day', CAST(fecha_inicio AS TIMESTAMP)) AS DATE) AS fecha_inicio_dia
FROM raw_eventos_mantenimiento;

CREATE OR REPLACE VIEW stg_alertas_operativas AS
SELECT
    CAST(alerta_id AS VARCHAR) AS alerta_id,
    CAST(timestamp AS TIMESTAMP) AS ts,
    CAST(DATE_TRUNC('day', CAST(timestamp AS TIMESTAMP)) AS DATE) AS fecha,
    CAST(unidad_id AS VARCHAR) AS unidad_id,
    CAST(componente_id AS VARCHAR) AS componente_id,
    CAST(tipo_alerta AS VARCHAR) AS tipo_alerta,
    CAST(severidad AS VARCHAR) AS severidad,
    CAST(trigger_origen AS VARCHAR) AS trigger_origen,
    CAST(alerta_temprana_flag AS INTEGER) AS alerta_temprana_flag,
    CAST(accion_sugerida AS VARCHAR) AS accion_sugerida,
    CAST(atendida_flag AS INTEGER) AS atendida_flag
FROM raw_alertas_operativas;

CREATE OR REPLACE VIEW stg_intervenciones_programadas AS
SELECT
    CAST(intervencion_id AS VARCHAR) AS intervencion_id,
    CAST(unidad_id AS VARCHAR) AS unidad_id,
    CAST(componente_id AS VARCHAR) AS componente_id,
    CAST(deposito_id AS VARCHAR) AS deposito_id,
    CAST(fecha_programada AS DATE) AS fecha_programada,
    CAST(prioridad_planificada AS INTEGER) AS prioridad_planificada,
    CAST(estado_intervencion AS VARCHAR) AS estado_intervencion,
    CAST(ventana_operativa_disponible AS INTEGER) AS ventana_operativa_disponible,
    CAST(impacto_si_no_se_ejecuta AS DOUBLE) AS impacto_si_no_se_ejecuta
FROM raw_intervenciones_programadas;

CREATE OR REPLACE VIEW stg_disponibilidad_servicio AS
SELECT
    CAST(fecha AS DATE) AS fecha,
    CAST(unidad_id AS VARCHAR) AS unidad_id,
    CAST(flota_id AS VARCHAR) AS flota_id,
    CAST(linea_servicio AS VARCHAR) AS linea_servicio,
    CAST(horas_planificadas AS DOUBLE) AS horas_planificadas,
    CAST(horas_disponibles AS DOUBLE) AS horas_disponibles,
    CAST(horas_no_disponibles AS DOUBLE) AS horas_no_disponibles,
    CAST(motivo_no_disponibilidad AS VARCHAR) AS motivo_no_disponibilidad,
    CAST(cancelaciones_proxy AS DOUBLE) AS cancelaciones_proxy,
    CAST(puntualidad_impactada_proxy AS DOUBLE) AS puntualidad_impactada_proxy
FROM raw_disponibilidad_servicio;

CREATE OR REPLACE VIEW stg_asignacion_servicio AS
SELECT
    CAST(fecha AS DATE) AS fecha,
    CAST(unidad_id AS VARCHAR) AS unidad_id,
    CAST(linea_servicio AS VARCHAR) AS linea_servicio,
    CAST(servicio_planificado AS DOUBLE) AS servicio_planificado,
    CAST(servicio_realizado AS DOUBLE) AS servicio_realizado,
    CAST(reserva_flag AS INTEGER) AS reserva_flag,
    CAST(sustitucion_requerida_flag AS INTEGER) AS sustitucion_requerida_flag
FROM raw_asignacion_servicio;

CREATE OR REPLACE VIEW stg_backlog_mantenimiento AS
SELECT
    CAST(fecha AS DATE) AS fecha,
    CAST(deposito_id AS VARCHAR) AS deposito_id,
    CAST(unidad_id AS VARCHAR) AS unidad_id,
    CAST(componente_id AS VARCHAR) AS componente_id,
    CAST(tipo_pendencia AS VARCHAR) AS tipo_pendencia,
    CAST(antiguedad_backlog_dias AS DOUBLE) AS antiguedad_backlog_dias,
    CAST(severidad_pendiente AS VARCHAR) AS severidad_pendiente,
    CAST(riesgo_acumulado AS DOUBLE) AS riesgo_acumulado
FROM raw_backlog_mantenimiento;

CREATE OR REPLACE VIEW stg_parametros_operativos_contexto AS
SELECT
    CAST(fecha AS DATE) AS fecha,
    CAST(linea_servicio AS VARCHAR) AS linea_servicio,
    CAST(region AS VARCHAR) AS region,
    CAST(temperatura_ambiente AS DOUBLE) AS temperatura_ambiente,
    CAST(humedad AS DOUBLE) AS humedad,
    CAST(tipo_explotacion AS VARCHAR) AS tipo_explotacion,
    CAST(intensidad_servicio AS DOUBLE) AS intensidad_servicio,
    CAST(nivel_congestion_operativa_proxy AS DOUBLE) AS nivel_congestion_operativa_proxy
FROM raw_parametros_operativos_contexto;

CREATE OR REPLACE VIEW stg_escenarios_mantenimiento AS
SELECT
    CAST(fecha AS DATE) AS fecha,
    CAST(escenario AS VARCHAR) AS escenario,
    CAST(intensidad_operacion_indice AS DOUBLE) AS intensidad_operacion_indice,
    CAST(tension_backlog_indice AS DOUBLE) AS tension_backlog_indice,
    CAST(disponibilidad_recursos_indice AS DOUBLE) AS disponibilidad_recursos_indice,
    CAST(presion_coste_indice AS DOUBLE) AS presion_coste_indice
FROM raw_escenarios_mantenimiento;
