-- Dialecto: DuckDB SQL
-- Objetivo: estandarizar dimensiones y activos

CREATE OR REPLACE VIEW stg_flotas AS
SELECT
    CAST(flota_id AS VARCHAR) AS flota_id,
    CAST(nombre_flota AS VARCHAR) AS nombre_flota,
    CAST(tipo_material AS VARCHAR) AS tipo_material,
    CAST(operador AS VARCHAR) AS operador,
    CAST(region AS VARCHAR) AS region,
    CAST(ano_fabricacion_base AS INTEGER) AS ano_fabricacion_base,
    CAST(uso_intensidad AS DOUBLE) AS uso_intensidad,
    CAST(criticidad_operativa AS DOUBLE) AS criticidad_operativa,
    CAST(estrategia_mantenimiento_actual AS VARCHAR) AS estrategia_mantenimiento_actual
FROM raw_flotas;

CREATE OR REPLACE VIEW stg_depositos AS
SELECT
    CAST(deposito_id AS VARCHAR) AS deposito_id,
    CAST(nombre_deposito AS VARCHAR) AS nombre_deposito,
    CAST(region AS VARCHAR) AS region,
    CAST(capacidad_taller AS DOUBLE) AS capacidad_taller,
    CAST(capacidad_inspeccion AS DOUBLE) AS capacidad_inspeccion,
    CAST(especializacion_tecnica AS VARCHAR) AS especializacion_tecnica,
    CAST(carga_operativa_media AS DOUBLE) AS carga_operativa_media
FROM raw_depositos;

CREATE OR REPLACE VIEW stg_unidades AS
SELECT
    CAST(unidad_id AS VARCHAR) AS unidad_id,
    CAST(flota_id AS VARCHAR) AS flota_id,
    CAST(deposito_id AS VARCHAR) AS deposito_id,
    CAST(linea_servicio AS VARCHAR) AS linea_servicio,
    CAST(fecha_entrada_servicio AS DATE) AS fecha_entrada_servicio,
    CAST(kilometraje_acumulado_km AS DOUBLE) AS kilometraje_acumulado_km,
    CAST(horas_operacion_acumuladas AS DOUBLE) AS horas_operacion_acumuladas,
    CAST(configuracion_unidad AS VARCHAR) AS configuracion_unidad,
    CAST(criticidad_servicio AS DOUBLE) AS criticidad_servicio,
    CAST(disponibilidad_objetivo AS DOUBLE) AS disponibilidad_objetivo
FROM raw_unidades;

CREATE OR REPLACE VIEW stg_componentes_criticos AS
SELECT
    CAST(componente_id AS VARCHAR) AS componente_id,
    CAST(unidad_id AS VARCHAR) AS unidad_id,
    CAST(sistema_principal AS VARCHAR) AS sistema_principal,
    CAST(subsistema AS VARCHAR) AS subsistema,
    CAST(tipo_componente AS VARCHAR) AS tipo_componente,
    CAST(fabricante_proxy AS VARCHAR) AS fabricante_proxy,
    CAST(fecha_instalacion AS DATE) AS fecha_instalacion,
    CAST(edad_componente_dias AS DOUBLE) AS edad_componente_dias,
    CAST(ciclos_acumulados AS DOUBLE) AS ciclos_acumulados,
    CAST(criticidad_componente AS DOUBLE) AS criticidad_componente,
    CAST(vida_util_teorica_dias AS DOUBLE) AS vida_util_teorica_dias,
    CAST(vida_util_teorica_ciclos AS DOUBLE) AS vida_util_teorica_ciclos,
    CAST(edad_componente_dias AS DOUBLE) / NULLIF(CAST(vida_util_teorica_dias AS DOUBLE), 0) AS age_ratio,
    CAST(ciclos_acumulados AS DOUBLE) / NULLIF(CAST(vida_util_teorica_ciclos AS DOUBLE), 0) AS cycles_ratio
FROM raw_componentes_criticos;
