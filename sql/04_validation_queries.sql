DROP VIEW IF EXISTS vw_validacion_llaves;
DROP VIEW IF EXISTS vw_validacion_rangos;
DROP VIEW IF EXISTS vw_validacion_completitud;

CREATE VIEW vw_validacion_llaves AS
SELECT 'sensor_sin_instancia' AS check_name, COUNT(*) AS n_errores
FROM stg_sensor_diario s
LEFT JOIN stg_instancia i ON s.instancia_id = i.instancia_id
WHERE i.instancia_id IS NULL
UNION ALL
SELECT 'operacion_sin_unidad', COUNT(*)
FROM stg_operacion_diaria o
LEFT JOIN stg_unidad u ON o.unidad_id = u.unidad_id
WHERE u.unidad_id IS NULL
UNION ALL
SELECT 'orden_sin_instancia', COUNT(*)
FROM stg_ordenes_trabajo o
LEFT JOIN stg_instancia i ON o.instancia_id = i.instancia_id
WHERE i.instancia_id IS NULL;

CREATE VIEW vw_validacion_rangos AS
SELECT 'temperatura_fuera_rango' AS check_name, COUNT(*) AS n_errores
FROM stg_sensor_diario
WHERE temperatura_media_c < -20 OR temperatura_media_c > 150
UNION ALL
SELECT 'vibracion_fuera_rango', COUNT(*)
FROM stg_sensor_diario
WHERE vibracion_rms_mm_s < 0 OR vibracion_rms_mm_s > 40
UNION ALL
SELECT 'corriente_fuera_rango', COUNT(*)
FROM stg_sensor_diario
WHERE corriente_a < 0 OR corriente_a > 600;

CREATE VIEW vw_validacion_completitud AS
SELECT
  'fact_sensor_diario' AS tabla,
  ROUND(100.0 * SUM(CASE WHEN temperatura_media_c IS NULL THEN 1 ELSE 0 END) / COUNT(*), 4) AS pct_null_temp,
  ROUND(100.0 * SUM(CASE WHEN vibracion_rms_mm_s IS NULL THEN 1 ELSE 0 END) / COUNT(*), 4) AS pct_null_vibr,
  ROUND(100.0 * SUM(CASE WHEN corriente_a IS NULL THEN 1 ELSE 0 END) / COUNT(*), 4) AS pct_null_corriente
FROM stg_sensor_diario;
