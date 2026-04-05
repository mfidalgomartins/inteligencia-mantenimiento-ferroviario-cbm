# Diccionario de Datos

## Tablas Raw (data/raw)

### `flotas`
- `flota_id`: identificador de flota.
- `nombre_flota`: nombre operativo.
- `tipo_material`: EMU/DMU/LRV.
- `operador`, `region`.
- `ano_fabricacion_base`: año de referencia del parque.
- `uso_intensidad`: índice proxy de uso.
- `criticidad_operativa`: criticidad de flota para operación.
- `estrategia_mantenimiento_actual`: estrategia dominante actual.

### `unidades`
- `unidad_id`: identificador único de unidad.
- `flota_id`, `deposito_id`, `linea_servicio`.
- `fecha_entrada_servicio`.
- `kilometraje_acumulado_km`, `horas_operacion_acumuladas`.
- `configuracion_unidad`, `criticidad_servicio`, `disponibilidad_objetivo`.

### `depositos`
- `deposito_id`, `nombre_deposito`, `region`.
- `capacidad_taller`, `capacidad_inspeccion`.
- `especializacion_tecnica`.
- `carga_operativa_media`.

### `componentes_criticos`
- `componente_id`, `unidad_id`.
- `sistema_principal`, `subsistema`, `tipo_componente`.
- `fabricante_proxy`, `fecha_instalacion`.
- `edad_componente_dias`, `ciclos_acumulados`.
- `criticidad_componente`, `vida_util_teorica_dias`, `vida_util_teorica_ciclos`.

### `sensores_componentes`
- `timestamp`, `unidad_id`, `componente_id`, `sensor_tipo`.
- `valor_sensor`, `temperatura_operacion`, `vibracion_proxy`, `presion_proxy`, `desgaste_proxy`.
- `corriente_proxy`, `ruido_proxy`, `velocidad_operativa`, `carga_operativa`, `ambiente_externo_proxy`.

### `inspecciones_automaticas`
- `inspeccion_id`, `timestamp`, `unidad_id`, `componente_id`.
- `familia_inspeccion` (wheel/brake/bogie/pantograph).
- `detector_origen`, `severidad_hallazgo`, `score_defecto`, `defecto_detectado`.
- `confianza_deteccion`, `recomendacion_inicial`.

### `eventos_mantenimiento`
- `mantenimiento_id`, `unidad_id`, `componente_id`, `deposito_id`.
- `fecha_inicio`, `fecha_fin`.
- `tipo_mantenimiento`, `motivo_intervencion`.
- `correctiva_flag`, `basada_en_condicion_flag`, `programada_flag`.
- `horas_taller`, `coste_mano_obra_proxy`, `coste_material_proxy`, `resultado_intervencion`.

### `fallas_historicas`
- `falla_id`, `unidad_id`, `componente_id`, `fecha_falla`.
- `modo_falla`, `severidad_falla`, `impacto_en_servicio`.
- `tiempo_fuera_servicio_horas`, `causa_raiz_proxy`, `repetitiva_flag`.

### `alertas_operativas`
- `alerta_id`, `timestamp`, `unidad_id`, `componente_id`.
- `tipo_alerta`, `severidad`, `trigger_origen`, `alerta_temprana_flag`.
- `accion_sugerida`, `atendida_flag`.

### `intervenciones_programadas`
- `intervencion_id`, `unidad_id`, `componente_id`, `deposito_id`.
- `fecha_programada`, `prioridad_planificada`, `estado_intervencion`.
- `ventana_operativa_disponible`, `impacto_si_no_se_ejecuta`.

### `disponibilidad_servicio`
- `fecha`, `unidad_id`, `flota_id`, `linea_servicio`.
- `horas_planificadas`, `horas_disponibles`, `horas_no_disponibles`.
- `motivo_no_disponibilidad`, `cancelaciones_proxy`, `puntualidad_impactada_proxy`.

### `asignacion_servicio`
- `fecha`, `unidad_id`, `linea_servicio`.
- `servicio_planificado`, `servicio_realizado`, `reserva_flag`, `sustitucion_requerida_flag`.

### `backlog_mantenimiento`
- `fecha`, `deposito_id`, `unidad_id`, `componente_id`.
- `tipo_pendencia`, `antiguedad_backlog_dias`, `severidad_pendiente`, `riesgo_acumulado`.

### `parametros_operativos_contexto`
- `fecha`, `linea_servicio`, `region`.
- `temperatura_ambiente`, `humedad`, `tipo_explotacion`, `intensidad_servicio`, `nivel_congestion_operativa_proxy`.

### `escenarios_mantenimiento`
- `fecha`, `escenario`.
- `intensidad_operacion_indice`, `tension_backlog_indice`, `disponibilidad_recursos_indice`, `presion_coste_indice`.

## Tablas Processed Principales
- `component_day_features`, `unit_day_features`, `fleet_week_features`, `workshop_priority_features`
- `component_health_score`, `component_failure_risk_score`, `component_rul_estimate`, `unit_unavailability_risk_score`
- `workshop_priority_table`, `workshop_scheduling_recommendation`
- `comparativo_estrategias`, `impacto_diferimiento_resumen`
