-- Objetivo: consultas KPI de negocio y vista de valor CBM

CREATE OR REPLACE VIEW kpi_top_unidades_por_riesgo AS
SELECT
    fecha,
    unidad_id,
    flota_id,
    predicted_unavailability_risk,
    critical_components_at_risk,
    backlog_physical_items,
    backlog_overdue_items,
    backlog_critical_items,
    backlog_exposure_adjusted_score,
    -- Compatibilidad legacy.
    backlog_risk,
    horas_no_disponibles
FROM mart_unit_day
ORDER BY predicted_unavailability_risk DESC, horas_no_disponibles DESC
LIMIT 50;

CREATE OR REPLACE VIEW kpi_top_componentes_por_criticidad AS
SELECT
    fecha,
    componente_id,
    unidad_id,
    subsistema,
    tipo_componente,
    criticidad_componente,
    estimated_health_input_index,
    deterioration_input_index,
    failures_30d,
    critical_alerts_count
FROM mart_component_day
ORDER BY criticidad_componente DESC, estimated_health_input_index ASC, deterioration_input_index DESC
LIMIT 80;

CREATE OR REPLACE VIEW kpi_top_depositos_por_saturacion AS
SELECT
    fecha,
    deposito_id,
    nombre_deposito,
    saturation_ratio,
    backlog_physical_items,
    backlog_overdue_items,
    backlog_critical_items,
    backlog_exposure_adjusted_score,
    -- Compatibilidad legacy.
    backlog_items,
    backlog_risk,
    avg_unit_risk,
    corrective_share
FROM vw_depot_maintenance_pressure
ORDER BY saturation_ratio DESC, backlog_critical_items DESC
LIMIT 40;

CREATE OR REPLACE VIEW kpi_fallas_repetitivas_mas_frecuentes AS
SELECT
    subsistema,
    tipo_componente,
    modo_falla,
    COUNT(*) AS unit_component_cases,
    SUM(repetitive_events) AS repetitive_events,
    SUM(downtime_total_h) AS downtime_total_h
FROM vw_failure_repetition_patterns
GROUP BY subsistema, tipo_componente, modo_falla
ORDER BY repetitive_events DESC, downtime_total_h DESC;

CREATE OR REPLACE VIEW kpi_unidades_mayor_indisponibilidad AS
SELECT
    unidad_id,
    flota_id,
    SUM(horas_no_disponibles) AS horas_no_disponibles_total,
    SUM(cancelaciones_proxy) AS cancelaciones_totales,
    AVG(predicted_unavailability_risk) AS riesgo_promedio
FROM mart_unit_day
GROUP BY unidad_id, flota_id
ORDER BY horas_no_disponibles_total DESC
LIMIT 50;

CREATE OR REPLACE VIEW kpi_backlog_mas_critico AS
SELECT
    deposito_id,
    SUM(backlog_physical_items) AS backlog_fisico_total,
    SUM(backlog_overdue_items) AS backlog_vencido_total,
    SUM(backlog_critical_items) AS backlog_critico_total,
    AVG(backlog_exposure_adjusted_score) AS backlog_exposure_adjusted_promedio,
    AVG(saturation_ratio) AS saturation_ratio_promedio
FROM vw_depot_maintenance_pressure
GROUP BY deposito_id
ORDER BY backlog_critico_total DESC, backlog_vencido_total DESC
LIMIT 20;

CREATE OR REPLACE VIEW kpi_familias_peor_health_trend AS
SELECT
    tipo_componente,
    AVG(rolling_slope) AS rolling_slope_promedio,
    AVG(estimated_health_input_index) AS health_input_promedio,
    AVG(deterioration_input_index) AS deterioration_input_promedio,
    AVG(failures_30d) AS failures_30d_promedio
FROM mart_component_day
GROUP BY tipo_componente
ORDER BY deterioration_input_promedio DESC, health_input_promedio ASC;

CREATE OR REPLACE VIEW kpi_inspeccion_automatica_por_familia AS
WITH component_family AS (
    SELECT
        c.unidad_id,
        c.componente_id,
        CASE
            WHEN LOWER(COALESCE(c.sistema_principal, '') || ' ' || COALESCE(c.subsistema, '') || ' ' || COALESCE(c.tipo_componente, '')) LIKE '%wheel%' THEN 'wheel'
            WHEN LOWER(COALESCE(c.sistema_principal, '') || ' ' || COALESCE(c.subsistema, '') || ' ' || COALESCE(c.tipo_componente, '')) LIKE '%rodadura%' THEN 'wheel'
            WHEN LOWER(COALESCE(c.sistema_principal, '') || ' ' || COALESCE(c.subsistema, '') || ' ' || COALESCE(c.tipo_componente, '')) LIKE '%brake%' THEN 'brake'
            WHEN LOWER(COALESCE(c.sistema_principal, '') || ' ' || COALESCE(c.subsistema, '') || ' ' || COALESCE(c.tipo_componente, '')) LIKE '%fren%' THEN 'brake'
            WHEN LOWER(COALESCE(c.sistema_principal, '') || ' ' || COALESCE(c.subsistema, '') || ' ' || COALESCE(c.tipo_componente, '')) LIKE '%bogie%' THEN 'bogie'
            WHEN LOWER(COALESCE(c.sistema_principal, '') || ' ' || COALESCE(c.subsistema, '') || ' ' || COALESCE(c.tipo_componente, '')) LIKE '%suspension%' THEN 'bogie'
            WHEN LOWER(COALESCE(c.sistema_principal, '') || ' ' || COALESCE(c.subsistema, '') || ' ' || COALESCE(c.tipo_componente, '')) LIKE '%wheelset%' THEN 'bogie'
            WHEN LOWER(COALESCE(c.sistema_principal, '') || ' ' || COALESCE(c.subsistema, '') || ' ' || COALESCE(c.tipo_componente, '')) LIKE '%pant%' THEN 'pantograph'
            WHEN LOWER(COALESCE(c.sistema_principal, '') || ' ' || COALESCE(c.subsistema, '') || ' ' || COALESCE(c.tipo_componente, '')) LIKE '%capt%' THEN 'pantograph'
            ELSE 'other'
        END AS family_technical
    FROM stg_componentes_criticos c
),
inspection_enriched AS (
    SELECT
        i.inspeccion_id,
        i.ts,
        i.unidad_id,
        i.componente_id,
        i.severidad_hallazgo,
        CAST(i.defecto_detectado AS DOUBLE) AS defecto_detectado,
        LEAST(1.0, GREATEST(0.0, i.confianza_deteccion)) AS confianza_deteccion,
        CASE
            WHEN LOWER(COALESCE(i.familia_inspeccion, '')) LIKE '%wheel%' THEN 'wheel'
            WHEN LOWER(COALESCE(i.familia_inspeccion, '')) LIKE '%rodadura%' THEN 'wheel'
            WHEN LOWER(COALESCE(i.familia_inspeccion, '')) LIKE '%brake%' THEN 'brake'
            WHEN LOWER(COALESCE(i.familia_inspeccion, '')) LIKE '%fren%' THEN 'brake'
            WHEN LOWER(COALESCE(i.familia_inspeccion, '')) LIKE '%bogie%' THEN 'bogie'
            WHEN LOWER(COALESCE(i.familia_inspeccion, '')) LIKE '%suspension%' THEN 'bogie'
            WHEN LOWER(COALESCE(i.familia_inspeccion, '')) LIKE '%pant%' THEN 'pantograph'
            WHEN LOWER(COALESCE(i.familia_inspeccion, '')) LIKE '%capt%' THEN 'pantograph'
            ELSE 'other'
        END AS family_reported,
        COALESCE(
            cf.family_technical,
            CASE
                WHEN LOWER(COALESCE(i.familia_inspeccion, '')) LIKE '%wheel%' THEN 'wheel'
                WHEN LOWER(COALESCE(i.familia_inspeccion, '')) LIKE '%rodadura%' THEN 'wheel'
                WHEN LOWER(COALESCE(i.familia_inspeccion, '')) LIKE '%brake%' THEN 'brake'
                WHEN LOWER(COALESCE(i.familia_inspeccion, '')) LIKE '%fren%' THEN 'brake'
                WHEN LOWER(COALESCE(i.familia_inspeccion, '')) LIKE '%bogie%' THEN 'bogie'
                WHEN LOWER(COALESCE(i.familia_inspeccion, '')) LIKE '%suspension%' THEN 'bogie'
                WHEN LOWER(COALESCE(i.familia_inspeccion, '')) LIKE '%pant%' THEN 'pantograph'
                WHEN LOWER(COALESCE(i.familia_inspeccion, '')) LIKE '%capt%' THEN 'pantograph'
                ELSE 'other'
            END
        ) AS family
    FROM stg_inspecciones_automaticas i
    LEFT JOIN component_family cf
        ON cf.unidad_id = i.unidad_id
       AND cf.componente_id = i.componente_id
),
inspection_filtered AS (
    SELECT
        ie.*,
        CASE
            WHEN ie.family_reported = ie.family THEN 1.0
            WHEN ie.family_reported = 'other' THEN 1.0
            ELSE 0.0
        END AS family_mapping_consistency_flag
    FROM inspection_enriched ie
    WHERE ie.family IN ('wheel', 'brake', 'bogie', 'pantograph')
),
failures_enriched AS (
    SELECT
        f.falla_id,
        f.unidad_id,
        f.componente_id,
        f.fecha_falla,
        COALESCE(
            cf.family_technical,
            CASE
                WHEN LOWER(COALESCE(f.modo_falla, '')) LIKE '%wheel%' THEN 'wheel'
                WHEN LOWER(COALESCE(f.modo_falla, '')) LIKE '%rodadura%' THEN 'wheel'
                WHEN LOWER(COALESCE(f.modo_falla, '')) LIKE '%brake%' THEN 'brake'
                WHEN LOWER(COALESCE(f.modo_falla, '')) LIKE '%fren%' THEN 'brake'
                WHEN LOWER(COALESCE(f.modo_falla, '')) LIKE '%bogie%' THEN 'bogie'
                WHEN LOWER(COALESCE(f.modo_falla, '')) LIKE '%suspension%' THEN 'bogie'
                WHEN LOWER(COALESCE(f.modo_falla, '')) LIKE '%pant%' THEN 'pantograph'
                WHEN LOWER(COALESCE(f.modo_falla, '')) LIKE '%capt%' THEN 'pantograph'
                ELSE 'other'
            END
        ) AS family
    FROM stg_fallas_historicas f
    LEFT JOIN component_family cf
        ON cf.unidad_id = f.unidad_id
       AND cf.componente_id = f.componente_id
),
failures_filtered AS (
    SELECT *
    FROM failures_enriched
    WHERE family IN ('wheel', 'brake', 'bogie', 'pantograph')
),
monitored AS (
    SELECT
        family_technical AS family,
        COUNT(DISTINCT componente_id) AS monitored_components
    FROM component_family
    WHERE family_technical IN ('wheel', 'brake', 'bogie', 'pantograph')
    GROUP BY family_technical
),
inspected AS (
    SELECT
        family,
        COUNT(DISTINCT componente_id) AS inspected_components
    FROM inspection_filtered
    GROUP BY family
),
inspection_base AS (
    SELECT
        family,
        COUNT(DISTINCT inspeccion_id) AS total_inspections,
        SUM(defecto_detectado) AS detections,
        AVG(confianza_deteccion) AS avg_confidence_all,
        AVG(family_mapping_consistency_flag) AS family_mapping_consistency_rate
    FROM inspection_filtered
    GROUP BY family
),
failure_base AS (
    SELECT
        family,
        COUNT(DISTINCT falla_id) AS total_failures
    FROM failures_filtered
    GROUP BY family
),
failure_candidates AS (
    SELECT
        f.falla_id,
        f.unidad_id,
        f.componente_id,
        f.family,
        i.inspeccion_id,
        i.ts AS inspection_ts,
        f.fecha_falla,
        DATE_DIFF('day', i.ts, CAST(f.fecha_falla AS TIMESTAMP)) AS days_before_failure,
        i.confianza_deteccion,
        ROW_NUMBER() OVER (PARTITION BY f.falla_id ORDER BY i.ts DESC) AS rn
    FROM failures_filtered f
    JOIN inspection_filtered i
        ON i.unidad_id = f.unidad_id
       AND i.componente_id = f.componente_id
       AND i.family = f.family
       AND i.defecto_detectado = 1
       AND DATE_DIFF('day', i.ts, CAST(f.fecha_falla AS TIMESTAMP)) BETWEEN 0 AND 30
),
failure_link AS (
    SELECT
        falla_id,
        unidad_id,
        componente_id,
        family,
        inspeccion_id,
        inspection_ts,
        fecha_falla,
        days_before_failure,
        confianza_deteccion
    FROM failure_candidates
    WHERE rn = 1
),
pre_failure AS (
    SELECT
        family,
        COUNT(DISTINCT falla_id) AS failures_with_pre_detection,
        AVG(days_before_failure) AS lead_time_medio_dias,
        AVG(confianza_deteccion) AS avg_confidence_pre_failure
    FROM failure_link
    GROUP BY family
),
detections AS (
    SELECT *
    FROM inspection_filtered
    WHERE defecto_detectado = 1
),
detection_candidates AS (
    SELECT
        d.inspeccion_id,
        d.family,
        DATE_DIFF('day', d.ts, CAST(f.fecha_falla AS TIMESTAMP)) AS days_to_failure,
        ROW_NUMBER() OVER (PARTITION BY d.inspeccion_id ORDER BY f.fecha_falla ASC) AS rn
    FROM detections d
    LEFT JOIN failures_filtered f
        ON f.unidad_id = d.unidad_id
       AND f.componente_id = d.componente_id
       AND f.family = d.family
       AND DATE_DIFF('day', d.ts, CAST(f.fecha_falla AS TIMESTAMP)) BETWEEN 0 AND 30
),
detection_outcomes AS (
    SELECT
        d.inspeccion_id,
        d.family,
        CASE WHEN c.days_to_failure IS NULL THEN 0 ELSE 1 END AS failure_within_horizon_flag
    FROM detections d
    LEFT JOIN detection_candidates c
        ON c.inspeccion_id = d.inspeccion_id
       AND c.rn = 1
),
det_out AS (
    SELECT
        family,
        SUM(failure_within_horizon_flag) AS detections_with_future_failure,
        COUNT(DISTINCT inspeccion_id) AS total_detections
    FROM detection_outcomes
    GROUP BY family
),
early_alerts AS (
    SELECT
        unidad_id,
        componente_id,
        ts AS alert_ts
    FROM stg_alertas_operativas
    WHERE alerta_temprana_flag = 1
),
chain_candidates AS (
    SELECT
        fl.falla_id,
        fl.family,
        fl.unidad_id,
        fl.componente_id,
        fl.inspection_ts,
        fl.fecha_falla,
        ea.alert_ts,
        DATE_DIFF('hour', fl.inspection_ts, ea.alert_ts) AS detection_to_alert_h,
        DATE_DIFF('hour', ea.alert_ts, CAST(fl.fecha_falla AS TIMESTAMP)) AS alert_to_failure_h,
        ROW_NUMBER() OVER (PARTITION BY fl.falla_id ORDER BY ea.alert_ts ASC) AS rn
    FROM failure_link fl
    LEFT JOIN early_alerts ea
        ON ea.unidad_id = fl.unidad_id
       AND ea.componente_id = fl.componente_id
       AND ea.alert_ts BETWEEN fl.inspection_ts AND CAST(fl.fecha_falla AS TIMESTAMP)
),
chain_metrics AS (
    SELECT
        family,
        AVG(CASE WHEN alert_ts IS NULL THEN 0.0 ELSE 1.0 END) AS alert_chain_rate,
        AVG(detection_to_alert_h) AS detection_to_alert_mean_h,
        AVG(alert_to_failure_h) AS alert_to_failure_mean_h
    FROM chain_candidates
    WHERE rn = 1 OR rn IS NULL
    GROUP BY family
),
severe_detections AS (
    SELECT
        d.inspeccion_id,
        d.family,
        d.unidad_id,
        d.componente_id,
        d.ts
    FROM detections d
    WHERE d.severidad_hallazgo IN ('alta', 'critica')
),
maintenance_candidates AS (
    SELECT
        sd.inspeccion_id,
        sd.family,
        DATE_DIFF('day', sd.ts, m.fecha_inicio) AS days_to_maintenance,
        ROW_NUMBER() OVER (PARTITION BY sd.inspeccion_id ORDER BY m.fecha_inicio ASC) AS rn
    FROM severe_detections sd
    LEFT JOIN stg_eventos_mantenimiento m
        ON m.unidad_id = sd.unidad_id
       AND m.componente_id = sd.componente_id
       AND DATE_DIFF('day', sd.ts, m.fecha_inicio) BETWEEN 0 AND 14
),
maintenance_follow AS (
    SELECT
        family,
        AVG(CASE WHEN days_to_maintenance IS NULL THEN 0.0 ELSE 1.0 END) AS maintenance_followthrough_rate
    FROM maintenance_candidates
    WHERE rn = 1 OR rn IS NULL
    GROUP BY family
),
family_grid AS (
    SELECT 'wheel' AS family
    UNION ALL SELECT 'brake'
    UNION ALL SELECT 'bogie'
    UNION ALL SELECT 'pantograph'
)
SELECT
    fg.family,
    COALESCE(m.monitored_components, 0) AS monitored_components,
    COALESCE(i.inspected_components, 0) AS inspected_components,
    COALESCE(ib.total_inspections, 0) AS total_inspections,
    COALESCE(ib.detections, 0) AS detections,
    COALESCE(fb.total_failures, 0) AS total_failures,
    COALESCE(pf.failures_with_pre_detection, 0) AS failures_with_pre_detection,
    COALESCE(dto.detections_with_future_failure, 0) AS detections_with_future_failure,
    COALESCE(dto.total_detections, 0) AS total_detections,
    LEAST(1.0, GREATEST(0.0, COALESCE(i.inspected_components::DOUBLE / NULLIF(m.monitored_components, 0), 0.0))) AS inspection_coverage,
    LEAST(1.0, GREATEST(0.0, COALESCE(ib.detections::DOUBLE / NULLIF(ib.total_inspections, 0), 0.0))) AS defect_detection_rate,
    LEAST(1.0, GREATEST(0.0, COALESCE(pf.failures_with_pre_detection::DOUBLE / NULLIF(fb.total_failures, 0), 0.0))) AS pre_failure_detection_rate,
    LEAST(1.0, GREATEST(0.0, 1.0 - COALESCE(dto.detections_with_future_failure::DOUBLE / NULLIF(dto.total_detections, 0), 0.0))) AS false_alert_proxy,
    LEAST(
        1.0,
        GREATEST(
            0.0,
            COALESCE(i.inspected_components::DOUBLE / NULLIF(m.monitored_components, 0), 0.0)
            * COALESCE(pf.failures_with_pre_detection::DOUBLE / NULLIF(fb.total_failures, 0), 0.0)
            * COALESCE(pf.avg_confidence_pre_failure, 0.0)
            * COALESCE(dto.detections_with_future_failure::DOUBLE / NULLIF(dto.total_detections, 0), 0.0)
        )
    ) AS confidence_adjusted_detection_value,
    LEAST(1.0, GREATEST(0.0, COALESCE(ib.family_mapping_consistency_rate, 0.0))) AS family_mapping_consistency_rate,
    LEAST(1.0, GREATEST(0.0, COALESCE(cm.alert_chain_rate, 0.0))) AS alert_chain_rate,
    LEAST(1.0, GREATEST(0.0, COALESCE(mf.maintenance_followthrough_rate, 0.0))) AS maintenance_followthrough_rate,
    COALESCE(pf.lead_time_medio_dias, 0.0) AS lead_time_medio_dias,
    COALESCE(cm.detection_to_alert_mean_h, 0.0) AS detection_to_alert_mean_h,
    COALESCE(cm.alert_to_failure_mean_h, 0.0) AS alert_to_failure_mean_h
FROM family_grid fg
LEFT JOIN monitored m ON m.family = fg.family
LEFT JOIN inspected i ON i.family = fg.family
LEFT JOIN inspection_base ib ON ib.family = fg.family
LEFT JOIN failure_base fb ON fb.family = fg.family
LEFT JOIN pre_failure pf ON pf.family = fg.family
LEFT JOIN det_out dto ON dto.family = fg.family
LEFT JOIN chain_metrics cm ON cm.family = fg.family
LEFT JOIN maintenance_follow mf ON mf.family = fg.family
ORDER BY fg.family;

CREATE OR REPLACE VIEW vw_condition_based_value AS
WITH early_alerts AS (
    SELECT
        COUNT(*) AS total_early_alerts,
        SUM(CASE WHEN atendida_flag = 1 THEN 1 ELSE 0 END) AS early_alerts_attended
    FROM stg_alertas_operativas
    WHERE alerta_temprana_flag = 1
),
cbm_interventions AS (
    SELECT
        COUNT(*) AS cbm_interventions,
        SUM(horas_taller) AS cbm_hours,
        SUM(coste_mano_obra_proxy + coste_material_proxy) AS cbm_cost
    FROM stg_eventos_mantenimiento
    WHERE basada_en_condicion_flag = 1
),
avoidable_corrective_proxy AS (
    SELECT
        SUM(CASE WHEN trigger_origen = 'sensor' AND alerta_temprana_flag = 1 THEN 1 ELSE 0 END) * 0.18 AS avoidable_correctives_proxy
    FROM stg_alertas_operativas
),
operational_value AS (
    SELECT
        SUM(horas_no_disponibles) * 0.12 AS avoidable_unavailability_hours_proxy,
        SUM(cancelaciones_proxy) * 95 AS service_impact_cost_proxy
    FROM stg_disponibilidad_servicio
)
SELECT
    ea.total_early_alerts,
    ea.early_alerts_attended,
    CASE
        WHEN ea.total_early_alerts > 0 THEN ea.early_alerts_attended::DOUBLE / ea.total_early_alerts
        ELSE 0
    END AS early_alert_attention_rate,
    ci.cbm_interventions,
    ci.cbm_hours,
    ci.cbm_cost,
    ac.avoidable_correctives_proxy,
    ov.avoidable_unavailability_hours_proxy,
    ov.service_impact_cost_proxy,
    ov.service_impact_cost_proxy - ci.cbm_cost AS net_operational_value_proxy
FROM early_alerts ea
CROSS JOIN cbm_interventions ci
CROSS JOIN avoidable_corrective_proxy ac
CROSS JOIN operational_value ov;

CREATE OR REPLACE VIEW kpi_valor_potencial_cbm AS
SELECT
    total_early_alerts,
    early_alerts_attended,
    early_alert_attention_rate,
    cbm_interventions,
    avoidable_correctives_proxy,
    avoidable_unavailability_hours_proxy,
    net_operational_value_proxy
FROM vw_condition_based_value;
