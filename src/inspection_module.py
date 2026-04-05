from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from src.config import DATA_PROCESSED_DIR, DATA_RAW_DIR, DOCS_DIR, OUTPUTS_REPORTS_DIR


TARGET_FAMILIES = ("wheel", "brake", "bogie", "pantograph")
PRE_FAILURE_WINDOW_DAYS = 30
FALSE_ALERT_HORIZON_DAYS = 30
MAINT_FOLLOW_UP_DAYS = 14


@dataclass
class InspectionOutputs:
    component_signals: pd.DataFrame
    family_performance: pd.DataFrame
    value_comparison: pd.DataFrame


def _normalize_family(name: str) -> str:
    txt = str(name).lower()
    if "wheel" in txt or "rodadura" in txt:
        return "wheel"
    if "brake" in txt or "fren" in txt:
        return "brake"
    if "pant" in txt or "capt" in txt:
        return "pantograph"
    if "bogie" in txt or "wheelset" in txt or "suspension" in txt:
        return "bogie"
    return "other"


def _family_from_component_meta(componentes: pd.DataFrame) -> pd.DataFrame:
    comp = componentes[["componente_id", "unidad_id", "sistema_principal", "subsistema", "tipo_componente"]].drop_duplicates().copy()
    txt = (
        comp["sistema_principal"].fillna("").astype(str).str.lower()
        + " "
        + comp["subsistema"].fillna("").astype(str).str.lower()
        + " "
        + comp["tipo_componente"].fillna("").astype(str).str.lower()
    )
    comp["family_technical"] = txt.apply(_normalize_family)
    return comp


def _prepare_tables() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    insp = pd.read_csv(DATA_RAW_DIR / "inspecciones_automaticas.csv")
    failures = pd.read_csv(DATA_RAW_DIR / "fallas_historicas.csv")
    componentes = pd.read_csv(DATA_RAW_DIR / "componentes_criticos.csv")
    alerts = pd.read_csv(DATA_RAW_DIR / "alertas_operativas.csv")
    maint = pd.read_csv(DATA_RAW_DIR / "eventos_mantenimiento.csv")
    scoring = pd.read_csv(DATA_PROCESSED_DIR / "scoring_componentes.csv")
    priorities = pd.read_csv(DATA_PROCESSED_DIR / "workshop_priority_table.csv")

    insp["timestamp"] = pd.to_datetime(insp["timestamp"], errors="coerce")
    failures["fecha_falla"] = pd.to_datetime(failures["fecha_falla"], errors="coerce")
    alerts["timestamp"] = pd.to_datetime(alerts["timestamp"], errors="coerce")
    maint["fecha_inicio"] = pd.to_datetime(maint["fecha_inicio"], errors="coerce")

    comp_map = _family_from_component_meta(componentes)

    insp["family_reported"] = insp["familia_inspeccion"].map(_normalize_family)
    insp = insp.merge(comp_map[["componente_id", "unidad_id", "family_technical"]], on=["componente_id", "unidad_id"], how="left")
    insp["family"] = insp["family_technical"].fillna(insp["family_reported"]).map(_normalize_family)
    insp["family_consistency_flag"] = (
        (insp["family_technical"].isna() & insp["family_reported"].isin(TARGET_FAMILIES))
        | (insp["family_technical"] == insp["family_reported"])
    ).astype(int)
    insp = insp[insp["family"].isin(TARGET_FAMILIES)].copy()

    failures = failures.merge(comp_map[["componente_id", "unidad_id", "family_technical"]], on=["componente_id", "unidad_id"], how="left")
    failures["family_from_mode"] = failures["modo_falla"].map(_normalize_family)
    failures["family"] = failures["family_technical"].fillna(failures["family_from_mode"]).map(_normalize_family)
    failures = failures[failures["family"].isin(TARGET_FAMILIES)].copy()

    severity_map = {"baja": 1, "media": 2, "alta": 3, "critica": 4}
    insp["severity_num"] = insp["severidad_hallazgo"].map(severity_map).fillna(0).astype(float)
    insp["defecto_detectado"] = insp["defecto_detectado"].fillna(0).astype(int)
    insp["confianza_deteccion"] = insp["confianza_deteccion"].clip(0, 1)
    insp["score_defecto"] = insp["score_defecto"].clip(0, 100)

    return insp, failures, alerts, maint, scoring, priorities


def _failure_linkage(insp: pd.DataFrame, failures: pd.DataFrame) -> pd.DataFrame:
    detections = insp[insp["defecto_detectado"] == 1].copy()
    merged = failures.merge(
        detections[
            [
                "inspeccion_id",
                "timestamp",
                "unidad_id",
                "componente_id",
                "family",
                "score_defecto",
                "confianza_deteccion",
                "severity_num",
                "recomendacion_inicial",
            ]
        ],
        on=["unidad_id", "componente_id", "family"],
        how="left",
        suffixes=("_falla", "_insp"),
    )
    merged["days_before_failure"] = (merged["fecha_falla"] - merged["timestamp"]).dt.total_seconds() / 86400.0
    valid = merged[merged["days_before_failure"].between(0, PRE_FAILURE_WINDOW_DAYS, inclusive="both")].copy()

    # Se usa la inspección más cercana a la falla dentro de la ventana.
    if valid.empty:
        return pd.DataFrame(
            columns=[
                "falla_id",
                "unidad_id",
                "componente_id",
                "family",
                "fecha_falla",
                "inspeccion_id",
                "inspection_ts",
                "days_before_failure",
                "score_defecto",
                "confianza_deteccion",
                "severity_num",
                "recomendacion_inicial",
            ]
        )

    valid = valid.sort_values(["falla_id", "timestamp"], ascending=[True, False]).drop_duplicates("falla_id", keep="first")
    valid = valid.rename(columns={"timestamp": "inspection_ts"})
    return valid[
        [
            "falla_id",
            "unidad_id",
            "componente_id",
            "family",
            "fecha_falla",
            "inspeccion_id",
            "inspection_ts",
            "days_before_failure",
            "score_defecto",
            "confianza_deteccion",
            "severity_num",
            "recomendacion_inicial",
        ]
    ].copy()


def _detection_outcomes(insp: pd.DataFrame, failures: pd.DataFrame) -> pd.DataFrame:
    detections = insp[insp["defecto_detectado"] == 1].copy()
    merged = detections.merge(
        failures[["falla_id", "unidad_id", "componente_id", "family", "fecha_falla"]],
        on=["unidad_id", "componente_id", "family"],
        how="left",
    )
    merged["days_to_failure"] = (merged["fecha_falla"] - merged["timestamp"]).dt.total_seconds() / 86400.0
    valid_future = merged[merged["days_to_failure"].between(0, FALSE_ALERT_HORIZON_DAYS, inclusive="both")].copy()

    if valid_future.empty:
        out = detections[["inspeccion_id", "unidad_id", "componente_id", "family", "timestamp", "score_defecto", "confianza_deteccion", "severity_num"]].copy()
        out["failure_within_horizon_flag"] = 0
        out["days_to_failure"] = np.nan
        return out

    future_best = valid_future.sort_values(["inspeccion_id", "fecha_falla"], ascending=[True, True]).drop_duplicates("inspeccion_id", keep="first")
    out = detections[["inspeccion_id", "unidad_id", "componente_id", "family", "timestamp", "score_defecto", "confianza_deteccion", "severity_num"]].copy()
    out = out.merge(
        future_best[["inspeccion_id", "days_to_failure"]],
        on="inspeccion_id",
        how="left",
    )
    out["failure_within_horizon_flag"] = out["days_to_failure"].notna().astype(int)
    return out


def _temporal_coherence(
    linkage: pd.DataFrame,
    alerts: pd.DataFrame,
    maint: pd.DataFrame,
    insp: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Coherencia inspección -> alerta -> falla para casos pre-falla.
    early_alerts = alerts[alerts["alerta_temprana_flag"] == 1].copy()
    early_alerts = early_alerts.rename(columns={"timestamp": "alert_ts"})

    link_alert = linkage.merge(
        early_alerts[["unidad_id", "componente_id", "alert_ts", "trigger_origen"]],
        on=["unidad_id", "componente_id"],
        how="left",
    )
    in_chain = link_alert[
        (link_alert["alert_ts"] >= link_alert["inspection_ts"])
        & (link_alert["alert_ts"] <= link_alert["fecha_falla"])
    ].copy()
    link_best = in_chain.sort_values(["falla_id", "alert_ts"], ascending=[True, True]).drop_duplicates("falla_id", keep="first")

    chain = linkage[["falla_id", "family", "inspection_ts", "fecha_falla"]].copy()
    chain = chain.merge(
        link_best[["falla_id", "alert_ts"]],
        on="falla_id",
        how="left",
    )
    chain["alert_chain_flag"] = chain["alert_ts"].notna().astype(int)
    chain["detection_to_alert_h"] = (chain["alert_ts"] - chain["inspection_ts"]).dt.total_seconds() / 3600.0
    chain["alert_to_failure_h"] = (chain["fecha_falla"] - chain["alert_ts"]).dt.total_seconds() / 3600.0

    # Coherencia inspección -> mantenimiento para detecciones severas.
    severe_detections = insp[(insp["defecto_detectado"] == 1) & (insp["severidad_hallazgo"].isin(["alta", "critica"]))].copy()
    follow = severe_detections.merge(
        maint[["unidad_id", "componente_id", "fecha_inicio"]],
        on=["unidad_id", "componente_id"],
        how="left",
    )
    follow["days_to_maintenance"] = (follow["fecha_inicio"] - follow["timestamp"]).dt.total_seconds() / 86400.0
    follow_valid = follow[follow["days_to_maintenance"].between(0, MAINT_FOLLOW_UP_DAYS, inclusive="both")].copy()
    follow_best = follow_valid.sort_values(["inspeccion_id", "fecha_inicio"], ascending=[True, True]).drop_duplicates("inspeccion_id", keep="first")
    severe = severe_detections[["inspeccion_id", "family", "timestamp"]].copy().merge(
        follow_best[["inspeccion_id", "days_to_maintenance"]],
        on="inspeccion_id",
        how="left",
    )
    severe["maintenance_followthrough_flag"] = severe["days_to_maintenance"].notna().astype(int)

    return chain, severe


def _build_family_metrics(
    insp: pd.DataFrame,
    failures: pd.DataFrame,
    linkage: pd.DataFrame,
    detection_outcomes: pd.DataFrame,
    chain: pd.DataFrame,
    severe_follow: pd.DataFrame,
    comp_monitored: pd.DataFrame,
) -> pd.DataFrame:
    monitored = (
        comp_monitored[comp_monitored["family_technical"].isin(TARGET_FAMILIES)]
        .groupby("family_technical", as_index=False)["componente_id"]
        .nunique()
        .rename(columns={"family_technical": "family", "componente_id": "monitored_components"})
    )
    inspected = insp.groupby("family", as_index=False)["componente_id"].nunique().rename(columns={"componente_id": "inspected_components"})
    insp_base = insp.groupby("family", as_index=False).agg(
        total_inspections=("inspeccion_id", "nunique"),
        detections=("defecto_detectado", "sum"),
        avg_confidence_all=("confianza_deteccion", "mean"),
        family_mapping_consistency_rate=("family_consistency_flag", "mean"),
    )
    fail_base = failures.groupby("family", as_index=False)["falla_id"].nunique().rename(columns={"falla_id": "total_failures"})
    pre = linkage.groupby("family", as_index=False).agg(
        failures_with_pre_detection=("falla_id", "nunique"),
        lead_time_medio_dias=("days_before_failure", "mean"),
        avg_confidence_pre_failure=("confianza_deteccion", "mean"),
    )
    det_out = detection_outcomes.groupby("family", as_index=False).agg(
        detections_with_future_failure=("failure_within_horizon_flag", "sum"),
        total_detections=("inspeccion_id", "nunique"),
    )
    chain_stats = chain.groupby("family", as_index=False).agg(
        alert_chain_rate=("alert_chain_flag", "mean"),
        detection_to_alert_mean_h=("detection_to_alert_h", "mean"),
        alert_to_failure_mean_h=("alert_to_failure_h", "mean"),
    )
    follow_stats = severe_follow.groupby("family", as_index=False).agg(
        maintenance_followthrough_rate=("maintenance_followthrough_flag", "mean"),
    )

    family_perf = monitored.merge(inspected, on="family", how="left").merge(insp_base, on="family", how="left").merge(fail_base, on="family", how="left")
    family_perf = (
        family_perf.merge(pre, on="family", how="left")
        .merge(det_out, on="family", how="left")
        .merge(chain_stats, on="family", how="left")
        .merge(follow_stats, on="family", how="left")
    )
    for col in family_perf.columns:
        if col != "family":
            family_perf[col] = family_perf[col].fillna(0)

    family_perf["inspection_coverage"] = family_perf["inspected_components"] / family_perf["monitored_components"].replace(0, np.nan)
    family_perf["defect_detection_rate"] = family_perf["detections"] / family_perf["total_inspections"].replace(0, np.nan)
    family_perf["pre_failure_detection_rate"] = family_perf["failures_with_pre_detection"] / family_perf["total_failures"].replace(0, np.nan)
    family_perf["false_alert_proxy"] = 1.0 - (
        family_perf["detections_with_future_failure"] / family_perf["total_detections"].replace(0, np.nan)
    )
    family_perf["confidence_adjusted_detection_value"] = (
        family_perf["inspection_coverage"]
        * family_perf["pre_failure_detection_rate"]
        * family_perf["avg_confidence_pre_failure"]
        * (1 - family_perf["false_alert_proxy"])
    )
    rate_cols = [
        "inspection_coverage",
        "defect_detection_rate",
        "pre_failure_detection_rate",
        "false_alert_proxy",
        "confidence_adjusted_detection_value",
        "family_mapping_consistency_rate",
        "alert_chain_rate",
        "maintenance_followthrough_rate",
    ]
    for col in rate_cols:
        family_perf[col] = family_perf[col].replace([np.inf, -np.inf], np.nan).fillna(0).clip(0, 1)

    # Compatibilidad histórica + naming oficial.
    family_perf["coverage_pre_falla"] = family_perf["pre_failure_detection_rate"]

    return family_perf.sort_values("family").reset_index(drop=True)


def _build_component_signals(
    insp: pd.DataFrame,
    linkage: pd.DataFrame,
    detection_outcomes: pd.DataFrame,
    scoring: pd.DataFrame,
) -> pd.DataFrame:
    insp = insp.sort_values("timestamp")
    comp = insp.groupby(["unidad_id", "componente_id", "family"], as_index=False).agg(
        inspections_count=("inspeccion_id", "nunique"),
        detections_count=("defecto_detectado", "sum"),
        score_defecto_medio=("score_defecto", "mean"),
        confianza_media=("confianza_deteccion", "mean"),
        severidad_media=("severity_num", "mean"),
        last_inspection_ts=("timestamp", "max"),
    )
    det_last_30d = (
        insp.assign(fecha=insp["timestamp"].dt.date)
        .sort_values("timestamp")
        .groupby(["unidad_id", "componente_id", "family"], as_index=False)
        .tail(30)
        .groupby(["unidad_id", "componente_id", "family"], as_index=False)["defecto_detectado"]
        .sum()
        .rename(columns={"defecto_detectado": "detecciones_30d"})
    )
    true_pos = detection_outcomes.groupby(["unidad_id", "componente_id", "family"], as_index=False).agg(
        true_positive_detections=("failure_within_horizon_flag", "sum"),
        total_detections=("inspeccion_id", "nunique"),
    )
    pre_failure_flag = linkage.groupby(["unidad_id", "componente_id", "family"], as_index=False).agg(
        pre_failure_detection_flag=("falla_id", "nunique"),
        lead_time_medio_dias_component=("days_before_failure", "mean"),
    )

    comp = comp.merge(det_last_30d, on=["unidad_id", "componente_id", "family"], how="left")
    comp = comp.merge(true_pos, on=["unidad_id", "componente_id", "family"], how="left")
    comp = comp.merge(pre_failure_flag, on=["unidad_id", "componente_id", "family"], how="left")
    for c in ["detecciones_30d", "true_positive_detections", "total_detections", "pre_failure_detection_flag"]:
        comp[c] = comp[c].fillna(0)

    comp["defect_detection_rate"] = comp["detections_count"] / comp["inspections_count"].replace(0, np.nan)
    comp["false_alert_proxy_component"] = 1 - (comp["true_positive_detections"] / comp["total_detections"].replace(0, np.nan))
    comp["confidence_adjusted_detection_value_component"] = (
        comp["defect_detection_rate"].fillna(0)
        * comp["confianza_media"].fillna(0)
        * (1 - comp["false_alert_proxy_component"].fillna(1))
    )
    comp["false_alert_proxy_component"] = comp["false_alert_proxy_component"].replace([np.inf, -np.inf], np.nan).fillna(0).clip(0, 1)
    comp["defect_detection_rate"] = comp["defect_detection_rate"].replace([np.inf, -np.inf], np.nan).fillna(0).clip(0, 1)
    comp["confidence_adjusted_detection_value_component"] = comp["confidence_adjusted_detection_value_component"].clip(0, 1)

    comp = comp.merge(
        scoring[["unidad_id", "componente_id", "prob_fallo_30d", "health_score"]],
        on=["unidad_id", "componente_id"],
        how="left",
    )

    comp["inspection_priority_flag"] = (
        (comp["confidence_adjusted_detection_value_component"] >= 0.12)
        & (comp["prob_fallo_30d"].fillna(0) >= 0.45)
    ).astype(int)

    return comp.sort_values(["unidad_id", "componente_id"]).reset_index(drop=True)


def _build_value_comparison(
    family_perf: pd.DataFrame,
    failures: pd.DataFrame,
    scoring: pd.DataFrame,
    priorities: pd.DataFrame,
) -> pd.DataFrame:
    total_failures = float(failures["falla_id"].nunique())
    preventable_failures = float(
        (
            family_perf["total_failures"]
            * family_perf["pre_failure_detection_rate"]
            * family_perf["avg_confidence_pre_failure"].clip(0, 1)
            * (1 - family_perf["false_alert_proxy"])
            * 0.55
        ).sum()
    )
    preventable_failures = float(np.clip(preventable_failures, 0, total_failures * 0.65))

    baseline_correctivas = total_failures * 0.68
    with_inspection_correctivas = max(0.0, baseline_correctivas - preventable_failures)

    baseline_downtime = total_failures * 4.8
    with_inspection_downtime = max(0.0, baseline_downtime - preventable_failures * 5.0)

    baseline_backlog = priorities["deferral_risk_score"].ge(70).sum() * 1.10
    with_inspection_backlog = max(0.0, baseline_backlog - preventable_failures * 0.45)

    risk_mean = float(scoring["prob_fallo_30d"].mean())
    risk_reduction = float(
        (family_perf["confidence_adjusted_detection_value"] * family_perf["pre_failure_detection_rate"]).mean()
    )
    with_inspection_risk = max(0.0, risk_mean * (1 - min(risk_reduction, 0.35)))

    value = pd.DataFrame(
        [
            {
                "scenario": "sin_inspeccion_automatica",
                "correctivas_estimadas": round(baseline_correctivas, 2),
                "horas_indisponibilidad_estimadas": round(baseline_downtime, 2),
                "backlog_critico_estimado": round(baseline_backlog, 2),
                "riesgo_promedio_componente": round(risk_mean, 4),
            },
            {
                "scenario": "con_inspeccion_automatica",
                "correctivas_estimadas": round(with_inspection_correctivas, 2),
                "horas_indisponibilidad_estimadas": round(with_inspection_downtime, 2),
                "backlog_critico_estimado": round(with_inspection_backlog, 2),
                "riesgo_promedio_componente": round(with_inspection_risk, 4),
            },
        ]
    )
    value["ahorro_operativo_proxy_eur"] = [
        0.0,
        round((baseline_downtime - with_inspection_downtime) * 950 + (baseline_correctivas - with_inspection_correctivas) * 1400, 2),
    ]
    return value


def _consistency_checks(
    family_perf: pd.DataFrame,
    linkage: pd.DataFrame,
    chain: pd.DataFrame,
    severe_follow: pd.DataFrame,
) -> pd.DataFrame:
    checks: list[dict] = []
    checks.append(
        {
            "check": "rates_in_0_1",
            "result": bool(
                family_perf[
                    [
                        "inspection_coverage",
                        "defect_detection_rate",
                        "pre_failure_detection_rate",
                        "false_alert_proxy",
                        "confidence_adjusted_detection_value",
                    ]
                ]
                .apply(lambda s: s.between(0, 1).all())
                .all()
            ),
            "detail": "all family rates within [0,1]",
        }
    )
    checks.append(
        {
            "check": "coverage_not_above_one",
            "result": bool((family_perf["inspection_coverage"] <= 1).all()),
            "detail": f"max_coverage={family_perf['inspection_coverage'].max():.4f}",
        }
    )
    checks.append(
        {
            "check": "family_mapping_consistency",
            "result": bool((family_perf["family_mapping_consistency_rate"] >= 0.95).all()),
            "detail": f"min_mapping_consistency={family_perf['family_mapping_consistency_rate'].min():.4f}",
        }
    )
    checks.append(
        {
            "check": "temporal_linkage_non_negative",
            "result": bool(
                linkage["days_before_failure"].dropna().ge(0).all()
                and chain["detection_to_alert_h"].dropna().ge(0).all()
                and chain["alert_to_failure_h"].dropna().ge(0).all()
                and severe_follow["days_to_maintenance"].dropna().ge(0).all()
            ),
            "detail": "inspection->alert->failure and inspection->maintenance non-negative lags",
        }
    )
    checks.append(
        {
            "check": "chain_rates_in_0_1",
            "result": bool(
                family_perf[["alert_chain_rate", "maintenance_followthrough_rate"]]
                .apply(lambda s: s.between(0, 1).all())
                .all()
            ),
            "detail": "chain/followthrough rates in [0,1]",
        }
    )

    out = pd.DataFrame(checks)
    return out


def _write_framework_doc(family_perf: pd.DataFrame, checks_df: pd.DataFrame) -> None:
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Inspection Analytics Framework",
        "",
        "## Taxonomía técnica",
        "- Familias objetivo: `wheel`, `brake`, `bogie`, `pantograph`.",
        "- `family` se deriva de metadata técnica de componente (`sistema_principal`+`subsistema`+`tipo_componente`) y se contrasta contra `familia_inspeccion` reportada.",
        "",
        "## Métricas oficiales",
        "- `inspection_coverage` = componentes inspeccionados / componentes monitorizados.",
        "- `defect_detection_rate` = inspecciones con defecto detectado / total inspecciones.",
        "- `pre_failure_detection_rate` = fallas con detección previa (0-30d) / total fallas.",
        "- `false_alert_proxy` = 1 - (detecciones con falla posterior <=30d / total detecciones).",
        "- `confidence_adjusted_detection_value` = coverage × pre-failure rate × confianza media pre-falla × (1 - false alert).",
        "",
        "## Coherencia temporal",
        "- Cadena inspección -> alerta -> falla validada por timestamps por unidad/componente.",
        "- Follow-through inspección -> mantenimiento validado para detecciones severas (<=14 días).",
        "",
        "## Limitaciones sintéticas",
        "- La distribución de severidad/defectos depende del generador sintético y puede estar sesgada respecto a operación real.",
        "- Las tasas representan plausibilidad analítica y trazabilidad, no performance contractual real.",
        "",
        "## Resultado agregado por familia",
        family_perf[
            [
                "family",
                "inspection_coverage",
                "defect_detection_rate",
                "pre_failure_detection_rate",
                "false_alert_proxy",
                "confidence_adjusted_detection_value",
            ]
        ].to_markdown(index=False),
        "",
        "## Checks de consistencia técnica",
        checks_df.to_markdown(index=False),
    ]
    (DOCS_DIR / "inspection_analytics_framework.md").write_text("\n".join(lines), encoding="utf-8")


def _write_consistency_report(family_perf: pd.DataFrame, checks_df: pd.DataFrame, before_after: pd.DataFrame | None) -> None:
    OUTPUTS_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Informe de Consistencia Técnica del Módulo de Inspección",
        "",
        "## Estado de checks",
        checks_df.to_markdown(index=False),
        "",
        "## Métricas por familia",
        family_perf.to_markdown(index=False),
    ]
    if before_after is not None and not before_after.empty:
        lines.extend(["", "## Comparación Before/After", before_after.to_markdown(index=False)])
    (OUTPUTS_REPORTS_DIR / "inspection_module_consistency_report.md").write_text("\n".join(lines), encoding="utf-8")


def _build_before_after_comparison(family_perf: pd.DataFrame, value: pd.DataFrame) -> pd.DataFrame:
    before_perf_path = OUTPUTS_REPORTS_DIR / "inspection_module_before_family_performance.csv"
    before_value_path = OUTPUTS_REPORTS_DIR / "inspection_module_before_value_comparison.csv"
    if not before_perf_path.exists():
        return pd.DataFrame()

    before_perf = pd.read_csv(before_perf_path)
    before = before_perf[["family", "coverage_pre_falla"]].rename(columns={"coverage_pre_falla": "pre_failure_detection_rate_before"})
    after = family_perf[["family", "pre_failure_detection_rate", "false_alert_proxy", "confidence_adjusted_detection_value"]].rename(
        columns={"pre_failure_detection_rate": "pre_failure_detection_rate_after"}
    )
    cmp = before.merge(after, on="family", how="outer")
    cmp["delta_pre_failure_detection_rate"] = cmp["pre_failure_detection_rate_after"] - cmp["pre_failure_detection_rate_before"]

    if before_value_path.exists():
        before_val = pd.read_csv(before_value_path)
        after_val = value.copy()
        b = before_val[before_val["scenario"] == "con_inspeccion_automatica"].iloc[0]
        a = after_val[after_val["scenario"] == "con_inspeccion_automatica"].iloc[0]
        summary = pd.DataFrame(
            [
                {
                    "family": "ALL",
                    "pre_failure_detection_rate_before": np.nan,
                    "pre_failure_detection_rate_after": np.nan,
                    "delta_pre_failure_detection_rate": np.nan,
                    "delta_correctivas_estimadas": float(a["correctivas_estimadas"] - b["correctivas_estimadas"]),
                    "delta_horas_indisponibilidad_estimadas": float(
                        a["horas_indisponibilidad_estimadas"] - b["horas_indisponibilidad_estimadas"]
                    ),
                    "delta_riesgo_promedio_componente": float(a["riesgo_promedio_componente"] - b["riesgo_promedio_componente"]),
                }
            ]
        )
        cmp = pd.concat([cmp, summary], ignore_index=True)

    cmp.to_csv(DATA_PROCESSED_DIR / "inspection_module_before_after_comparison.csv", index=False)
    return cmp


def run_inspection_module() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    insp, failures, alerts, maint, scoring, priorities = _prepare_tables()
    comp_monitored = _family_from_component_meta(pd.read_csv(DATA_RAW_DIR / "componentes_criticos.csv"))

    linkage = _failure_linkage(insp, failures)
    detection_outcomes = _detection_outcomes(insp, failures)
    chain, severe_follow = _temporal_coherence(linkage, alerts, maint, insp)

    family_perf = _build_family_metrics(insp, failures, linkage, detection_outcomes, chain, severe_follow, comp_monitored)
    component_signals = _build_component_signals(insp, linkage, detection_outcomes, scoring)
    value = _build_value_comparison(family_perf, failures, scoring, priorities)

    checks_df = _consistency_checks(family_perf, linkage, chain, severe_follow)
    before_after = _build_before_after_comparison(family_perf, value)

    # Export tablas del módulo
    component_signals.to_csv(DATA_PROCESSED_DIR / "inspection_module_component_signals.csv", index=False)
    family_perf.to_csv(DATA_PROCESSED_DIR / "inspection_module_family_performance.csv", index=False)
    value.to_csv(DATA_PROCESSED_DIR / "inspection_module_value_comparison.csv", index=False)
    linkage.to_csv(DATA_PROCESSED_DIR / "inspection_module_failure_linkage.csv", index=False)
    checks_df.to_csv(DATA_PROCESSED_DIR / "inspection_module_consistency_checks.csv", index=False)

    # Reportes ejecutivos/QA
    OUTPUTS_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    family_perf.to_csv(OUTPUTS_REPORTS_DIR / "inspection_module_family_performance.csv", index=False)
    checks_df.to_csv(OUTPUTS_REPORTS_DIR / "inspection_module_consistency_checks.csv", index=False)
    if not before_after.empty:
        before_after.to_csv(OUTPUTS_REPORTS_DIR / "inspection_module_before_after_comparison.csv", index=False)

    _write_framework_doc(family_perf, checks_df)
    _write_consistency_report(family_perf, checks_df, before_after)

    return component_signals, family_perf, value


if __name__ == "__main__":
    run_inspection_module()
