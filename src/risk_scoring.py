from __future__ import annotations

import numpy as np
import pandas as pd

from src.config import DATA_PROCESSED_DIR, DATA_RAW_DIR, OUTPUTS_REPORTS_DIR, DOCS_DIR
from src.recommendation_engine import assign_component_recommendations


def _sigmoid(x: pd.Series) -> pd.Series:
    return 1.0 / (1.0 + np.exp(-x))


def _minmax01(s: pd.Series) -> pd.Series:
    s = s.astype(float)
    lo = float(s.min())
    hi = float(s.max())
    if hi - lo < 1e-9:
        return pd.Series(np.zeros(len(s)), index=s.index)
    return (s - lo) / (hi - lo)


def _family_threshold(df: pd.DataFrame, family_map: dict[str, float], default: float, col: str = "component_family") -> pd.Series:
    return df[col].map(family_map).fillna(default).astype(float)


def _component_family(row: pd.Series) -> str:
    txt = f"{row.get('sistema_principal','')} {row.get('subsistema','')} {row.get('tipo_componente','')}".lower()
    if "wheel" in txt or "rodadura" in txt:
        return "wheel"
    if "brake" in txt or "fren" in txt:
        return "brake"
    if "pant" in txt or "capt" in txt:
        return "pantograph"
    return "bogie"


def _build_modeling_framework_doc() -> None:
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Modeling Framework | Salud, Riesgo y RUL",
        "",
        "## 1) Degradation scoring basado en reglas",
        "- Inputs: `deterioration_index`, `degradation_velocity`, `inspection_defect_score_recent`, `critical_alerts_count`, `backlog_exposure_flag`.",
        "- Lógica: penalización aditiva interpretable con límites [0,100].",
        "- Supuesto: deterioro acumulado y alertas críticas elevan riesgo de forma no lineal pero acotable.",
        "- Limitación: no estima física de daño específica por fabricante/componente.",
        "- Utilidad operativa: priorización rápida y trazable por componente.",
        "",
        "## 2) Failure risk scoring interpretable",
        "- Inputs: salud, deterioro, velocidad de degradación, estrés operativo, historial de fallas, exposición de backlog.",
        "- Lógica: combinación lineal + sigmoide para `prob_fallo_30d`.",
        "- Supuesto: la probabilidad depende de degradación reciente y carga operacional.",
        "- Limitación: calibración sobre dato sintético; requiere recalibración con histórico real.",
        "- Utilidad operativa: ranking de riesgo para decidir entrada a taller.",
        "",
        "## 3) Early warning rules",
        "- Inputs: `prob_fallo_30d`, `component_health_score`, confianza de señal y criticidad operacional.",
        "- Lógica: jerarquía de decisión no destructiva con 7 categorías de acción.",
        "- Supuesto: umbrales conservadores para minimizar fallas no detectadas.",
        "- Limitación: trade-off precision/recall depende de ventanas de alerta elegidas.",
        "",
        "## 4) RUL proxy",
        "- Inputs: tendencia de salud 60 días + distancia a umbral técnico.",
        "- Lógica: extrapolación lineal con tope de 365 días y banda de confianza.",
        "- Supuesto: degradación localmente cuasi-lineal en ventana corta.",
        "- Limitación: puede infraestimar mejoras tras mantenimiento mayor.",
        "",
        "## 5) Riesgo de indisponibilidad por unidad",
        "- Inputs: riesgo de componentes, criticidad de servicio, backlog, impacto de servicio y sustitución requerida.",
        "- Lógica: agregación ponderada al nivel unidad para `unit_unavailability_risk_score`.",
        "- Utilidad operativa: secuenciar intervenciones con impacto en servicio.",
    ]
    (DOCS_DIR / "modeling_framework.md").write_text("\n".join(lines), encoding="utf-8")


def run_risk_scoring() -> pd.DataFrame:
    component_day = pd.read_csv(DATA_PROCESSED_DIR / "component_day_features.csv")
    unit_day = pd.read_csv(DATA_PROCESSED_DIR / "unit_day_features.csv")
    fallas = pd.read_csv(DATA_RAW_DIR / "fallas_historicas.csv")

    component_day["fecha"] = pd.to_datetime(component_day["fecha"], errors="coerce")
    unit_day["fecha"] = pd.to_datetime(unit_day["fecha"], errors="coerce")
    fallas["fecha_falla"] = pd.to_datetime(fallas["fecha_falla"], errors="coerce")

    latest = component_day["fecha"].max()
    latest_comp = component_day[component_day["fecha"] == latest].copy()
    latest_unit = unit_day[unit_day["fecha"] == latest][
        ["unidad_id", "predicted_unavailability_risk", "impact_on_service_proxy", "service_exposure", "fleet_dependency_flag"]
    ].copy()

    df = latest_comp.merge(latest_unit, on="unidad_id", how="left")
    df["component_family"] = df.apply(_component_family, axis=1)
    if "deterioration_index" not in df.columns:
        df["deterioration_index"] = (100 - df["estimated_health_index"].fillna(55)).clip(0, 100)
    if "maintenance_restoration_index" not in df.columns:
        days_since_maint = df["days_since_last_maintenance"].fillna(3650).clip(lower=0)
        df["maintenance_restoration_index"] = (100 * np.exp(-days_since_maint / 45.0)).clip(0, 100)

    # Score técnico de salud (alto = mejor)
    df["component_health_score"] = (
        df["estimated_health_index"].fillna(60) * 0.58
        + (100 - df["deterioration_index"].fillna(40)).clip(0, 100) * 0.17
        + (100 - (df["degradation_velocity"].fillna(0) * 10)).clip(0, 100) * 0.12
        + (100 - df["inspection_defect_score_recent"].fillna(0)).clip(0, 100) * 0.07
        + df["maintenance_restoration_index"].fillna(0) * 0.10
        - df["backlog_exposure_flag"].fillna(0) * 2.0
    ).clip(5, 100)

    # Riesgo de falla interpretable (30 días), calibrado para evitar colapso de clases.
    risk_raw = (
        0.45 * _minmax01(df["deterioration_index"].fillna(40))
        + 0.22 * _minmax01(df["degradation_velocity"].fillna(0))
        + 0.12 * _minmax01(df["operating_stress_index"].fillna(0.9))
        + 0.08 * _minmax01(df["anomaly_count_30d"].fillna(0))
        + 0.05 * _minmax01(df["shock_event_count"].fillna(0))
        + 0.05 * _minmax01(df["backlog_exposure_flag"].fillna(0))
        + 0.03 * _minmax01(df["repetitive_failure_flag"].fillna(0))
        - 0.10 * _minmax01(df["maintenance_restoration_index"].fillna(0))
    ).clip(0, 1)
    family_pct = risk_raw.groupby(df["component_family"]).rank(pct=True)
    global_pct = risk_raw.rank(pct=True)
    calibrated_pct = (0.60 * family_pct + 0.40 * global_pct).clip(0, 1)
    df["component_failure_risk_score"] = (0.03 + 0.88 * (calibrated_pct ** 1.55)).clip(0.02, 0.95)

    # Driver principal (mayor contribución)
    drivers = pd.DataFrame(
        {
            "degradacion": (
                _minmax01(df["deterioration_index"].fillna(40)) * 0.45
                + _minmax01(df["degradation_velocity"].fillna(0)) * 0.35
                + _minmax01(df["inspection_defect_score_recent"].fillna(0)) * 0.20
            ),
            "estres_operacion": _minmax01(df["operating_stress_index"].fillna(0.9)),
            "anomalias": _minmax01(df["anomaly_count_30d"].fillna(0) + 0.6 * df["shock_event_count"].fillna(0)),
            "backlog": _minmax01(df["backlog_exposure_flag"].fillna(0) + (df["days_since_last_maintenance"].fillna(365) > 120).astype(int)),
            "repetitividad": _minmax01(df["repetitive_failure_flag"].fillna(0) + (df["days_since_last_failure"].fillna(365) < 60).astype(int)),
        }
    )
    base_driver = drivers.idxmax(axis=1)
    anom_signal = df["anomaly_count_30d"].fillna(0) + 0.6 * df["shock_event_count"].fillna(0)
    stress_q = float(df["operating_stress_index"].fillna(0).quantile(0.88))
    anom_q = float(anom_signal.quantile(0.85))

    df["main_risk_driver"] = base_driver
    df.loc[
        (df["backlog_exposure_flag"].fillna(0) == 1) & (df["component_failure_risk_score"] >= 0.55),
        "main_risk_driver",
    ] = "backlog"
    df.loc[
        (df["operating_stress_index"].fillna(0) >= stress_q)
        & (df["deterioration_index"].fillna(0) < 80)
        & (df["component_failure_risk_score"] >= 0.45),
        "main_risk_driver",
    ] = "estres_operacion"
    df.loc[
        (anom_signal >= anom_q)
        & (df["component_failure_risk_score"] >= 0.50),
        "main_risk_driver",
    ] = "anomalias"
    df.loc[
        (df["repetitive_failure_flag"].fillna(0) == 1)
        & (df["component_failure_risk_score"] >= 0.50)
        & (df["deterioration_index"].fillna(0) < 75),
        "main_risk_driver",
    ] = "repetitividad"

    # Confianza del score (calibrada para evitar colapso de clases).
    completeness_cols = [
        "sensor_mean",
        "sensor_std",
        "rolling_mean_7d",
        "rolling_std_7d",
        "deterioration_index",
        "operating_stress_index",
        "degradation_velocity",
    ]
    completeness = df[completeness_cols].notna().mean(axis=1)
    freshness = np.exp(-df["days_since_last_maintenance"].fillna(180).clip(0, 365) / 180.0)
    inspection_conf = df["defect_confidence_recent"].fillna(0.55).clip(0, 1)
    anomaly_support = _minmax01(df["anomaly_count_30d"].fillna(0) + 0.5 * df["shock_event_count"].fillna(0))

    quality_signal = (
        completeness * 0.45
        + freshness * 0.20
        + inspection_conf * 0.25
        + (1 - anomaly_support) * 0.10
    ).clip(0, 1)

    q_low = float(quality_signal.quantile(0.20))
    q_high = float(quality_signal.quantile(0.70))
    q_low = max(0.35, min(q_low, 0.60))
    q_high = max(q_high, q_low + 0.08)
    q_high = min(q_high, 0.90)
    df["confidence_flag"] = np.where(
        quality_signal >= q_high,
        "alta",
        np.where(quality_signal >= q_low, "media", "baja"),
    )

    # Recomendación inicial de acción (motor jerárquico no colapsado).
    df = assign_component_recommendations(df)

    # Score unidad
    unit_score = (
        df.groupby("unidad_id", as_index=False)
        .agg(
            component_failure_risk_mean=("component_failure_risk_score", "mean"),
            component_failure_risk_p90=("component_failure_risk_score", lambda s: float(np.percentile(s, 90))),
            components_high_risk=("component_failure_risk_score", lambda s: int((s >= 0.65).sum())),
            component_count=("component_failure_risk_score", "size"),
            avg_component_deterioration=("deterioration_index", "mean"),
            service_exposure=("service_exposure", "mean"),
            predicted_unavailability_risk=("predicted_unavailability_risk", "mean"),
            impact_on_service_proxy=("impact_on_service_proxy", "mean"),
            fleet_dependency_flag=("fleet_dependency_flag", "max"),
        )
    )
    unit_score["high_risk_ratio"] = unit_score["components_high_risk"] / unit_score["component_count"].clip(lower=1)
    unit_score["unit_unavailability_risk_score"] = (
        unit_score["component_failure_risk_mean"] * 100 * 0.30
        + unit_score["component_failure_risk_p90"] * 100 * 0.22
        + unit_score["predicted_unavailability_risk"].fillna(0) * 100 * 0.18
        + unit_score["impact_on_service_proxy"].fillna(0) * 0.12
        + unit_score["avg_component_deterioration"].fillna(0) * 0.08
        + unit_score["high_risk_ratio"] * 100 * 0.10
        + unit_score["fleet_dependency_flag"] * 3.0
    ).clip(0, 100)

    # Comparación score técnico vs evidencia histórica (falla 60d)
    history = component_day[["fecha", "componente_id", "estimated_health_index", "deterioration_index", "degradation_velocity"]].copy()
    history = history.sort_values(["componente_id", "fecha"])
    events = fallas[["componente_id", "fecha_falla"]].dropna().sort_values(["componente_id", "fecha_falla"])

    history_chunks: list[pd.DataFrame] = []
    for comp_id, grp in history.groupby("componente_id", sort=False):
        comp_events = events[events["componente_id"] == comp_id][["fecha_falla"]]
        grp = grp.sort_values("fecha")
        if comp_events.empty:
            grp["fecha_falla"] = pd.NaT
            history_chunks.append(grp)
            continue

        merged = pd.merge_asof(
            grp,
            comp_events.sort_values("fecha_falla"),
            left_on="fecha",
            right_on="fecha_falla",
            direction="forward",
        )
        history_chunks.append(merged)

    history_merge = pd.concat(history_chunks, ignore_index=True)
    history_merge["days_to_next_failure"] = (history_merge["fecha_falla"] - history_merge["fecha"]).dt.days
    history_merge["failure_in_30d"] = history_merge["days_to_next_failure"].between(0, 30, inclusive="both").astype(int)

    history_merge["health_risk_proxy"] = history_merge["deterioration_index"].fillna((100 - history_merge["estimated_health_index"]).clip(0, 100)).clip(0, 100)
    evidence_table = (
        history_merge.groupby(pd.cut(history_merge["health_risk_proxy"], bins=[-1, 20, 40, 60, 80, 100]), observed=False)
        .agg(component_days=("failure_in_30d", "size"), failures_30d=("failure_in_30d", "sum"))
        .reset_index()
    )
    evidence_table["failure_rate_30d"] = (evidence_table["failures_30d"] / evidence_table["component_days"]).round(4)

    # Señales más determinantes (correlación con riesgo)
    feature_cols = [
        "deterioration_index",
        "degradation_velocity",
        "operating_stress_index",
        "anomaly_count_30d",
        "shock_event_count",
        "inspection_defect_score_recent",
        "backlog_exposure_flag",
        "maintenance_restoration_index",
        "days_since_last_failure",
        "days_since_last_maintenance",
    ]
    det = []
    for col in feature_cols:
        if col in df.columns:
            x = df[col].fillna(df[col].median()).rank(method="average")
            y = df["component_failure_risk_score"].rank(method="average")
            if x.nunique(dropna=True) < 2 or y.nunique(dropna=True) < 2:
                corr = 0.0
            else:
                corr = x.corr(y, method="pearson")
            det.append({"feature": col, "spearman_corr_with_failure_risk": round(float(corr), 4)})
    determinantes = pd.DataFrame(det).sort_values("spearman_corr_with_failure_risk", ascending=False)

    # Segmentación por familia
    segmentacion = (
        df.groupby("component_family", as_index=False)
        .agg(
            component_count=("componente_id", "nunique"),
            health_score_avg=("component_health_score", "mean"),
            failure_risk_avg=("component_failure_risk_score", "mean"),
        )
        .sort_values("failure_risk_avg", ascending=False)
    )

    # Accuracy práctica de early warning proxy (umbral riesgo)
    history_eval = history_merge[["componente_id", "fecha", "failure_in_30d", "health_risk_proxy"]].copy()
    history_eval["early_warning_flag"] = (history_eval["health_risk_proxy"] >= 55).astype(int)
    tp = int(((history_eval["early_warning_flag"] == 1) & (history_eval["failure_in_30d"] == 1)).sum())
    fp = int(((history_eval["early_warning_flag"] == 1) & (history_eval["failure_in_30d"] == 0)).sum())
    fn = int(((history_eval["early_warning_flag"] == 0) & (history_eval["failure_in_30d"] == 1)).sum())
    precision = tp / max(tp + fp, 1)
    recall = tp / max(tp + fn, 1)

    precision_df = pd.DataFrame(
        [
            {
                "threshold": "health_risk_proxy>=55",
                "tp": tp,
                "fp": fp,
                "fn": fn,
                "precision": round(precision, 4),
                "recall": round(recall, 4),
            }
        ]
    )

    # Salidas obligatorias
    out = df[
        [
            "fecha",
            "unidad_id",
            "componente_id",
            "sistema_principal",
            "subsistema",
            "tipo_componente",
            "component_family",
            "component_health_score",
            "deterioration_index",
            "component_failure_risk_score",
            "confidence_flag",
            "main_risk_driver",
            "recommended_action_initial",
            "recommendation_rule_id",
            "recommendation_conflict_flag",
            "recommendation_rationale",
        ]
    ].copy()

    out.to_csv(DATA_PROCESSED_DIR / "component_model_scores.csv", index=False)

    out[["fecha", "unidad_id", "componente_id", "component_health_score"]].to_csv(
        DATA_PROCESSED_DIR / "component_health_score.csv", index=False
    )
    out[["fecha", "unidad_id", "componente_id", "component_failure_risk_score", "confidence_flag", "main_risk_driver", "recommended_action_initial"]].to_csv(
        DATA_PROCESSED_DIR / "component_failure_risk_score.csv", index=False
    )

    unit_score[["unidad_id", "unit_unavailability_risk_score"]].to_csv(
        DATA_PROCESSED_DIR / "unit_unavailability_risk_score.csv", index=False
    )

    evidence_table.to_csv(DATA_PROCESSED_DIR / "score_vs_evidence_history.csv", index=False)
    determinantes.to_csv(DATA_PROCESSED_DIR / "risk_signal_determinants.csv", index=False)
    segmentacion.to_csv(DATA_PROCESSED_DIR / "risk_segmentation_component_family.csv", index=False)
    precision_df.to_csv(DATA_PROCESSED_DIR / "early_warning_practical_accuracy.csv", index=False)

    # Compatibilidad con outputs legacy
    legacy = out.merge(unit_score[["unidad_id", "unit_unavailability_risk_score"]], on="unidad_id", how="left")
    legacy["health_score"] = legacy["component_health_score"]
    legacy["prob_fallo_30d"] = legacy["component_failure_risk_score"]
    legacy["riesgo_ajustado_negocio"] = (
        legacy["prob_fallo_30d"] * 100 * 0.7
        + (100 - legacy["health_score"]) * 0.2
        + legacy["unit_unavailability_risk_score"].fillna(0) * 0.1
    ).clip(0, 100)

    legacy = legacy.sort_values("riesgo_ajustado_negocio", ascending=False).reset_index(drop=True)
    legacy["ranking_riesgo"] = np.arange(1, len(legacy) + 1)
    legacy.to_csv(DATA_PROCESSED_DIR / "scoring_componentes.csv", index=False)

    # Copias para reportes
    OUTPUTS_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    legacy.head(40).to_csv(OUTPUTS_REPORTS_DIR / "scoring_componentes_top40.csv", index=False)
    determinantes.to_csv(OUTPUTS_REPORTS_DIR / "drivers_principales_riesgo.csv", index=False)

    _build_modeling_framework_doc()
    return legacy


if __name__ == "__main__":
    run_risk_scoring()
