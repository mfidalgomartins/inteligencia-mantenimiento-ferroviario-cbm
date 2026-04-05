from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable

import numpy as np
import pandas as pd

from src.config import DATA_PROCESSED_DIR, DATA_RAW_DIR, DOCS_DIR


@dataclass
class FeatureOutputs:
    component_day_features: pd.DataFrame
    unit_day_features: pd.DataFrame
    fleet_week_features: pd.DataFrame
    workshop_priority_features: pd.DataFrame


def _safe_div(a: pd.Series, b: pd.Series, default: float = 0.0) -> pd.Series:
    out = a / b.replace(0, np.nan)
    return out.replace([np.inf, -np.inf], np.nan).fillna(default)


def _compute_days_since_event(
    base: pd.DataFrame,
    events: pd.DataFrame,
    base_component_col: str,
    base_date_col: str,
    event_component_col: str,
    event_date_col: str,
    out_col: str,
) -> pd.Series:
    rows = []
    events = events[[event_component_col, event_date_col]].dropna().copy()
    events[event_date_col] = pd.to_datetime(events[event_date_col], errors="coerce")
    events = events.dropna()

    if events.empty:
        return pd.Series(np.nan, index=base.index, name=out_col)

    events = events.rename(columns={event_component_col: "componente_id", event_date_col: "event_date"})

    tmp = base[[base_component_col, base_date_col]].copy()
    tmp[base_date_col] = pd.to_datetime(tmp[base_date_col], errors="coerce")
    tmp = tmp.rename(columns={base_component_col: "componente_id", base_date_col: "fecha"})
    tmp["_idx"] = tmp.index

    for comp_id, grp in tmp.groupby("componente_id", sort=False):
        comp_events = events[events["componente_id"] == comp_id][["event_date"]].sort_values("event_date")
        comp_base = grp.sort_values("fecha")

        if comp_events.empty:
            comp_base[out_col] = np.nan
        else:
            merged = pd.merge_asof(
                comp_base,
                comp_events,
                left_on="fecha",
                right_on="event_date",
                direction="backward",
            )
            comp_base[out_col] = (merged["fecha"] - merged["event_date"]).dt.days
        rows.append(comp_base[["_idx", out_col]])

    out = pd.concat(rows, ignore_index=True).set_index("_idx")[out_col].sort_index()
    return out


def _build_component_day_features() -> pd.DataFrame:
    mart = pd.read_csv(DATA_PROCESSED_DIR / "mart_component_day.csv")
    componentes = pd.read_csv(DATA_RAW_DIR / "componentes_criticos.csv")
    unidades = pd.read_csv(DATA_RAW_DIR / "unidades.csv")
    parametros = pd.read_csv(DATA_RAW_DIR / "parametros_operativos_contexto.csv")
    mantenimiento = pd.read_csv(DATA_RAW_DIR / "eventos_mantenimiento.csv")
    fallas = pd.read_csv(DATA_RAW_DIR / "fallas_historicas.csv")
    backlog = pd.read_csv(DATA_RAW_DIR / "backlog_mantenimiento.csv")

    mart["fecha"] = pd.to_datetime(mart["fecha"], errors="coerce")
    parametros["fecha"] = pd.to_datetime(parametros["fecha"], errors="coerce")
    backlog["fecha"] = pd.to_datetime(backlog["fecha"], errors="coerce")

    comp_ref = componentes[[
        "componente_id",
        "unidad_id",
        "criticidad_componente",
        "edad_componente_dias",
        "vida_util_teorica_dias",
        "ciclos_acumulados",
        "vida_util_teorica_ciclos",
        "tipo_componente",
        "subsistema",
        "sistema_principal",
    ]].drop_duplicates()

    df = mart.merge(comp_ref, on=["componente_id", "unidad_id", "tipo_componente", "subsistema", "sistema_principal"], how="left")
    if "criticidad_componente" not in df.columns:
        left_col = "criticidad_componente_x"
        right_col = "criticidad_componente_y"
        if left_col in df.columns and right_col in df.columns:
            df["criticidad_componente"] = df[left_col].fillna(df[right_col])
        elif left_col in df.columns:
            df["criticidad_componente"] = df[left_col]
        elif right_col in df.columns:
            df["criticidad_componente"] = df[right_col]

    # Eventos de choque y anomalías (evita colapso a valores siempre activos)
    rolling_scale = (
        df["rolling_std_7d"]
        .fillna(df["sensor_std"])
        .replace(0, np.nan)
        .fillna(float(max(df["sensor_std"].quantile(0.35), 0.05)))
    )
    shock_residual = (df["sensor_max"] - df["rolling_mean_7d"]).fillna(0)
    shock_z = shock_residual / rolling_scale.clip(lower=0.05)
    shock_floor = df["sensor_std"].quantile(0.75) * 0.6
    df["shock_event"] = ((shock_z >= 2.9) & (shock_residual >= shock_floor)).astype(int)

    std_q95 = float(df["sensor_std"].quantile(0.95))
    anomaly_signal = (
        (df["sensor_std"] >= std_q95)
        | (df["operating_stress_index"] > 1.25)
        | (df["critical_alerts_count"] >= 2)
        | (df["shock_event"] == 1)
    )
    df["anomaly_event"] = anomaly_signal.astype(int)

    df = df.sort_values(["componente_id", "fecha"]).reset_index(drop=True)
    df["anomaly_count_30d"] = (
        df.groupby("componente_id")["anomaly_event"]
        .rolling(window=30, min_periods=1)
        .sum()
        .reset_index(level=0, drop=True)
    )
    df["shock_event_count"] = (
        df.groupby("componente_id")["shock_event"]
        .rolling(window=30, min_periods=1)
        .sum()
        .reset_index(level=0, drop=True)
    )

    df["inspection_defect_score_recent"] = (
        df.groupby("componente_id")["inspection_defect_score"]
        .rolling(window=14, min_periods=1)
        .max()
        .reset_index(level=0, drop=True)
    )
    df["defect_confidence_recent"] = (
        df.groupby("componente_id")["inspection_confidence"]
        .rolling(window=14, min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
    )

    df["days_since_last_maintenance"] = _compute_days_since_event(
        base=df,
        events=mantenimiento,
        base_component_col="componente_id",
        base_date_col="fecha",
        event_component_col="componente_id",
        event_date_col="fecha_fin",
        out_col="days_since_last_maintenance",
    )

    df["days_since_last_failure"] = _compute_days_since_event(
        base=df,
        events=fallas,
        base_component_col="componente_id",
        base_date_col="fecha",
        event_component_col="componente_id",
        event_date_col="fecha_falla",
        out_col="days_since_last_failure",
    )

    unit_line = unidades[["unidad_id", "linea_servicio"]].drop_duplicates()
    df = df.merge(unit_line, on="unidad_id", how="left")

    env = parametros[["fecha", "linea_servicio", "temperatura_ambiente", "humedad", "intensidad_servicio", "nivel_congestion_operativa_proxy"]]
    df = df.merge(env, on=["fecha", "linea_servicio"], how="left")
    df["environment_stress_proxy"] = (
        (df["temperatura_ambiente"].sub(df["temperatura_ambiente"].median()).abs() / 18.0)
        + (df["humedad"].sub(50).abs() / 55.0)
        + (df["intensidad_servicio"].fillna(1.0) * 0.65)
        + (df["nivel_congestion_operativa_proxy"].fillna(0.4) * 0.9)
    ).clip(0, 4.5)

    backlog_key = backlog[["fecha", "unidad_id", "componente_id", "antiguedad_backlog_dias", "riesgo_acumulado", "severidad_pendiente"]]
    df = df.merge(backlog_key, on=["fecha", "unidad_id", "componente_id"], how="left")
    backlog_risk_q80 = float(backlog["riesgo_acumulado"].dropna().quantile(0.80)) if backlog["riesgo_acumulado"].notna().any() else 40.0
    df["backlog_exposure_flag"] = (
        (df["antiguedad_backlog_dias"].fillna(0) >= 25)
        | ((df["riesgo_acumulado"].notna()) & (df["riesgo_acumulado"].fillna(0) >= backlog_risk_q80))
        | (df["severidad_pendiente"].fillna("baja").isin(["alta", "critica"]))
    ).astype(int)

    # Semántica oficial:
    # - `estimated_health_*`: alto = mejor condición
    # - `deterioration_*` / `degradation_*`: alto = peor condición
    # - `maintenance_restoration_*`: alto = efecto restaurativo reciente
    if "deterioration_input_index" not in df.columns:
        df["deterioration_input_index"] = (100 - df["estimated_health_input_index"].clip(0, 100)).clip(0, 100)

    # Se normaliza la volatilidad sensórica para que degrade de forma positiva y consistente.
    sensor_std_scale = max(float(df["sensor_std"].quantile(0.95)), 1.0)
    df["sensor_std_norm"] = (df["sensor_std"].fillna(0) / sensor_std_scale).clip(0, 2.5)
    sensor_change_scale = max(float(df["rolling_abs_change_7d"].fillna(0).quantile(0.95)), 1.0)
    df["sensor_change_norm"] = (df["rolling_abs_change_7d"].fillna(0) / sensor_change_scale).clip(0, 2.5)
    df["shock_event_rate"] = (df["shock_event_count"].fillna(0) / 30.0).clip(0, 1.0)
    stress_scale = max(float(df["operating_stress_index"].fillna(0).quantile(0.95)), 1.0)
    df["operating_stress_norm"] = (df["operating_stress_index"].fillna(0) / stress_scale).clip(0, 2.0)

    df["degradation_velocity"] = (
        2.10 * df["sensor_std_norm"]
        + 1.35 * df["sensor_change_norm"]
        + 1.05 * df["shock_event_rate"]
        + 0.95 * df["operating_stress_norm"]
        + 1.25 * (df["deterioration_input_index"].fillna(0) / 100.0)
    ).clip(0, 10)

    days_since_maint = df["days_since_last_maintenance"].fillna(3650).clip(lower=0)
    maint_decay = np.exp(-days_since_maint / 180.0)
    maint_freq_norm = (df["maintenance_frequency_180d"].fillna(0) / 6.0).clip(0, 1)
    df["maintenance_restoration_index"] = (100 * (0.65 * maint_decay + 0.35 * maint_freq_norm)).clip(0, 100)

    base_health_input = df["estimated_health_input_index"].fillna(55).clip(0, 100)
    df["estimated_health_index"] = (
        base_health_input * 0.52
        + (100 - (df["degradation_velocity"].fillna(0) * 10)).clip(0, 100) * 0.18
        + (100 - df["inspection_defect_score_recent"].fillna(0)).clip(0, 100) * 0.12
        + df["maintenance_restoration_index"] * 0.10
        - df["critical_alerts_count"].fillna(0) * 0.65
        - df["backlog_exposure_flag"] * 2.0
        + 14.0
    ).clip(1, 100)
    df["deterioration_index"] = (100 - df["estimated_health_index"]).clip(0, 100)

    out = df[
        [
            "fecha",
            "unidad_id",
            "componente_id",
            "sistema_principal",
            "subsistema",
            "tipo_componente",
            "sensor_mean",
            "sensor_std",
            "sensor_max",
            "rolling_mean_7d",
            "rolling_std_7d",
            "rolling_slope",
            "shock_event_count",
            "anomaly_count_30d",
            "inspection_defect_score_recent",
            "defect_confidence_recent",
            "days_since_last_maintenance",
            "days_since_last_failure",
            "maintenance_frequency_180d",
            "repetitive_failure_flag",
            "age_ratio",
            "cycles_ratio",
            "deterioration_input_index",
            "estimated_health_index",
            "deterioration_index",
            "degradation_velocity",
            "operating_stress_index",
            "environment_stress_proxy",
            "maintenance_restoration_index",
            "alert_density_30d",
            "critical_alerts_count",
            "backlog_exposure_flag",
            "criticidad_componente",
            "linea_servicio",
        ]
    ].copy()

    out = out.rename(columns={"alert_density_30d": "alert_density"})
    out.to_csv(DATA_PROCESSED_DIR / "component_day_features.csv", index=False)

    # Compatibilidad con artefactos legacy
    legacy = out.rename(
        columns={
            "fecha": "fecha",
            "componente_id": "instancia_id",
            "estimated_health_index": "health_proxy",
            "degradation_velocity": "velocidad_degradacion_proxy",
            "inspection_defect_score_recent": "desgaste_detectado_pct",
            "alert_density": "alert_density_30d",
            "rolling_slope": "delta_vibr_14d",
        }
    )
    legacy.to_csv(DATA_PROCESSED_DIR / "features_componentes_diario.csv", index=False)

    return out


def _build_unit_day_features(component_day: pd.DataFrame) -> pd.DataFrame:
    unit_mart = pd.read_csv(DATA_PROCESSED_DIR / "mart_unit_day.csv")
    unidades = pd.read_csv(DATA_RAW_DIR / "unidades.csv")

    unit_mart["fecha"] = pd.to_datetime(unit_mart["fecha"], errors="coerce")
    component_day["fecha"] = pd.to_datetime(component_day["fecha"], errors="coerce")

    comp_agg = (
        component_day.groupby(["fecha", "unidad_id"], as_index=False)
        .agg(
            aggregated_health_score=("estimated_health_index", "mean"),
            risk_components=("estimated_health_index", lambda s: int((s < 55).sum())),
            component_alert_density=("alert_density", "mean"),
            avg_component_criticality=("criticidad_componente", "mean"),
        )
    )

    df = unit_mart.merge(comp_agg, on=["fecha", "unidad_id"], how="left")
    df = df.merge(unidades[["unidad_id", "criticidad_servicio", "configuracion_unidad"]], on="unidad_id", how="left")

    # Taxonomía oficial de backlog (físico vs riesgo de diferimiento).
    # Este mart solo usa backlog físico; el riesgo de diferimiento vive en priorización.
    if "backlog_physical_items" not in df.columns:
        df["backlog_physical_items"] = df.get("backlog_items", 0)
    if "backlog_physical_risk_accum" not in df.columns:
        df["backlog_physical_risk_accum"] = df.get("backlog_risk", 0)
    if "backlog_overdue_items" not in df.columns:
        df["backlog_overdue_items"] = 0.0
    if "backlog_critical_items" not in df.columns:
        df["backlog_critical_items"] = 0.0
    if "backlog_overdue_ratio" not in df.columns:
        df["backlog_overdue_ratio"] = _safe_div(
            df["backlog_overdue_items"].astype(float),
            df["backlog_physical_items"].astype(float),
            default=0.0,
        ).clip(0, 1)
    if "backlog_critical_ratio" not in df.columns:
        df["backlog_critical_ratio"] = _safe_div(
            df["backlog_critical_items"].astype(float),
            df["backlog_physical_items"].astype(float),
            default=0.0,
        ).clip(0, 1)
    if "backlog_exposure_adjusted_score" not in df.columns:
        df["backlog_exposure_adjusted_score"] = (
            (
                df["backlog_overdue_items"].fillna(0) * 0.75
                + df["backlog_critical_items"].fillna(0) * 1.20
                + df["backlog_physical_risk_accum"].fillna(0) / 90.0
            )
            / df["backlog_physical_items"].replace(0, np.nan).fillna(1)
            * 28.0
        ).clip(0, 100)

    # Compatibilidad legacy (mantener columnas históricas mientras migra consumo).
    df["backlog_items"] = df["backlog_physical_items"]
    df["backlog_risk"] = df["backlog_physical_risk_accum"]

    df["maintenance_load_proxy"] = (
        df["maintenance_hours"].fillna(0) * 0.65
        + df["backlog_physical_items"].fillna(0) * 0.30
        + df["backlog_overdue_items"].fillna(0) * 0.60
        + df["backlog_critical_items"].fillna(0) * 1.00
        + df["backlog_exposure_adjusted_score"].fillna(0) * 0.12
    ).clip(0, 100)

    df["service_exposure"] = (
        df["horas_planificadas"].fillna(0)
        * (1 + df["cancelaciones_proxy"].fillna(0) * 0.15)
        * (1 + df["criticidad_servicio"].fillna(3.5) / 10)
    ).clip(0, 80)

    df["substitution_difficulty"] = (
        (df["sustitucion_requerida_flag"].fillna(0) * 45)
        + (df["service_exposure"] / 2.2)
        + np.where(df["configuracion_unidad"].fillna("").str.contains("6_coches|5_coches"), 16, 8)
    ).clip(0, 100)

    df = df.sort_values(["unidad_id", "fecha"]).reset_index(drop=True)
    df["hours_lost_recent"] = (
        df.groupby("unidad_id")["horas_no_disponibles"]
        .rolling(window=30, min_periods=1)
        .sum()
        .reset_index(level=0, drop=True)
    )

    hours_scale = max(float(df["hours_lost_recent"].quantile(0.95)), 1.0)
    cancel_scale = max(float(df["cancelaciones_proxy"].fillna(0).quantile(0.95)), 1.0)
    punctuality_scale = max(float(df["puntualidad_impactada_proxy"].fillna(0).quantile(0.95)), 1.0)

    df["hours_lost_norm"] = (df["hours_lost_recent"] / hours_scale).clip(0, 1.4)
    df["cancelaciones_norm"] = (df["cancelaciones_proxy"].fillna(0) / cancel_scale).clip(0, 1.4)
    df["puntualidad_norm"] = (df["puntualidad_impactada_proxy"].fillna(0) / punctuality_scale).clip(0, 1.4)

    df["impact_on_service_proxy"] = (
        (df["hours_lost_norm"] * 0.52 + df["cancelaciones_norm"] * 0.28 + df["puntualidad_norm"] * 0.20) * 100
    ).clip(0, 100)

    flota_size = df.groupby("flota_id")["unidad_id"].nunique().rename("fleet_size")
    line_unit_counts = df.groupby(["fecha", "linea_servicio"])["unidad_id"].nunique().rename("line_units")
    df = df.merge(flota_size, on="flota_id", how="left")
    df = df.merge(line_unit_counts, on=["fecha", "linea_servicio"], how="left")

    df["fleet_dependency_flag"] = (
        (df["fleet_size"].fillna(0) <= 22)
        | (df["line_units"].fillna(0) <= 5)
        | (df["service_exposure"] >= df["service_exposure"].quantile(0.85))
    ).astype(int)

    out = df[
        [
            "fecha",
            "unidad_id",
            "flota_id",
            "linea_servicio",
            "critical_components_at_risk",
            "aggregated_health_score",
            "predicted_unavailability_risk",
            "maintenance_load_proxy",
            "service_exposure",
            "substitution_difficulty",
            "hours_lost_recent",
            "impact_on_service_proxy",
            "fleet_dependency_flag",
            "backlog_physical_items",
            "backlog_physical_risk_accum",
            "backlog_overdue_items",
            "backlog_critical_items",
            "backlog_overdue_ratio",
            "backlog_critical_ratio",
            "backlog_exposure_adjusted_score",
            # Compatibilidad legacy
            "backlog_items",
            "backlog_risk",
            "operating_stress_index",
            "component_alert_density",
            "avg_component_criticality",
        ]
    ].copy()

    out.to_csv(DATA_PROCESSED_DIR / "unit_day_features.csv", index=False)
    return out


def _build_fleet_week_features() -> pd.DataFrame:
    fleet = pd.read_csv(DATA_PROCESSED_DIR / "mart_fleet_week.csv")
    depot = pd.read_csv(DATA_PROCESSED_DIR / "vw_depot_maintenance_pressure.csv")
    unidades = pd.read_csv(DATA_RAW_DIR / "unidades.csv")

    fleet["week_start"] = pd.to_datetime(fleet["week_start"], errors="coerce")
    depot["fecha"] = pd.to_datetime(depot["fecha"], errors="coerce")

    unidades_depot = unidades[["unidad_id", "flota_id", "deposito_id"]].drop_duplicates()

    # presión de capacidad por depósito-flota en semana
    depot_week = depot.assign(week_start=depot["fecha"] - pd.to_timedelta(depot["fecha"].dt.dayofweek, unit="D"))
    depot_week = depot_week.merge(unidades_depot[["flota_id", "deposito_id"]].drop_duplicates(), on="deposito_id", how="left")

    cap = (
        depot_week.groupby(["week_start", "flota_id"], as_index=False)
        .agg(capacity_pressure_by_depot=("saturation_ratio", "mean"))
    )

    out = fleet.merge(cap, on=["week_start", "flota_id"], how="left")
    if "backlog_exposure_adjusted_score" not in out.columns:
        out["backlog_exposure_adjusted_score"] = (out.get("backlog_risk", 0).fillna(0) / 2.2).clip(0, 100)
    if "backlog_critical_items" not in out.columns:
        out["backlog_critical_items"] = 0.0

    out["capacity_pressure_by_depot"] = out["capacity_pressure_by_depot"].fillna(out["backlog_exposure_adjusted_score"] / 100)
    out["backlog_pressure"] = (
        out["backlog_exposure_adjusted_score"].fillna(0) * 0.75
        + out["backlog_critical_items"].fillna(0).clip(0, 300) * 0.25
    ).clip(0, 100)

    out = out[
        [
            "week_start",
            "flota_id",
            "availability_rate",
            "mtbf_proxy",
            "mttr_proxy",
            "backlog_pressure",
            "backlog_critical_items",
            "backlog_exposure_adjusted_score",
            "corrective_share",
            "cbm_share",
            "repetitive_failure_intensity",
            "capacity_pressure_by_depot",
            "avg_unavailability_risk",
            "maintenance_hours",
            "downtime_h",
        ]
    ]
    out.to_csv(DATA_PROCESSED_DIR / "fleet_week_features.csv", index=False)
    return out


def _build_workshop_priority_features(component_day: pd.DataFrame, unit_day: pd.DataFrame) -> pd.DataFrame:
    depositos = pd.read_csv(DATA_RAW_DIR / "depositos.csv")
    componentes = pd.read_csv(DATA_RAW_DIR / "componentes_criticos.csv")
    intervenciones = pd.read_csv(DATA_RAW_DIR / "intervenciones_programadas.csv")
    depot_pressure = pd.read_csv(DATA_PROCESSED_DIR / "vw_depot_maintenance_pressure.csv")

    latest_day = pd.to_datetime(component_day["fecha"]).max()
    comp_latest = component_day[pd.to_datetime(component_day["fecha"]) == latest_day].copy()
    unit_latest = unit_day[pd.to_datetime(unit_day["fecha"]) == latest_day].copy()

    unit_latest_cols = unit_latest[
        [
            "unidad_id",
            "predicted_unavailability_risk",
            "impact_on_service_proxy",
            "service_exposure",
            "substitution_difficulty",
            "fleet_dependency_flag",
            "backlog_physical_items",
            "backlog_overdue_items",
            "backlog_critical_items",
            "backlog_exposure_adjusted_score",
            # Compatibilidad legacy
            "backlog_items",
            "backlog_risk",
        ]
    ]

    df = comp_latest.merge(unit_latest_cols, on="unidad_id", how="left")
    df = df.merge(componentes[["componente_id", "criticidad_componente", "subsistema", "tipo_componente"]], on=["componente_id", "subsistema", "tipo_componente"], how="left")
    if "criticidad_componente" not in df.columns:
        left_col = "criticidad_componente_x"
        right_col = "criticidad_componente_y"
        if left_col in df.columns and right_col in df.columns:
            df["criticidad_componente"] = df[left_col].fillna(df[right_col])
        elif left_col in df.columns:
            df["criticidad_componente"] = df[left_col]
        elif right_col in df.columns:
            df["criticidad_componente"] = df[right_col]

    depot_latest = depot_pressure[pd.to_datetime(depot_pressure["fecha"]) == pd.to_datetime(depot_pressure["fecha"]).max()].copy()
    keep_cols = [
        "deposito_id",
        "saturation_ratio",
        "backlog_physical_items",
        "backlog_overdue_items",
        "backlog_critical_items",
        "backlog_exposure_adjusted_score",
        "backlog_risk",
        "corrective_share",
        "programmed_share",
    ]
    for col in keep_cols:
        if col not in depot_latest.columns:
            depot_latest[col] = 0.0
    depot_latest = depot_latest[keep_cols]

    intervenciones["fecha_programada"] = pd.to_datetime(intervenciones["fecha_programada"], errors="coerce")
    next_window = (
        intervenciones[intervenciones["fecha_programada"] >= latest_day]
        .sort_values("fecha_programada")
        .groupby(["unidad_id", "componente_id"], as_index=False)
        .first()[["unidad_id", "componente_id", "deposito_id", "fecha_programada", "prioridad_planificada", "ventana_operativa_disponible"]]
    )

    if next_window.empty:
        unidades = pd.read_csv(DATA_RAW_DIR / "unidades.csv")
        next_window = df[["unidad_id", "componente_id"]].merge(unidades[["unidad_id", "deposito_id"]], on="unidad_id", how="left")
        next_window["fecha_programada"] = latest_day + pd.Timedelta(days=14)
        next_window["prioridad_planificada"] = "media"
        next_window["ventana_operativa_disponible"] = "media"

    df = df.merge(next_window, on=["unidad_id", "componente_id"], how="left")
    unidades_ref = pd.read_csv(DATA_RAW_DIR / "unidades.csv")[["unidad_id", "deposito_id"]].drop_duplicates()
    df = df.merge(unidades_ref.rename(columns={"deposito_id": "deposito_unidad"}), on="unidad_id", how="left")
    df["deposito_id"] = df["deposito_id"].fillna(df["deposito_unidad"])
    df = df.drop(columns=["deposito_unidad"])
    df = df.merge(depot_latest, on="deposito_id", how="left", suffixes=("", "_depot"))

    # Ajuste por especialización técnica del depósito
    deposito_specs = depositos.set_index("deposito_id")["especializacion_tecnica"].to_dict()

    def _spec_match(row: pd.Series) -> float:
        spec = str(deposito_specs.get(row["deposito_id"], "")).lower()
        target = f"{row['subsistema']}_{row['tipo_componente']}".lower()
        if row["subsistema"].lower() in spec or row["tipo_componente"].lower() in spec:
            return 1.0
        if "bogie" in spec and row["subsistema"].lower() in {"wheelset", "bogie", "brake"}:
            return 0.75
        return 0.35

    df["specialization_match"] = df.apply(_spec_match, axis=1)

    df["urgency_inputs"] = (
        df["deterioration_index"].fillna(40) * 0.45
        + df["degradation_velocity"].fillna(0) * 7.5
        + df["critical_alerts_count"].fillna(0) * 3.8
        + df["shock_event_count"].fillna(0) * 1.2
        - df["maintenance_restoration_index"].fillna(0) * 0.10
    ).clip(0, 100)

    df["technical_risk_inputs"] = (
        df["urgency_inputs"] * 0.55
        + df["criticidad_componente"].fillna(3.5) * 8.0
        + df["repetitive_failure_flag"].fillna(0) * 12
    ).clip(0, 100)

    df["service_impact_inputs"] = (
        df["impact_on_service_proxy"].fillna(0) * 0.45
        + df["service_exposure"].fillna(0) * 0.38
        + df["substitution_difficulty"].fillna(0) * 0.17
    ).clip(0, 100)

    df["workshop_efficiency_inputs"] = (
        df["specialization_match"] * 55
        + (1 - df["saturation_ratio"].fillna(0.75).clip(0, 1.4) / 1.4) * 30
        + (1 - df["backlog_exposure_adjusted_score_depot"].fillna(35).clip(0, 100) / 100) * 15
    ).clip(0, 100)

    df["deferral_risk_inputs"] = (
        df["technical_risk_inputs"] * 0.4
        + df["service_impact_inputs"] * 0.3
        + (df["predicted_unavailability_risk"].fillna(0) * 100) * 0.2
        + df["backlog_exposure_adjusted_score"].fillna(0).clip(0, 100) * 0.1
    ).clip(0, 100)

    out = df[
        [
            "fecha",
            "unidad_id",
            "componente_id",
            "deposito_id",
            "fecha_programada",
            "prioridad_planificada",
            "ventana_operativa_disponible",
            "urgency_inputs",
            "service_impact_inputs",
            "technical_risk_inputs",
            "workshop_efficiency_inputs",
            "deferral_risk_inputs",
            "backlog_physical_items",
            "backlog_overdue_items",
            "backlog_critical_items",
            "backlog_exposure_adjusted_score",
            "predicted_unavailability_risk",
            "estimated_health_index",
            "deterioration_index",
            "degradation_velocity",
            "maintenance_restoration_index",
            "criticidad_componente",
            "subsistema",
            "tipo_componente",
            "linea_servicio",
        ]
    ].copy()

    out.to_csv(DATA_PROCESSED_DIR / "workshop_priority_features.csv", index=False)

    # Compatibilidad con artefactos legacy para tests/reporting existente
    legacy_instancia = out.rename(
        columns={
            "componente_id": "instancia_id",
            "urgency_inputs": "riesgo_ajustado_negocio",
            "service_impact_inputs": "impacto_operativo_score",
            "technical_risk_inputs": "criticidad_operativa",
        }
    )
    legacy_instancia.to_csv(DATA_PROCESSED_DIR / "features_instancia.csv", index=False)

    return out


def _write_feature_dictionary() -> None:
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    lines = [
        "# Diccionario de Features",
        "",
        "## component_day_features",
        "- `sensor_mean`, `sensor_std`, `sensor_max`: señales observadas de condición por componente-día.",
        "- `rolling_mean_7d`, `rolling_std_7d`, `rolling_slope`: comportamiento reciente y tendencia de degradación.",
        "- `shock_event_count`: recuento 30d de picos anómalos de señal.",
        "- `anomaly_count_30d`: intensidad de episodios fuera de patrón en 30 días.",
        "- `inspection_defect_score_recent`, `defect_confidence_recent`: severidad/confianza reciente de inspección automática.",
        "- `days_since_last_maintenance`, `days_since_last_failure`: tiempo desde intervención/falla previa.",
        "- `maintenance_frequency_180d`: densidad de mantenimiento en 180 días.",
        "- `repetitive_failure_flag`: indica historial de repetición de fallas.",
        "- `age_ratio`, `cycles_ratio`: consumo relativo de vida útil por edad/ciclos.",
        "- `deterioration_input_index`: deterioro estructural base del mart SQL (0-100, mayor es peor).",
        "- `estimated_health_index`: índice de salud interpretable (0-100, mayor es mejor).",
        "- `deterioration_index`: deterioro derivado de salud (0-100, mayor es peor).",
        "- `degradation_velocity`: velocidad proxy de degradación acumulada.",
        "- `maintenance_restoration_index`: efecto restaurativo tras mantenimiento reciente (0-100, mayor es mejor).",
        "- `operating_stress_index`, `environment_stress_proxy`: estrés de operación y contexto externo.",
        "- `alert_density`: concentración de alertas por componente.",
        "- `backlog_exposure_flag`: exposición a backlog crítico en componente.",
        "",
        "## unit_day_features",
        "- `critical_components_at_risk`: número de componentes críticos comprometidos en unidad.",
        "- `aggregated_health_score`: salud agregada de componentes de la unidad.",
        "- `predicted_unavailability_risk`: riesgo proxy de indisponibilidad operacional.",
        "- `maintenance_load_proxy`: presión combinada de mantenimiento y backlog.",
        "- `service_exposure`: exposición operacional por carga y criticidad de servicio.",
        "- `substitution_difficulty`: dificultad estimada de sustitución de material.",
        "- `hours_lost_recent`: horas perdidas en ventana reciente.",
        "- `impact_on_service_proxy`: impacto en cancelaciones/puntualidad.",
        "- `fleet_dependency_flag`: unidad con dependencia alta para continuidad de servicio.",
        "",
        "## fleet_week_features",
        "- `availability_rate`, `mtbf_proxy`, `mttr_proxy`: fiabilidad/disponibilidad semanal por flota.",
        "- `backlog_pressure`: presión de backlog agregada.",
        "- `corrective_share`, `cbm_share`: mix de estrategia de intervención.",
        "- `repetitive_failure_intensity`: intensidad de fallas repetitivas.",
        "- `capacity_pressure_by_depot`: presión media de capacidad de depósitos que atienden la flota.",
        "",
        "## workshop_priority_features",
        "- `urgency_inputs`: urgencia técnica inmediata (salud, degradación y alertas).",
        "- `service_impact_inputs`: impacto en servicio esperado por no intervenir.",
        "- `technical_risk_inputs`: riesgo técnico consolidado.",
        "- `workshop_efficiency_inputs`: ajuste de eficiencia según especialización/carga del depósito.",
        "- `deferral_risk_inputs`: riesgo agregado de diferimiento para decisión táctica.",
        "",
        "## Utilidad para CAF / entorno industrial ferroviario",
        "Estas señales permiten pasar de una lógica de OTs aisladas a un esquema de priorización defendible en operación,",
        "alineando salud de activo, disponibilidad de flota y saturación de taller con decisiones diarias de entrada a depósito.",
    ]

    (DOCS_DIR / "feature_dictionary.md").write_text("\n".join(lines), encoding="utf-8")


def build_feature_tables() -> FeatureOutputs:
    component_day = _build_component_day_features()
    unit_day = _build_unit_day_features(component_day)
    fleet_week = _build_fleet_week_features()
    workshop_priority = _build_workshop_priority_features(component_day, unit_day)

    _write_feature_dictionary()

    return FeatureOutputs(
        component_day_features=component_day,
        unit_day_features=unit_day,
        fleet_week_features=fleet_week,
        workshop_priority_features=workshop_priority,
    )


if __name__ == "__main__":
    build_feature_tables()
