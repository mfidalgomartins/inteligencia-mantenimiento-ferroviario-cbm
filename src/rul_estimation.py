from __future__ import annotations

from dataclasses import dataclass

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.config import DATA_PROCESSED_DIR, DATA_RAW_DIR, DOCS_DIR, OUTPUTS_CHARTS_DIR, OUTPUTS_REPORTS_DIR

matplotlib.use("Agg")


RUL_FAILURE_THRESHOLD_LEGACY = 30.0


@dataclass(frozen=True)
class FamilyProfile:
    failure_health_threshold: float
    base_daily_damage: float
    min_rul_days: int
    max_rul_days: int
    note: str


FAMILY_PROFILES: dict[str, FamilyProfile] = {
    "wheel": FamilyProfile(
        failure_health_threshold=38.0,
        base_daily_damage=0.20,
        min_rul_days=4,
        max_rul_days=260,
        note="desgaste progresivo, sensible a carga y perfil de rueda",
    ),
    "brake": FamilyProfile(
        failure_health_threshold=36.0,
        base_daily_damage=0.24,
        min_rul_days=3,
        max_rul_days=220,
        note="degradación más rápida bajo ciclos térmicos/frenado",
    ),
    "bogie": FamilyProfile(
        failure_health_threshold=34.0,
        base_daily_damage=0.16,
        min_rul_days=6,
        max_rul_days=320,
        note="tendencia más lenta, sensible a fatiga estructural",
    ),
    "pantograph": FamilyProfile(
        failure_health_threshold=37.0,
        base_daily_damage=0.27,
        min_rul_days=3,
        max_rul_days=200,
        note="sensibilidad alta a estrés operativo/contacto",
    ),
}


def _clip(value: float, low: float, high: float) -> float:
    return float(np.clip(value, low, high))


def _safe_float(value: float | int | None, default: float) -> float:
    if value is None:
        return default
    if isinstance(value, (float, int)):
        if np.isfinite(value):
            return float(value)
        return default
    try:
        parsed = float(value)
    except Exception:
        return default
    return parsed if np.isfinite(parsed) else default


def _component_family(row: pd.Series) -> str:
    txt = f"{row.get('sistema_principal','')} {row.get('subsistema','')} {row.get('tipo_componente','')}".lower()
    if "wheel" in txt or "rodadura" in txt:
        return "wheel"
    if "brake" in txt or "fren" in txt:
        return "brake"
    if "pant" in txt or "capt" in txt:
        return "pantograph"
    return "bogie"


def _r2_score(y: np.ndarray, y_hat: np.ndarray) -> float:
    ss_res = float(np.sum((y - y_hat) ** 2))
    ss_tot = float(np.sum((y - np.mean(y)) ** 2))
    if ss_tot <= 1e-12:
        return 0.0
    return max(0.0, 1.0 - ss_res / ss_tot)


def _linear_slope(y: np.ndarray) -> float:
    if len(y) < 2:
        return -0.08
    x = np.arange(len(y), dtype=float)
    slope, _ = np.polyfit(x, y, 1)
    return float(slope)


def _legacy_rul_projection(grp: pd.DataFrame) -> dict[str, float]:
    y = grp["estimated_health_index"].astype(float).to_numpy()
    y = np.where(np.isfinite(y), y, np.nan)
    if np.isnan(y).all():
        y = np.full(len(grp), 65.0, dtype=float)
    else:
        y = np.where(np.isnan(y), float(np.nanmedian(y)), y)

    x = np.arange(len(grp), dtype=float)
    if len(grp) < 12:
        slope = -0.08
        r2 = 0.2
    else:
        slope, intercept = np.polyfit(x, y, 1)
        y_hat = slope * x + intercept
        r2 = _r2_score(y, y_hat)
    health_current = float(y[-1]) if len(y) else 65.0
    if slope >= -0.02:
        rul_days = 365
    else:
        rul_days = int(np.clip((health_current - RUL_FAILURE_THRESHOLD_LEGACY) / abs(slope), 1, 365))
    confidence = float(np.clip(0.35 + 0.5 * r2 + min(len(grp), 60) / 300, 0.30, 0.98))
    return {
        "legacy_rul_days": int(rul_days),
        "legacy_slope_daily": float(slope),
        "legacy_confidence_rul": confidence,
    }


def _rul_bucket(days: float) -> str:
    if days <= 14:
        return "00_<=14"
    if days <= 30:
        return "01_15_30"
    if days <= 60:
        return "02_31_60"
    if days <= 90:
        return "03_61_90"
    if days <= 180:
        return "04_91_180"
    return "05_>180"


def _new_rul_projection(grp: pd.DataFrame, latest_row: pd.Series, prob_fallo_30d: float | None) -> dict[str, float | str]:
    family = _component_family(latest_row)
    profile = FAMILY_PROFILES[family]

    y = grp["estimated_health_index"].astype(float).to_numpy()
    y = np.where(np.isfinite(y), y, np.nan)
    if np.isnan(y).all():
        y = np.full(len(grp), 55.0, dtype=float)
    else:
        y = np.where(np.isnan(y), float(np.nanmedian(y)), y)

    slope_60d = _linear_slope(y)
    slope_30d = _linear_slope(y[-30:] if len(y) >= 30 else y)
    slope_accel = max(0.0, -slope_60d)
    trend_consistency = _clip(1.0 - abs(slope_30d - slope_60d) / (abs(slope_60d) + 0.03), 0.0, 1.0)

    health_current = _safe_float(latest_row.get("estimated_health_index"), float(y[-1]))
    deterioration = _safe_float(latest_row.get("deterioration_index"), 100.0 - health_current)
    degradation_velocity = _safe_float(latest_row.get("degradation_velocity"), 3.5)
    operating_stress = _safe_float(latest_row.get("operating_stress_index"), 0.85)
    environment_stress = _safe_float(latest_row.get("environment_stress_proxy"), 1.8)
    maintenance_restoration = _safe_float(latest_row.get("maintenance_restoration_index"), 22.0)
    maintenance_frequency = _safe_float(latest_row.get("maintenance_frequency_180d"), 4.0)
    repetitive_failure = int(_safe_float(latest_row.get("repetitive_failure_flag"), 0.0) >= 1.0)
    critical_alerts = _safe_float(latest_row.get("critical_alerts_count"), 0.0)
    age_ratio = _safe_float(latest_row.get("age_ratio"), 0.75)
    cycles_ratio = _safe_float(latest_row.get("cycles_ratio"), 0.25)
    days_since_maint = latest_row.get("days_since_last_maintenance")
    days_since_fail = latest_row.get("days_since_last_failure")

    # Señal de riesgo interpretable: mezcla proxy técnico + probabilidad de fallo si está disponible.
    risk_proxy = (
        0.52 * _clip(deterioration / 100.0, 0.0, 1.0)
        + 0.28 * _clip(degradation_velocity / 10.0, 0.0, 1.0)
        + 0.20 * _clip(critical_alerts / 3.0, 0.0, 1.0)
    )
    if prob_fallo_30d is None or not np.isfinite(prob_fallo_30d):
        risk_signal = _clip(risk_proxy, 0.02, 0.98)
    else:
        risk_signal = _clip(0.52 * float(prob_fallo_30d) + 0.48 * risk_proxy, 0.02, 0.98)

    deterioration_term = 0.92 + 0.55 * (deterioration / 100.0) ** 1.40
    velocity_term = 0.90 + 0.60 * (degradation_velocity / 10.0) ** 1.20
    stress_term = (
        0.92
        + 0.25 * _clip((operating_stress - 0.75) / 0.25, 0.0, 1.2)
        + 0.10 * _clip(environment_stress / 3.0, 0.0, 1.4)
    )
    life_term = 0.94 + 0.30 * _clip(max(age_ratio, cycles_ratio), 0.0, 2.2)
    alert_term = 1.0 + 0.04 * _clip(critical_alerts, 0.0, 4.0)
    repeat_term = 1.0 + (0.16 if repetitive_failure == 1 else 0.0)
    risk_term = 0.94 + 0.35 * risk_signal
    trend_term = 1.0 + 0.20 * _clip(slope_accel / 0.03, 0.0, 1.4)

    restoration_multiplier = 1.0 - 0.28 * _clip(maintenance_restoration / 100.0, 0.0, 1.0) ** 1.1
    if pd.notna(days_since_maint) and _safe_float(days_since_maint, 9999.0) <= 21:
        restoration_multiplier *= 0.90
    restoration_multiplier *= 1.0 - 0.05 * _clip(maintenance_frequency / 8.0, 0.0, 1.0)
    restoration_multiplier = _clip(restoration_multiplier, 0.58, 1.05)

    effective_daily_damage = (
        profile.base_daily_damage
        * deterioration_term
        * velocity_term
        * stress_term
        * life_term
        * alert_term
        * repeat_term
        * risk_term
        * trend_term
        * restoration_multiplier
    )
    effective_daily_damage = _clip(effective_daily_damage, 0.05, 12.0)

    health_buffer = max(0.8, health_current - profile.failure_health_threshold)
    nonlinear_accel = (
        1.0
        + 0.22 * _clip((deterioration - 60.0) / 40.0, 0.0, 1.2)
        + 0.18 * _clip((risk_signal - 0.60) / 0.40, 0.0, 1.0)
    )
    rul_days = health_buffer / (effective_daily_damage * nonlinear_accel)

    # Penalización por fallas repetitivas y reincidencia reciente.
    if repetitive_failure == 1:
        rul_days *= 0.92
    if pd.notna(days_since_fail) and _safe_float(days_since_fail, 9999.0) <= 45:
        rul_days *= 0.88

    # Extensión limitada tras restauración clara y señal de riesgo baja.
    if maintenance_restoration >= 70 and risk_signal <= 0.35:
        rul_days *= 1.10

    rul_days = _clip(rul_days, float(profile.min_rul_days), float(profile.max_rul_days))

    # Confianza del RUL: calidad de señal, estabilidad de tendencia y cobertura temporal.
    fields = [
        "estimated_health_index",
        "deterioration_index",
        "degradation_velocity",
        "operating_stress_index",
        "environment_stress_proxy",
        "maintenance_restoration_index",
        "maintenance_frequency_180d",
        "critical_alerts_count",
        "age_ratio",
        "cycles_ratio",
    ]
    signal_completeness = float(latest_row[fields].notna().mean()) if len(fields) > 0 else 0.6
    data_span_factor = _clip(len(grp) / 60.0, 0.2, 1.0)
    slope_strength = _clip(abs(slope_60d) / 0.03, 0.0, 1.0)
    confidence = (
        0.24
        + 0.24 * signal_completeness
        + 0.20 * trend_consistency
        + 0.12 * data_span_factor
        + 0.10 * slope_strength
        + 0.10 * (1.0 - _clip(risk_signal, 0.0, 1.0))
    )
    if rul_days >= profile.max_rul_days * 0.85:
        confidence *= 0.88
    if repetitive_failure == 1 or critical_alerts >= 2:
        confidence *= 0.94
    confidence = _clip(confidence, 0.25, 0.95)

    if confidence >= 0.78:
        conf_flag = "alta"
    elif confidence >= 0.58:
        conf_flag = "media"
    else:
        conf_flag = "baja"

    return {
        "component_family": family,
        "rul_health_threshold": float(profile.failure_health_threshold),
        "effective_daily_damage": float(effective_daily_damage),
        "risk_signal_for_rul": float(risk_signal),
        "degradation_slope_daily_60d": float(slope_60d),
        "degradation_slope_daily_30d": float(slope_30d),
        "health_current": float(health_current),
        "component_rul_estimate": int(round(rul_days)),
        "confidence_rul": float(confidence),
        "confidence_flag": conf_flag,
        "rul_window_bucket": _rul_bucket(rul_days),
    }


def _estimate_snapshot(
    *,
    daily: pd.DataFrame,
    cutoff: pd.Timestamp,
    scoring_map: dict[str, float] | None,
) -> pd.DataFrame:
    win = daily[(daily["fecha"] <= cutoff) & (daily["fecha"] >= cutoff - pd.Timedelta(days=59))].copy()
    win = win.sort_values(["componente_id", "fecha"]).reset_index(drop=True)

    rows: list[dict[str, float | str | int]] = []
    for comp_id, grp in win.groupby("componente_id", sort=False):
        latest_row = grp.iloc[-1]
        legacy = _legacy_rul_projection(grp)
        prob = scoring_map.get(comp_id) if scoring_map is not None else None
        new = _new_rul_projection(grp, latest_row, prob)

        rows.append(
            {
                "fecha_corte": cutoff.date().isoformat(),
                "unidad_id": str(latest_row["unidad_id"]),
                "componente_id": str(comp_id),
                "sistema_principal": str(latest_row["sistema_principal"]),
                "subsistema": str(latest_row["subsistema"]),
                "tipo_componente": str(latest_row["tipo_componente"]),
                "legacy_rul_days": int(legacy["legacy_rul_days"]),
                "legacy_slope_daily": float(legacy["legacy_slope_daily"]),
                "legacy_confidence_rul": float(legacy["legacy_confidence_rul"]),
                **new,
            }
        )
    return pd.DataFrame(rows)


def _save_chart(fig: plt.Figure, filename: str) -> None:
    OUTPUTS_CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(OUTPUTS_CHARTS_DIR / filename, dpi=140)
    plt.close(fig)


def _build_backtest_failure_linkage(daily: pd.DataFrame, fallas: pd.DataFrame, latest: pd.Timestamp) -> pd.DataFrame:
    min_date = daily["fecha"].min()
    eval_end = latest - pd.Timedelta(days=30)
    eval_start = max(min_date + pd.Timedelta(days=180), eval_end - pd.Timedelta(days=330))
    cutoffs = pd.date_range(start=eval_start.normalize(), end=eval_end.normalize(), freq="30D")
    if len(cutoffs) < 6:
        cutoffs = pd.date_range(end=eval_end.normalize(), periods=6, freq="30D")

    failure_map = {
        str(comp): grp["fecha_falla"].dropna().sort_values().to_numpy(dtype="datetime64[ns]")
        for comp, grp in fallas.groupby("componente_id", sort=False)
    }

    rows: list[dict[str, float | str | int]] = []
    for cutoff in cutoffs:
        snap = _estimate_snapshot(daily=daily, cutoff=cutoff, scoring_map=None)
        cutoff_np = np.datetime64(cutoff.to_pydatetime())
        horizon_np = np.datetime64((cutoff + pd.Timedelta(days=30)).to_pydatetime())
        for row in snap.itertuples(index=False):
            failures = failure_map.get(str(row.componente_id), np.array([], dtype="datetime64[ns]"))
            if failures.size == 0:
                in_30d = 0
            else:
                in_30d = int(np.any((failures > cutoff_np) & (failures <= horizon_np)))
            rows.append(
                {
                    "fecha_corte": row.fecha_corte,
                    "componente_id": row.componente_id,
                    "component_family": row.component_family,
                    "legacy_rul_days": int(row.legacy_rul_days),
                    "new_rul_days": int(row.component_rul_estimate),
                    "failure_in_30d": in_30d,
                }
            )
    out = pd.DataFrame(rows)
    if out.empty:
        return out

    out["legacy_bucket"] = out["legacy_rul_days"].map(_rul_bucket)
    out["new_bucket"] = out["new_rul_days"].map(_rul_bucket)
    return out


def _build_rul_validation_checks(
    *,
    latest_compare: pd.DataFrame,
    backtest_linkage: pd.DataFrame,
) -> pd.DataFrame:
    p10 = float(latest_compare["component_rul_estimate"].quantile(0.10))
    p90 = float(latest_compare["component_rul_estimate"].quantile(0.90))
    share_cap = float((latest_compare["component_rul_estimate"] >= latest_compare["component_rul_estimate"].max()).mean())
    family_medians = latest_compare.groupby("component_family")["component_rul_estimate"].median()
    family_disp = float(family_medians.max() - family_medians.min()) if not family_medians.empty else 0.0
    conf_entropy = float(-(latest_compare["confidence_flag"].value_counts(normalize=True) * np.log2(latest_compare["confidence_flag"].value_counts(normalize=True) + 1e-12)).sum())

    if backtest_linkage.empty:
        corr_new = np.nan
        low_vs_high_new = np.nan
    else:
        corr_new = float(backtest_linkage[["new_rul_days", "failure_in_30d"]].corr(method="spearman").iloc[0, 1])
        low_rate = float(backtest_linkage.loc[backtest_linkage["new_rul_days"] <= 30, "failure_in_30d"].mean())
        high_rate = float(backtest_linkage.loc[backtest_linkage["new_rul_days"] > 180, "failure_in_30d"].mean())
        low_vs_high_new = low_rate - high_rate

    checks = [
        {
            "check_id": "rul_distribution_not_saturated",
            "severity": "alta",
            "passed": bool(share_cap <= 0.60),
            "metric_value": share_cap,
            "threshold": "<=0.60",
            "detail": "share en techo de RUL",
        },
        {
            "check_id": "rul_distribution_spread",
            "severity": "alta",
            "passed": bool((p90 - p10) >= 55),
            "metric_value": float(p90 - p10),
            "threshold": ">=55 días",
            "detail": "amplitud P90-P10",
        },
        {
            "check_id": "rul_family_discrimination",
            "severity": "media",
            "passed": bool(family_disp >= 12),
            "metric_value": family_disp,
            "threshold": ">=12 días",
            "detail": "diferencia medianas entre familias",
        },
        {
            "check_id": "rul_confidence_entropy",
            "severity": "media",
            "passed": bool(conf_entropy >= 0.65),
            "metric_value": conf_entropy,
            "threshold": ">=0.65",
            "detail": "entropía de confidence_flag",
        },
        {
            "check_id": "rul_failure_linkage_direction",
            "severity": "alta",
            "passed": bool(np.isfinite(corr_new) and corr_new <= -0.08),
            "metric_value": corr_new,
            "threshold": "<=-0.08",
            "detail": "correlación RUL vs falla_30d (esperada negativa)",
        },
        {
            "check_id": "rul_failure_bucket_separation",
            "severity": "alta",
            "passed": bool(np.isfinite(low_vs_high_new) and low_vs_high_new >= 0.02),
            "metric_value": low_vs_high_new,
            "threshold": ">=0.02",
            "detail": "failure_rate(<=30d) - failure_rate(>180d)",
        },
    ]
    return pd.DataFrame(checks)


def _write_rul_framework_doc(
    *,
    latest_compare: pd.DataFrame,
    summary_before_after: pd.DataFrame,
    family_summary: pd.DataFrame,
    failure_linkage: pd.DataFrame,
    validation_checks: pd.DataFrame,
) -> None:
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    legacy_share_cap = float((latest_compare["legacy_rul_days"] >= 365).mean())
    new_share_cap = float((latest_compare["component_rul_estimate"] >= latest_compare["component_rul_estimate"].max()).mean())

    lines = [
        "# RUL Framework",
        "",
        "## 1) Diagnóstico de la lógica anterior",
        "- La lógica legacy usaba extrapolación lineal y regla de saturación `slope >= -0.02 => RUL=365`.",
        f"- Resultado observado: `share_rul_365_legacy = {legacy_share_cap:.3f}` (colapso en horizontes amplios).",
        "- Impacto: baja discriminación para priorización, intervención y scheduling.",
        "",
        "## 2) Definición operativa del nuevo RUL",
        "RUL (`component_rul_estimate`) = días estimados hasta cruzar umbral de condición crítica por familia técnica,",
        "bajo condiciones operativas actuales y degradación efectiva diaria.",
        "",
        "### Qué incorpora",
        "- Perfil por familia (`wheel`, `brake`, `bogie`, `pantograph`) con umbral y horizonte máximo específicos.",
        "- Degradación no lineal por deterioro, velocidad de degradación y aceleración de tendencia.",
        "- Estrés operacional y ambiental.",
        "- Resets parciales por restauración de mantenimiento (sin asumir reset perfecto).",
        "- Penalización por repetitividad de falla y alertas críticas.",
        "- Banda de confianza (`confidence_rul`, `confidence_flag`) por completitud y estabilidad de señal.",
        "",
        "## 3) Cuándo usar y cuándo no usar RUL",
        "### Usar RUL para:",
        "- Dimensionar ventana de intervención (urgente/corta/media/larga).",
        "- Desempatar prioridades entre activos con riesgo similar.",
        "- Ajustar secuencia de taller junto con impacto de servicio y fit de depósito.",
        "",
        "### No usar RUL como única señal cuando:",
        "- `confidence_flag = baja` o hay conflicto fuerte entre señales (ej. alto riesgo con RUL amplio).",
        "- Existen restricciones operativas duras (ventana/capacidad/repuesto) que dominan la decisión.",
        "- Se requiere causalidad física de fallo por componente (este marco es proxy interpretable, no modelo físico).",
        "",
        "## 4) Convivencia con health y risk",
        "- `health_score`: estado actual (alto=mejor).",
        "- `prob_fallo_30d`: probabilidad de fallo a corto plazo.",
        "- `component_rul_estimate`: horizonte temporal de agotamiento bajo condiciones actuales.",
        "- Regla práctica: decisión prioritaria cuando riesgo alto + RUL corto + impacto servicio alto.",
        "",
        "## 5) Before / After",
        summary_before_after.to_markdown(index=False),
        "",
        "### Discriminación por familia",
        family_summary.to_markdown(index=False),
        "",
        "### Relación con fallas posteriores (backtest)",
        failure_linkage.to_markdown(index=False),
        "",
        "## 6) Validaciones específicas de RUL",
        validation_checks.to_markdown(index=False),
        "",
        "## 7) Integración con recomendación y scheduling",
        "- `component_rul_estimate` y `confidence_rul` alimentan `assign_operational_decisions` y `workshop_priority_table`.",
        "- RUL corto empuja decisiones de `intervención inmediata` / `próxima ventana` según conflicto de capacidad.",
        "- RUL amplio con riesgo bajo permite `observación` / `no acción` con menor presión de taller.",
        "",
        "## 8) Limitaciones",
        "- Datos sintéticos: requiere recalibración con histórico real antes de despliegue operativo.",
        "- El módulo no sustituye modelos físicos de desgaste por fabricante.",
        "- `days_since_last_maintenance` puede ser escaso en el sintético; se compensa con índices de restauración/frecuencia.",
    ]
    (DOCS_DIR / "rul_framework.md").write_text("\n".join(lines), encoding="utf-8")


def estimate_rul() -> pd.DataFrame:
    daily = pd.read_csv(DATA_PROCESSED_DIR / "component_day_features.csv")
    scoring = pd.read_csv(DATA_PROCESSED_DIR / "scoring_componentes.csv")
    fallas = pd.read_csv(DATA_RAW_DIR / "fallas_historicas.csv")

    daily["fecha"] = pd.to_datetime(daily["fecha"], errors="coerce")
    fallas["fecha_falla"] = pd.to_datetime(fallas["fecha_falla"], errors="coerce")
    latest = pd.to_datetime(daily["fecha"].max())

    scoring_map = scoring.set_index("componente_id")["prob_fallo_30d"].to_dict()
    latest_compare = _estimate_snapshot(daily=daily, cutoff=latest, scoring_map=scoring_map)

    latest_compare = latest_compare.merge(
        scoring[["unidad_id", "componente_id", "prob_fallo_30d", "recommended_action_initial"]],
        on=["unidad_id", "componente_id"],
        how="left",
    )
    if "prob_fallo_30d_x" in latest_compare.columns and "prob_fallo_30d_y" in latest_compare.columns:
        latest_compare["prob_fallo_30d"] = latest_compare["prob_fallo_30d_y"].fillna(latest_compare["prob_fallo_30d_x"])
        latest_compare = latest_compare.drop(columns=["prob_fallo_30d_x", "prob_fallo_30d_y"])

    latest_compare["component_rul_estimate"] = latest_compare["component_rul_estimate"].clip(1, 365).astype(int)
    latest_compare["legacy_rul_days"] = latest_compare["legacy_rul_days"].clip(1, 365).astype(int)

    rul = latest_compare[
        [
            "fecha_corte",
            "unidad_id",
            "componente_id",
            "sistema_principal",
            "subsistema",
            "tipo_componente",
            "component_family",
            "component_rul_estimate",
            "rul_window_bucket",
            "rul_health_threshold",
            "effective_daily_damage",
            "risk_signal_for_rul",
            "degradation_slope_daily_60d",
            "degradation_slope_daily_30d",
            "health_current",
            "confidence_rul",
            "confidence_flag",
            "prob_fallo_30d",
            "recommended_action_initial",
        ]
    ].copy()
    rul["degradation_slope_daily"] = rul["degradation_slope_daily_60d"]

    # Artefacto principal para consumo downstream.
    DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUTS_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUTS_CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    rul.to_csv(DATA_PROCESSED_DIR / "component_rul_estimate.csv", index=False)
    rul.to_csv(OUTPUTS_REPORTS_DIR / "component_rul_estimate.csv", index=False)

    # Compatibilidad legacy
    legacy = rul.rename(
        columns={
            "componente_id": "instancia_id",
            "component_rul_estimate": "rul_dias",
            "degradation_slope_daily": "pendiente_degradacion_diaria",
            "confidence_rul": "confianza_rul",
        }
    )
    legacy.to_csv(DATA_PROCESSED_DIR / "rul_instancia.csv", index=False)
    legacy.to_csv(OUTPUTS_REPORTS_DIR / "rul_instancia.csv", index=False)

    # Before/after latest snapshot.
    compare_latest = latest_compare[
        [
            "fecha_corte",
            "unidad_id",
            "componente_id",
            "component_family",
            "legacy_rul_days",
            "component_rul_estimate",
            "legacy_confidence_rul",
            "confidence_rul",
            "legacy_slope_daily",
            "degradation_slope_daily_60d",
            "health_current",
            "effective_daily_damage",
            "rul_window_bucket",
            "confidence_flag",
        ]
    ].copy()
    compare_latest["delta_rul_new_minus_legacy"] = compare_latest["component_rul_estimate"] - compare_latest["legacy_rul_days"]
    compare_latest.to_csv(DATA_PROCESSED_DIR / "rul_before_after_comparison.csv", index=False)
    compare_latest.to_csv(OUTPUTS_REPORTS_DIR / "rul_before_after_comparison.csv", index=False)

    summary_before_after = pd.DataFrame(
        [
            {
                "metodo": "legacy_lineal_365",
                "mean_rul": float(compare_latest["legacy_rul_days"].mean()),
                "p10_rul": float(compare_latest["legacy_rul_days"].quantile(0.10)),
                "p50_rul": float(compare_latest["legacy_rul_days"].quantile(0.50)),
                "p90_rul": float(compare_latest["legacy_rul_days"].quantile(0.90)),
                "share_rul_cap": float((compare_latest["legacy_rul_days"] >= 365).mean()),
                "share_rul_<=30": float((compare_latest["legacy_rul_days"] <= 30).mean()),
            },
            {
                "metodo": "nuevo_proxy_familia",
                "mean_rul": float(compare_latest["component_rul_estimate"].mean()),
                "p10_rul": float(compare_latest["component_rul_estimate"].quantile(0.10)),
                "p50_rul": float(compare_latest["component_rul_estimate"].quantile(0.50)),
                "p90_rul": float(compare_latest["component_rul_estimate"].quantile(0.90)),
                "share_rul_cap": float((compare_latest["component_rul_estimate"] >= compare_latest["component_rul_estimate"].max()).mean()),
                "share_rul_<=30": float((compare_latest["component_rul_estimate"] <= 30).mean()),
            },
        ]
    )
    summary_before_after.to_csv(DATA_PROCESSED_DIR / "rul_distribution_before_after.csv", index=False)
    summary_before_after.to_csv(OUTPUTS_REPORTS_DIR / "rul_distribution_before_after.csv", index=False)

    family_summary = (
        compare_latest.groupby("component_family", as_index=False)
        .agg(
            legacy_p50=("legacy_rul_days", "median"),
            legacy_p10=("legacy_rul_days", lambda s: float(np.quantile(s, 0.10))),
            legacy_p90=("legacy_rul_days", lambda s: float(np.quantile(s, 0.90))),
            new_p50=("component_rul_estimate", "median"),
            new_p10=("component_rul_estimate", lambda s: float(np.quantile(s, 0.10))),
            new_p90=("component_rul_estimate", lambda s: float(np.quantile(s, 0.90))),
            new_share_le_30=("component_rul_estimate", lambda s: float((s <= 30).mean())),
        )
        .sort_values("new_p50")
        .reset_index(drop=True)
    )
    family_summary.to_csv(DATA_PROCESSED_DIR / "rul_family_discrimination_before_after.csv", index=False)
    family_summary.to_csv(OUTPUTS_REPORTS_DIR / "rul_family_discrimination_before_after.csv", index=False)

    # Backtest de relación con fallas posteriores.
    backtest = _build_backtest_failure_linkage(daily=daily, fallas=fallas, latest=latest)
    backtest.to_csv(DATA_PROCESSED_DIR / "rul_backtest_component_cutoff.csv", index=False)
    backtest.to_csv(OUTPUTS_REPORTS_DIR / "rul_backtest_component_cutoff.csv", index=False)

    if not backtest.empty:
        long = pd.concat(
            [
                backtest[["legacy_rul_days", "failure_in_30d", "legacy_bucket"]]
                .rename(columns={"legacy_rul_days": "rul_days", "legacy_bucket": "rul_bucket"})
                .assign(method="legacy_lineal_365"),
                backtest[["new_rul_days", "failure_in_30d", "new_bucket"]]
                .rename(columns={"new_rul_days": "rul_days", "new_bucket": "rul_bucket"})
                .assign(method="nuevo_proxy_familia"),
            ],
            ignore_index=True,
        )
        failure_linkage = (
            long.groupby(["method", "rul_bucket"], as_index=False)
            .agg(
                observations=("failure_in_30d", "size"),
                failures_30d=("failure_in_30d", "sum"),
                failure_rate_30d=("failure_in_30d", "mean"),
            )
            .sort_values(["method", "rul_bucket"])
            .reset_index(drop=True)
        )
        corr_table = (
            long.groupby("method", as_index=False)
            .apply(lambda g: pd.Series({"spearman_rul_vs_failure30d": float(g[["rul_days", "failure_in_30d"]].corr(method="spearman").iloc[0, 1])}))
            .reset_index(drop=True)
        )
    else:
        failure_linkage = pd.DataFrame(columns=["method", "rul_bucket", "observations", "failures_30d", "failure_rate_30d"])
        corr_table = pd.DataFrame(columns=["method", "spearman_rul_vs_failure30d"])

    failure_linkage.to_csv(DATA_PROCESSED_DIR / "rul_backtest_failure_linkage.csv", index=False)
    failure_linkage.to_csv(OUTPUTS_REPORTS_DIR / "rul_backtest_failure_linkage.csv", index=False)
    corr_table.to_csv(DATA_PROCESSED_DIR / "rul_backtest_correlation.csv", index=False)
    corr_table.to_csv(OUTPUTS_REPORTS_DIR / "rul_backtest_correlation.csv", index=False)

    # Validaciones específicas de RUL.
    rul_checks = _build_rul_validation_checks(latest_compare=compare_latest, backtest_linkage=backtest)
    rul_checks.to_csv(DATA_PROCESSED_DIR / "rul_validation_checks.csv", index=False)
    rul_checks.to_csv(OUTPUTS_REPORTS_DIR / "rul_validation_checks.csv", index=False)

    # Gráficos before/after.
    fig1, ax1 = plt.subplots(figsize=(9.5, 4.8))
    ax1.hist(compare_latest["legacy_rul_days"], bins=24, alpha=0.45, color="#8d99ae", label="Legacy lineal")
    ax1.hist(compare_latest["component_rul_estimate"], bins=24, alpha=0.65, color="#2a9d8f", label="Nuevo proxy familia")
    ax1.set_title("RUL Before/After | Distribución en snapshot operativo")
    ax1.set_xlabel("RUL (días)")
    ax1.set_ylabel("N.º componentes")
    ax1.legend()
    _save_chart(fig1, "16_rul_before_after_distribution.png")

    fig2, ax2 = plt.subplots(figsize=(9.5, 4.8))
    fam = family_summary.copy()
    x = np.arange(len(fam))
    ax2.bar(x - 0.18, fam["legacy_p50"], width=0.36, color="#adb5bd", label="Legacy p50")
    ax2.bar(x + 0.18, fam["new_p50"], width=0.36, color="#1d3557", label="Nuevo p50")
    ax2.set_xticks(x)
    ax2.set_xticklabels(fam["component_family"])
    ax2.set_title("RUL por familia | Mediana before/after")
    ax2.set_ylabel("RUL mediano (días)")
    ax2.legend()
    _save_chart(fig2, "17_rul_family_discrimination_before_after.png")

    fig3, ax3 = plt.subplots(figsize=(10, 5))
    if not failure_linkage.empty:
        for method, color in [("legacy_lineal_365", "#7f8c8d"), ("nuevo_proxy_familia", "#e76f51")]:
            tmp = failure_linkage[failure_linkage["method"] == method].copy()
            ax3.plot(tmp["rul_bucket"], tmp["failure_rate_30d"], marker="o", label=method, color=color)
    ax3.set_title("Backtest | Relación entre bucket RUL y falla en 30 días")
    ax3.set_xlabel("Bucket de RUL")
    ax3.set_ylabel("Failure rate 30d")
    ax3.set_ylim(bottom=0)
    ax3.legend()
    _save_chart(fig3, "18_rul_failure_linkage_before_after.png")

    fig4, ax4 = plt.subplots(figsize=(9.5, 4.8))
    util = pd.DataFrame(
        {
            "bucket": ["<=14", "15-30", "31-60", "61-90", "91-180", ">180"],
            "legacy_share": [
                float((compare_latest["legacy_rul_days"] <= 14).mean()),
                float(compare_latest["legacy_rul_days"].between(15, 30).mean()),
                float(compare_latest["legacy_rul_days"].between(31, 60).mean()),
                float(compare_latest["legacy_rul_days"].between(61, 90).mean()),
                float(compare_latest["legacy_rul_days"].between(91, 180).mean()),
                float((compare_latest["legacy_rul_days"] > 180).mean()),
            ],
            "new_share": [
                float((compare_latest["component_rul_estimate"] <= 14).mean()),
                float(compare_latest["component_rul_estimate"].between(15, 30).mean()),
                float(compare_latest["component_rul_estimate"].between(31, 60).mean()),
                float(compare_latest["component_rul_estimate"].between(61, 90).mean()),
                float(compare_latest["component_rul_estimate"].between(91, 180).mean()),
                float((compare_latest["component_rul_estimate"] > 180).mean()),
            ],
        }
    )
    x2 = np.arange(len(util))
    ax4.bar(x2 - 0.18, util["legacy_share"], width=0.36, color="#adb5bd", label="Legacy")
    ax4.bar(x2 + 0.18, util["new_share"], width=0.36, color="#2a9d8f", label="Nuevo")
    ax4.set_xticks(x2)
    ax4.set_xticklabels(util["bucket"])
    ax4.set_title("Utilidad operativa | Ventanas de intervención sugeridas por RUL")
    ax4.set_ylabel("Share de componentes")
    ax4.legend()
    _save_chart(fig4, "19_rul_window_utility_before_after.png")

    util.to_csv(DATA_PROCESSED_DIR / "rul_window_utility_before_after.csv", index=False)
    util.to_csv(OUTPUTS_REPORTS_DIR / "rul_window_utility_before_after.csv", index=False)

    _write_rul_framework_doc(
        latest_compare=compare_latest,
        summary_before_after=summary_before_after,
        family_summary=family_summary,
        failure_linkage=failure_linkage,
        validation_checks=rul_checks,
    )
    return rul


if __name__ == "__main__":
    estimate_rul()
