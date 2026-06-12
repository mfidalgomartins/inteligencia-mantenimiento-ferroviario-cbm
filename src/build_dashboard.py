from __future__ import annotations

import base64
import json
from hashlib import sha1

import numpy as np
import pandas as pd

from src.config import DATA_PROCESSED_DIR, DATA_RAW_DIR, OUTPUTS_DASHBOARD_DIR, ROOT_DIR
from src.reporting_governance import load_or_compute_narrative_metrics

DASHBOARD_SLUG = "centro-control-mantenimiento-ferroviario.html"


def _embedded_font_faces() -> str:
    fonts_dir = ROOT_DIR / "assets" / "fonts"
    specs = [
        ("sans.woff2", "IBM Plex Sans", "normal", "400 700"),
        ("mono500.woff2", "IBM Plex Mono", "normal", "500"),
        ("mono600.woff2", "IBM Plex Mono", "normal", "600"),
    ]
    blocks = []
    for file_name, family, style, weight in specs:
        data = (fonts_dir / file_name).read_bytes()
        b64 = base64.b64encode(data).decode("ascii")
        blocks.append(
            "@font-face{font-family:'%s';font-style:%s;font-weight:%s;font-display:swap;"
            "src:url(data:font/woff2;base64,%s) format('woff2');}" % (family, style, weight, b64)
        )
    return "\n    ".join(blocks)


def _risk_tier(score: float) -> str:
    if score >= 80:
        return "Critico"
    if score >= 65:
        return "Alto"
    if score >= 45:
        return "Medio"
    return "Bajo"


def _window_bucket(days: float) -> str:
    if days <= 2:
        return "0-2d"
    if days <= 7:
        return "3-7d"
    if days <= 14:
        return "8-14d"
    return "15-21d"


def _component_family(row: pd.Series) -> str:
    txt = f"{row.get('sistema_principal','')} {row.get('subsistema','')} {row.get('tipo_componente','')}".lower()
    if "wheel" in txt or "rodadura" in txt:
        return "wheel"
    if "brake" in txt or "fren" in txt:
        return "brake"
    if "pant" in txt or "capt" in txt:
        return "pantograph"
    return "bogie"


def _latest_frame(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    out = df.copy()
    out[date_col] = pd.to_datetime(out[date_col], errors="coerce")
    return out[out[date_col] == out[date_col].max()].copy()


def _coalesce_columns(df: pd.DataFrame, canonical: str, candidates: list[str]) -> pd.DataFrame:
    """Create canonical column by first non-null candidate and drop alternates."""
    available = [c for c in candidates if c in df.columns]
    if canonical in df.columns:
        available = [canonical] + [c for c in available if c != canonical]
    if not available:
        return df

    out = df.copy()
    out[canonical] = out[available[0]]
    for col in available[1:]:
        out[canonical] = out[canonical].where(out[canonical].notna(), out[col])
    drop_cols = [c for c in available if c != canonical]
    if drop_cols:
        out = out.drop(columns=drop_cols)
    return out


def build_dashboard() -> str:
    OUTPUTS_DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)

    flotas = pd.read_csv(DATA_RAW_DIR / "flotas.csv")
    unidades = pd.read_csv(DATA_RAW_DIR / "unidades.csv")
    depositos = pd.read_csv(DATA_RAW_DIR / "depositos.csv")
    componentes = pd.read_csv(DATA_RAW_DIR / "componentes_criticos.csv")
    backlog_raw = pd.read_csv(
        DATA_RAW_DIR / "backlog_mantenimiento.csv",
        usecols=["fecha", "backlog_id", "deposito_id", "unidad_id", "componente_id"],
    )

    fleet_week = pd.read_csv(DATA_PROCESSED_DIR / "fleet_week_features.csv")
    unit_day = pd.read_csv(DATA_PROCESSED_DIR / "unit_day_features.csv")
    scoring = pd.read_csv(DATA_PROCESSED_DIR / "scoring_componentes.csv")
    rul = pd.read_csv(DATA_PROCESSED_DIR / "component_rul_estimate.csv")
    priorities = pd.read_csv(DATA_PROCESSED_DIR / "workshop_priority_table.csv")
    depot_pressure = pd.read_csv(DATA_PROCESSED_DIR / "vw_depot_maintenance_pressure.csv")
    strategy = pd.read_csv(DATA_PROCESSED_DIR / "comparativo_estrategias.csv")
    impact = pd.read_csv(DATA_PROCESSED_DIR / "impacto_diferimiento_resumen.csv")
    inspection_perf = pd.read_csv(DATA_PROCESSED_DIR / "inspection_module_family_performance.csv")
    scheduling_metrics = pd.read_csv(DATA_PROCESSED_DIR / "scheduling_before_after_metrics.csv")

    metrics = load_or_compute_narrative_metrics(force_recompute=False)

    fleet_week["week_start"] = pd.to_datetime(fleet_week["week_start"], errors="coerce")
    unit_day["fecha"] = pd.to_datetime(unit_day["fecha"], errors="coerce")
    depot_pressure["fecha"] = pd.to_datetime(depot_pressure["fecha"], errors="coerce")

    latest_unit_day = _latest_frame(unit_day, "fecha")
    latest_depot_calendar = _latest_frame(depot_pressure, "fecha")
    backlog_cols = ["backlog_physical_items", "backlog_overdue_items", "backlog_critical_items"]
    valid_depot_pressure = depot_pressure[depot_pressure[backlog_cols].fillna(0).sum(axis=1) > 0].copy()
    latest_depot = _latest_frame(valid_depot_pressure, "fecha") if not valid_depot_pressure.empty else latest_depot_calendar.copy()
    latest_depot_calendar_date = latest_depot_calendar["fecha"].max()
    latest_depot_valid_date = latest_depot["fecha"].max()
    latest_depot_calendar_zero_backlog = bool(latest_depot_calendar[backlog_cols].fillna(0).sum().sum() == 0)
    if "backlog_exposure_adjusted_score" not in latest_depot.columns:
        latest_depot["backlog_exposure_adjusted_score"] = latest_depot.get("backlog_risk", 0.0).fillna(0.0).clip(0, 100)

    scoring = scoring.copy()
    if "component_family" not in scoring.columns:
        scoring["component_family"] = scoring.apply(_component_family, axis=1)

    base = priorities.merge(
        componentes[["unidad_id", "componente_id", "sistema_principal", "subsistema", "tipo_componente"]],
        on=["unidad_id", "componente_id"],
        how="left",
    )
    base = base.merge(
        scoring[
            [
                "unidad_id",
                "componente_id",
                "health_score",
                "prob_fallo_30d",
                "main_risk_driver",
                "recommended_action_initial",
                "confidence_flag",
            ]
        ],
        on=["unidad_id", "componente_id"],
        how="left",
    )
    base = _coalesce_columns(base, "component_family", ["component_family", "component_family_x", "component_family_y"])
    base = _coalesce_columns(base, "sistema_principal", ["sistema_principal", "sistema_principal_x", "sistema_principal_y"])
    base = _coalesce_columns(base, "subsistema", ["subsistema", "subsistema_x", "subsistema_y"])
    base = _coalesce_columns(base, "tipo_componente", ["tipo_componente", "tipo_componente_x", "tipo_componente_y"])
    base = _coalesce_columns(base, "health_score", ["health_score", "health_score_x", "health_score_y"])
    base = _coalesce_columns(base, "prob_fallo_30d", ["prob_fallo_30d", "prob_fallo_30d_x", "prob_fallo_30d_y"])
    base = _coalesce_columns(base, "confidence_flag", ["confidence_flag", "confidence_flag_x", "confidence_flag_y"])
    base = base.merge(
        rul[["unidad_id", "componente_id", "component_rul_estimate", "confidence_rul", "rul_window_bucket"]],
        on=["unidad_id", "componente_id"],
        how="left",
    )
    base = _coalesce_columns(
        base,
        "component_rul_estimate",
        ["component_rul_estimate", "component_rul_estimate_x", "component_rul_estimate_y"],
    )
    base = base.merge(
        unidades[["unidad_id", "flota_id", "deposito_id", "linea_servicio", "criticidad_servicio"]],
        on="unidad_id",
        how="left",
    )
    base = _coalesce_columns(base, "deposito_id", ["deposito_id", "deposito_id_x", "deposito_id_y"])
    base = _coalesce_columns(base, "linea_servicio", ["linea_servicio", "linea_servicio_x", "linea_servicio_y"])
    base = base.merge(
        flotas[["flota_id", "estrategia_mantenimiento_actual", "criticidad_operativa"]],
        on="flota_id",
        how="left",
    )
    base = base.merge(
        latest_unit_day[["unidad_id", "impact_on_service_proxy", "predicted_unavailability_risk", "maintenance_load_proxy", "substitution_difficulty"]],
        on="unidad_id",
        how="left",
    )
    base["risk_level"] = base["intervention_priority_score"].apply(_risk_tier)
    base["time_window"] = base["suggested_window_days"].apply(_window_bucket)
    if "component_family" not in base.columns:
        base["component_family"] = base.apply(_component_family, axis=1)
    base["component_family"] = base["component_family"].fillna(base.apply(_component_family, axis=1)).fillna("unknown")
    if "component_rul_estimate" not in base.columns:
        base["component_rul_estimate"] = np.nan
    if "deposito_id" not in base.columns:
        base["deposito_id"] = "unknown"
    if "linea_servicio" not in base.columns:
        base["linea_servicio"] = "unknown"
    base["sistema_principal"] = base["sistema_principal"].fillna("unknown")
    base["recommended_action_initial"] = base["recommended_action_initial"].fillna("monitorizacion_intensiva")
    base["decision_type"] = base["decision_type"].fillna("monitorización intensiva")

    dep_cols = [
        "deposito_id",
        "saturation_ratio",
        "backlog_physical_items",
        "backlog_overdue_items",
        "backlog_critical_items",
        "backlog_exposure_adjusted_score",
    ]
    latest_depot = latest_depot[dep_cols].copy()

    coverage_start = str(metrics.get("coverage_start", fleet_week["week_start"].min().date().isoformat()))
    coverage_end = str(metrics.get("coverage_end", fleet_week["week_start"].max().date().isoformat()))

    fleet_payload = fleet_week[["week_start", "flota_id", "availability_rate", "mtbf_proxy", "mttr_proxy"]].copy()
    fleet_payload["week_start"] = fleet_payload["week_start"].dt.strftime("%Y-%m-%d")

    signature_cols = [
        "unidad_id",
        "componente_id",
        "intervention_priority_score",
        "deferral_risk_score",
        "health_score",
        "prob_fallo_30d",
        "component_rul_estimate",
    ]
    signature_input = (
        base[signature_cols].sort_values(["unidad_id", "componente_id"]).to_csv(index=False)
        + json.dumps(metrics, sort_keys=True, default=str)
    )
    payload_signature = sha1(
        signature_input.encode("utf-8")
    ).hexdigest()[:10]
    dashboard_version = f"{coverage_end.replace('-', '')}-{payload_signature[:4]}"

    priority_non_null_rate = float(
        base[
            [
                "intervention_priority_score",
                "deferral_risk_score",
                "service_impact_score",
                "workshop_fit_score",
                "health_score",
                "prob_fallo_30d",
                "component_rul_estimate",
            ]
        ]
        .notna()
        .mean()
        .mean()
        * 100
    )
    backlog_key = ["fecha", "backlog_id"]
    backlog_duplicate_count = int(backlog_raw.duplicated(backlog_key).sum())
    duplicate_backlog_issue = (
        f"{backlog_duplicate_count} órdenes duplicadas sobre key={backlog_key}"
        if backlog_duplicate_count > 0
        else "sin órdenes duplicadas en el snapshot de backlog"
    )

    anomalies = [
        {
            "severity": "critical",
            "title": "Crisis de backlog físico",
            "value": f"{int(metrics.get('backlog_critical_physical_count', 0)):,}".replace(",", "."),
            "description": f"{int(metrics.get('backlog_overdue_items_count', 0)):,}".replace(",", ".")
            + " pendientes vencidos frente a "
            + f"{int(metrics.get('backlog_physical_items_count', 0)):,}".replace(",", ".")
            + " pendientes físicos.",
        },
        {
            "severity": "warning",
            "title": "Capacidad no absorbe la cola",
            "value": f"{float(scheduling_metrics.loc[scheduling_metrics['scenario'] == 'heuristica_redisenada_35d', 'pendiente_capacidad_pct'].iloc[0]):.1f}%",
            "description": "casos siguen pendientes por capacidad incluso con heurística rediseñada.",
        },
        {
            "severity": "info",
            "title": "Último snapshot válido de backlog",
            "value": str(latest_depot_valid_date.date()) if pd.notna(latest_depot_valid_date) else "n/a",
            "description": "el calendario más reciente no trae backlog físico; el panel usa el último snapshot con carga real.",
        },
        {
            "severity": "warning" if backlog_duplicate_count > 0 else "info",
            "title": "Cobertura de priorización",
            "value": f"{priority_non_null_rate:.1f}%",
            "description": duplicate_backlog_issue,
        },
    ]

    payload = {
        "rows": base[
            [
                "unidad_id",
                "componente_id",
                "flota_id",
                "deposito_id",
                "deposito_recomendado",
                "linea_servicio",
                "component_family",
                "sistema_principal",
                "risk_level",
                "decision_type",
                "recommended_action_initial",
                "time_window",
                "intervention_priority_score",
                "deferral_risk_score",
                "service_impact_score",
                "workshop_fit_score",
                "health_score",
                "prob_fallo_30d",
                "component_rul_estimate",
                "main_risk_driver",
                "confidence_flag",
                "estrategia_mantenimiento_actual",
                "impact_on_service_proxy",
                "predicted_unavailability_risk",
            ]
        ].fillna("").to_dict(orient="records"),
        "fleet_week": fleet_payload.fillna("").to_dict(orient="records"),
        "depot_latest": latest_depot.fillna(0).to_dict(orient="records"),
        "strategy": strategy.fillna(0).to_dict(orient="records"),
        "deferral": impact[["defer_dias", "costo_total_eur", "downtime_total_h"]].fillna(0).to_dict(orient="records"),
        "inspection": inspection_perf.fillna(0).to_dict(orient="records"),
        "scheduling_metrics": scheduling_metrics.fillna(0).to_dict(orient="records"),
        "anomalies": anomalies,
        "metric_snapshot": {
            "fleet_availability_pct": float(metrics.get("fleet_availability_pct", 0.0)),
            "mtbf_proxy_hours": float(metrics.get("mtbf_proxy_hours", 0.0)),
            "mttr_proxy_hours": float(metrics.get("mttr_proxy_hours", 0.0)),
            "high_risk_units_count": float(metrics.get("high_risk_units_count", 0.0)),
            "backlog_physical_items_count": float(metrics.get("backlog_physical_items_count", 0.0)),
            "backlog_overdue_items_count": float(metrics.get("backlog_overdue_items_count", 0.0)),
            "backlog_critical_physical_count": float(metrics.get("backlog_critical_physical_count", 0.0)),
            "cbm_operational_savings_eur": float(metrics.get("cbm_operational_savings_eur", 0.0)),
            "cbm_value_range_min_eur": float(metrics.get("cbm_value_range_min_eur", 0.0)),
            "cbm_value_range_max_eur": float(metrics.get("cbm_value_range_max_eur", 0.0)),
            "cbm_prob_positive_savings": float(metrics.get("cbm_prob_positive_savings", 0.0)),
            "deferral_cost_delta_14d_eur": float(metrics.get("deferral_cost_delta_14d_eur", 0.0)),
            "deferral_downtime_delta_14d_h": float(metrics.get("deferral_downtime_delta_14d_h", 0.0)),
            "avoidable_downtime_hours_inspection": float(metrics.get("avoidable_downtime_hours_inspection", 0.0)),
            "avoidable_correctives_inspection": float(metrics.get("avoidable_correctives_inspection", 0.0)),
            "mean_depot_saturation_pct": float(metrics.get("mean_depot_saturation_pct", 0.0)),
            "high_deferral_risk_cases_count": float(metrics.get("high_deferral_risk_cases_count", 0.0)),
            "backlog_exposure_adjusted_mean": float(metrics.get("backlog_exposure_adjusted_mean", 0.0)),
            "top_unit_by_priority": str(metrics.get("top_unit_by_priority", "n/a")),
            "top_component_by_priority": str(metrics.get("top_component_by_priority", "n/a")),
            "top_component_family_by_priority": str(metrics.get("top_component_family_by_priority", "n/a")),
            "top_priority_score": float(metrics.get("top_priority_score", 0.0)),
            "top_deferral_risk_score": float(metrics.get("top_deferral_risk_score", 0.0)),
            "top_depot_by_saturation": str(metrics.get("top_depot_by_saturation", "n/a")),
            "top_depot_saturation_pct": float(metrics.get("top_depot_saturation_pct", 0.0)),
            "early_warnings_active_count": float((base["prob_fallo_30d"] >= 0.65).sum()),
            "row_count_components": float(len(base)),
            "row_count_units": float(base["unidad_id"].nunique()),
        },
        "meta": {
            "dashboard_version": dashboard_version,
            "payload_signature": payload_signature,
            "rows": int(len(base)),
            "latest_depot_valid_date": str(latest_depot_valid_date.date()) if pd.notna(latest_depot_valid_date) else "",
            "latest_depot_calendar_date": str(latest_depot_calendar_date.date()) if pd.notna(latest_depot_calendar_date) else "",
            "latest_depot_calendar_zero_backlog": latest_depot_calendar_zero_backlog,
        },
    }

    template = """
<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Dashboard CBM Ferroviario</title>
  <meta name="dashboard-version" content="__DASHBOARD_VERSION__" />
  <meta name="dashboard-signature" content="__PAYLOAD_SIGNATURE__" />
  <meta name="theme-color" content="#fafafa" media="(prefers-color-scheme: light)" />
  <meta name="theme-color" content="#0a0a0b" media="(prefers-color-scheme: dark)" />
  <style>
    __FONT_FACES__
    :root{
      color-scheme:light;
      --font-sans:"IBM Plex Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
      --font-mono:"IBM Plex Mono",ui-monospace,"SF Mono",Menlo,monospace;
      --bg:#fafafa;--bg-soft:#f4f4f5;--card:#ffffff;--card-soft:#fafafa;
      --ink:#18181b;--ink-soft:#3f3f46;--muted:#71717a;--muted-soft:#a1a1aa;
      --line:#e4e4e7;--line-strong:#d4d4d8;
      --accent:#2f6ae4;--accent-ink:#ffffff;--accent-wash:#eef3fd;--accent-line:#cfddf8;
      --critical:#b42318;--critical-wash:#fdf0ee;--critical-line:#f3cfc9;
      --warning:#b45309;--warning-wash:#fcf4e8;--warning-line:#f0ddbe;
      --positive:#15803d;--positive-wash:#edf6f0;--positive-line:#c5e4d1;
      --c-grid:#ececed;--c-axis:#a1a1aa;--c-ink:#3f3f46;
      --c-s1:#2f6ae4;--c-s2:#64748b;--c-danger:#b42318;--c-warning:#b45309;--c-positive:#15803d;--c-neutral:#94a3b8;
      --tip-bg:#18181b;--tip-ink:#fafafa;--tip-line:#3f3f46;
      --shadow:0 1px 2px rgba(24,24,27,.04),0 2px 6px rgba(24,24,27,.05);
      --shadow-soft:0 1px 2px rgba(24,24,27,.04);
      --sidebar-width:264px;--radius-lg:12px;--radius-md:9px;--radius-sm:7px;
    }
    [data-theme="dark"]{
      color-scheme:dark;
      --bg:#09090b;--bg-soft:#131316;--card:#161618;--card-soft:#1c1c1f;
      --ink:#f4f4f5;--ink-soft:#d4d4d8;--muted:#a1a1aa;--muted-soft:#71717a;
      --line:#27272a;--line-strong:#3f3f46;
      --accent:#5b8cff;--accent-ink:#0a0a0b;--accent-wash:#14203a;--accent-line:#2a3b63;
      --critical:#f0635a;--critical-wash:#2a1614;--critical-line:#52211d;
      --warning:#e0a04e;--warning-wash:#241b0f;--warning-line:#4a3414;
      --positive:#4ec07f;--positive-wash:#0f2218;--positive-line:#1d4530;
      --c-grid:#26272b;--c-axis:#71717a;--c-ink:#d4d4d8;
      --c-s1:#5b8cff;--c-s2:#94a3b8;--c-danger:#f0635a;--c-warning:#e0a04e;--c-positive:#4ec07f;--c-neutral:#5b6472;
      --tip-bg:#fafafa;--tip-ink:#18181b;--tip-line:#d4d4d8;
      --shadow:0 1px 2px rgba(0,0,0,.4),0 2px 8px rgba(0,0,0,.35);
      --shadow-soft:0 1px 2px rgba(0,0,0,.35);
    }
    *{box-sizing:border-box}
    html{scroll-behavior:smooth}
    body{margin:0;background:var(--bg);color:var(--ink);font-family:var(--font-sans);overflow-x:hidden;line-height:1.5;-webkit-font-smoothing:antialiased;font-feature-settings:"cv05","ss01";transition:background .2s ease,color .2s ease}
    body,button,input,select{font-variant-numeric:tabular-nums}
    button,select,input,a{touch-action:manipulation;font-family:inherit}
    button:focus-visible,select:focus-visible,input:focus-visible,a:focus-visible{outline:2px solid var(--accent);outline-offset:2px}
    .num{font-family:var(--font-mono);font-feature-settings:"tnum"}
    .skip-link{position:fixed;left:12px;top:12px;z-index:100;background:var(--card);color:var(--ink);padding:9px 12px;border-radius:8px;border:1px solid var(--line);box-shadow:var(--shadow);transform:translateY(-160%);transition:transform .16s ease}
    .skip-link:focus-visible{transform:translateY(0)}
    .layout{display:grid;grid-template-columns:minmax(260px,var(--sidebar-width)) minmax(0,1fr);gap:20px;min-height:100svh;width:100%;max-width:none;align-items:start;padding:20px}
    body.filters-collapsed .layout{grid-template-columns:minmax(0,1fr)}
    .sidebar{background:var(--card);color:var(--ink);padding:20px 18px;position:sticky;top:20px;height:auto;overflow:visible;border:1px solid var(--line);border-radius:var(--radius-lg);box-shadow:var(--shadow-soft);align-self:start}
    body.filters-collapsed .sidebar{display:none}
    .sidebar h2{margin:0 0 6px;font-size:.82rem;letter-spacing:.02em;font-weight:600;text-transform:uppercase;color:var(--muted)}
    .sidebar p{margin:0 0 18px;font-size:.78rem;color:var(--muted);line-height:1.5}
    .sidebar .brand{padding:0 0 16px;border-bottom:1px solid var(--line);margin-bottom:18px}
    .sidebar .brand b{display:block;font-size:.98rem;letter-spacing:-.01em;line-height:1.25;color:var(--ink);font-weight:600}
    .sidebar .brand span{font-size:.78rem;color:var(--muted);line-height:1.45}
    .sidebar .eyebrow{display:block;font-size:.64rem;text-transform:uppercase;letter-spacing:.14em;color:var(--accent);margin-bottom:8px;font-weight:600}
    .filter-group{margin-bottom:13px}
    .filter-group label{display:block;font-size:.72rem;margin-bottom:5px;color:var(--ink-soft);font-weight:500;letter-spacing:.01em}
    .filter-group select,.filter-group input{
      width:100%;padding:9px 32px 9px 11px;border-radius:var(--radius-sm);border:1px solid var(--line);background:var(--card-soft);color:var(--ink);font-size:.82rem;
      -webkit-appearance:none;appearance:none;background-image:
      linear-gradient(45deg, transparent 50%, var(--muted) 50%),
      linear-gradient(135deg, var(--muted) 50%, transparent 50%);
      background-position: calc(100% - 15px) calc(50% - 1px), calc(100% - 10px) calc(50% - 1px);
      background-size: 5px 5px, 5px 5px;background-repeat:no-repeat;transition:border-color .14s ease,box-shadow .14s ease;
    }
    .filter-group select:hover,.filter-group input:hover{border-color:var(--line-strong)}
    .filter-group select:focus,.filter-group input:focus{outline:none;border-color:var(--accent);box-shadow:0 0 0 3px var(--accent-wash)}
    .side-actions{display:flex;gap:8px;margin:16px 0 12px}
    .btn{border:1px solid var(--line);background:var(--card);color:var(--ink);border-radius:var(--radius-sm);padding:8px 13px;font-size:.78rem;font-weight:500;cursor:pointer;transition:background .14s ease,border-color .14s ease,color .14s ease}
    .btn:hover{background:var(--bg-soft);border-color:var(--line-strong)}
    .btn-reset{width:100%;background:var(--accent);border-color:var(--accent);color:var(--accent-ink);font-weight:600}
    .btn-reset:hover{filter:brightness(1.05);background:var(--accent);border-color:var(--accent)}
    .btn-top{position:fixed;right:18px;bottom:18px;background:var(--card);border:1px solid var(--line-strong);color:var(--ink);box-shadow:var(--shadow);z-index:30;opacity:0;pointer-events:none;transform:translateY(10px);transition:opacity .18s ease, transform .18s ease}
    .btn-top.visible{opacity:1;pointer-events:auto;transform:translateY(0)}
    .btn-icon{display:inline-flex;align-items:center;justify-content:center;width:34px;height:34px;padding:0;font-size:.95rem}
    .sidebar-stats{margin-top:18px;padding-top:16px;border-top:1px solid var(--line);font-size:.8rem;color:var(--muted);display:grid;gap:7px}
    .sidebar-stats div{display:flex;justify-content:space-between;align-items:baseline;gap:8px}
    .sidebar-stats b{color:var(--ink);font-family:var(--font-mono);font-size:.92rem;font-weight:600}
    .content{padding:0 0 32px;min-width:0;overflow-x:hidden;max-width:1640px;width:100%}
    .header{position:relative;background:transparent;color:var(--ink);padding:0 0 4px}
    .header-row{display:flex;justify-content:space-between;gap:18px;align-items:flex-start;flex-wrap:wrap}
    .header-main{max-width:920px}
    .header .eyebrow{display:block;font-size:.68rem;text-transform:uppercase;letter-spacing:.14em;color:var(--accent);font-weight:600;margin-bottom:7px}
    .header h1,.section-head h3{font-family:var(--font-sans);letter-spacing:-.02em;font-weight:600}
    .header h1{margin:0;font-size:1.56rem;line-height:1.1;max-width:980px;text-wrap:balance}
    .header-actions{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
    .sub{margin-top:9px;color:var(--muted);font-size:.92rem;max-width:880px;line-height:1.5}
    .meta{display:flex;gap:8px;flex-wrap:wrap;margin-top:16px;min-width:0}
    .pill{font-size:.74rem;background:var(--card);border:1px solid var(--line);color:var(--ink-soft);padding:5px 10px;border-radius:999px;font-weight:500}
    .pill b{font-family:var(--font-mono);font-weight:600;color:var(--ink)}
    .top-nav{display:flex;gap:6px;flex-wrap:wrap;margin-top:18px;min-width:0;padding-bottom:2px}
    .top-nav a{font-size:.78rem;text-decoration:none;color:var(--muted);background:transparent;border:1px solid transparent;padding:6px 11px;border-radius:7px;font-weight:500;transition:background .14s ease,color .14s ease}
    .top-nav a:hover{background:var(--bg-soft);color:var(--ink)}
    .insight{margin-top:14px;padding:13px 15px;border-radius:var(--radius-md);background:var(--card);border:1px solid var(--line);border-left:3px solid var(--accent);font-size:.88rem;color:var(--ink-soft);font-weight:400;line-height:1.5}
    .insight b{color:var(--ink);font-weight:600;font-family:var(--font-mono);font-size:.86rem}
    .filter-state{margin-top:12px;padding:9px 12px;border-radius:var(--radius-sm);background:var(--bg-soft);border:1px solid var(--line);font-size:.79rem;color:var(--muted);line-height:1.5}
    .cards{margin-top:18px;display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,200px),1fr));gap:12px;min-width:0}
    .cards.cards-primary{grid-template-columns:repeat(auto-fit,minmax(min(100%,232px),1fr));gap:12px}
    .card{overflow:hidden;background:var(--card);border:1px solid var(--line);border-radius:var(--radius-md);padding:15px 16px;box-shadow:var(--shadow-soft);min-width:0;transition:border-color .14s ease}
    .card:hover{border-color:var(--line-strong)}
    .card.primary{padding:17px 17px 16px}
    .card .k{font-size:.7rem;color:var(--muted);text-transform:uppercase;letter-spacing:.08em;font-weight:500}
    .card .v{margin-top:10px;font-family:var(--font-mono);font-size:1.6rem;font-weight:600;line-height:1;letter-spacing:-.02em;color:var(--ink)}
    .card.primary .v{font-size:2.15rem}
    .card.primary.risk .v{color:var(--critical)}
    .card.primary.capacity .v{color:var(--warning)}
    .card.primary.value.pos .v{color:var(--positive)}
    .card.primary.value.neg .v{color:var(--critical)}
    .card .delta{display:inline-flex;align-items:center;gap:4px;margin-top:11px;font-family:var(--font-mono);font-size:.74rem;font-weight:600;padding:2px 7px;border-radius:999px;letter-spacing:.01em}
    .card .delta.up{color:var(--positive);background:var(--positive-wash)}
    .card .delta.down{color:var(--critical);background:var(--critical-wash)}
    .card .delta.flat{color:var(--muted);background:var(--bg-soft)}
    .card .s{margin-top:9px;font-size:.79rem;color:var(--muted);line-height:1.45}
    .card .rule{margin-top:9px;padding-top:8px;border-top:1px solid var(--line);font-size:.71rem;color:var(--muted-soft);line-height:1.4;font-weight:400}
    .cards.cards-ribbon{margin-top:14px;grid-template-columns:repeat(4,minmax(0,1fr));gap:1px;background:var(--line);border:1px solid var(--line);border-radius:var(--radius-md);overflow:hidden;box-shadow:var(--shadow-soft)}
    @media (min-width:1500px){.cards.cards-ribbon{grid-template-columns:repeat(8,minmax(0,1fr))}}
    @media (max-width:760px){.cards.cards-ribbon{grid-template-columns:repeat(2,minmax(0,1fr))}}
    .cards-ribbon .card{border:none;border-radius:0;box-shadow:none;padding:13px 15px 14px;background:var(--card)}
    .cards-ribbon .card:hover{border:none;background:var(--card-soft)}
    .cards-ribbon .card .k{font-size:.67rem;letter-spacing:.07em}
    .cards-ribbon .card .v{font-size:1.34rem;margin-top:8px}
    .cards-ribbon .card .s{margin-top:6px;font-size:.72rem;color:var(--muted-soft)}
    .section{margin-top:30px;background:transparent;border:none;border-radius:0;padding:0;min-width:0;scroll-margin-top:18px}
    .section.priority-block{margin-top:28px}
    .section.detail-block{margin-top:28px}
    .section-head{display:flex;justify-content:space-between;gap:14px;align-items:flex-end;flex-wrap:wrap;margin-bottom:14px;padding-bottom:12px;border-bottom:1px solid var(--line)}
    .section-head .eyebrow{display:block;font-size:.67rem;text-transform:uppercase;letter-spacing:.13em;color:var(--accent);font-weight:600;margin-bottom:6px}
    .section-head h3{margin:0;font-size:1.18rem;line-height:1.15;text-wrap:balance}
    .section-head p{margin:7px 0 0;font-size:.84rem;color:var(--muted);max-width:760px;line-height:1.5}
    .grid2{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,440px),1fr));gap:14px;min-width:0}
    .chart-box{background:var(--card);border:1px solid var(--line);border-radius:var(--radius-md);padding:16px 17px;min-width:0;overflow:hidden;box-shadow:var(--shadow-soft)}
    .chart-head{display:flex;justify-content:space-between;gap:10px;align-items:flex-start;flex-wrap:wrap}
    .chart-box h4{margin:0;font-size:.96rem;color:var(--ink);line-height:1.3;letter-spacing:-.01em;font-weight:600}
    .chart-note{margin-top:5px;font-size:.79rem;color:var(--muted);line-height:1.5}
    .chart-question{display:block;margin-top:7px;color:var(--muted-soft);font-size:.74rem;font-weight:500;font-style:italic;line-height:1.4}
    .chart-legend{display:flex;gap:12px;flex-wrap:wrap;margin-top:11px}
    .legend-chip{display:inline-flex;align-items:center;gap:6px;font-size:.74rem;color:var(--muted);background:transparent;border:none;padding:0}
    .legend-dot{display:inline-block;width:9px;height:9px;border-radius:3px}
    .svg-chart{width:100%;height:clamp(244px,28vh,320px);min-height:244px;border-top:1px solid var(--line);overflow:hidden;margin-top:13px;padding-top:4px}
    .svg-chart text{font-family:var(--font-mono);font-size:11px}
    .svg-chart rect,.svg-chart circle,.svg-chart path{transition:opacity .12s ease}
    .chart-tooltip{
      position:fixed;z-index:80;pointer-events:none;max-width:260px;background:var(--tip-bg);color:var(--tip-ink);
      border:1px solid var(--tip-line);border-radius:8px;padding:8px 10px;font-size:.75rem;line-height:1.4;
      box-shadow:0 8px 24px rgba(0,0,0,.22)
    }
    .toolbar{display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin:14px 0 10px}
    .toolbar input{padding:10px 13px;border:1px solid var(--line);border-radius:var(--radius-sm);min-width:280px;max-width:100%;background:var(--card);color:var(--ink);font-size:.85rem}
    .toolbar input:focus{outline:none;border-color:var(--accent);box-shadow:0 0 0 3px var(--accent-wash)}
    .toolbar .count{font-size:.78rem;color:var(--muted);background:var(--bg-soft);padding:7px 11px;border-radius:999px;border:1px solid var(--line);font-weight:500}
    .pager{display:flex;gap:6px;align-items:center;flex-wrap:wrap;margin-left:auto}
    .pager .btn{padding:7px 11px}
    .pager .btn[disabled]{opacity:.4;cursor:not-allowed}
    .pager select{padding:7px 8px;border-radius:7px;border:1px solid var(--line);background:var(--card);color:var(--ink)}
    .pager .page-info{font-size:.78rem;color:var(--muted);background:var(--bg-soft);border:1px solid var(--line);padding:6px 10px;border-radius:999px;font-weight:500;font-family:var(--font-mono)}
    .pager-label{font-size:.78rem;color:var(--muted);display:inline-flex;align-items:center;gap:6px}
    .table-wrap{margin-top:10px;border:1px solid var(--line);border-radius:var(--radius-md);overflow:auto;max-height:620px;max-width:100%;background:var(--card)}
    .empty-row{text-align:center;color:var(--muted);padding:24px}
    table{width:100%;border-collapse:collapse;min-width:980px}
    th,td{padding:10px 12px;border-bottom:1px solid var(--line);font-size:.81rem;text-align:left;white-space:nowrap}
    td{color:var(--ink-soft)}
    td.num,th.num{font-family:var(--font-mono)}
    th{position:sticky;top:0;background:var(--bg-soft);color:var(--muted);cursor:pointer;z-index:2;letter-spacing:.02em;font-weight:600;text-transform:uppercase;font-size:.7rem;border-bottom:1px solid var(--line-strong)}
    th:hover{color:var(--ink)}
    tbody tr:hover td{background:var(--bg-soft)}
    .badge{padding:2px 9px;border-radius:999px;font-weight:500;font-size:.72rem;display:inline-block;border:1px solid transparent}
    .badge-critico{background:var(--critical-wash);color:var(--critical);border-color:var(--critical-line)}
    .badge-alto{background:var(--warning-wash);color:var(--warning);border-color:var(--warning-line)}
    .badge-medio{background:var(--bg-soft);color:var(--ink-soft);border-color:var(--line)}
    .badge-bajo{background:var(--positive-wash);color:var(--positive);border-color:var(--positive-line)}
    .footer-note{font-size:.77rem;color:var(--muted);margin-top:14px;background:var(--bg-soft);border:1px solid var(--line);padding:11px 13px;border-radius:var(--radius-sm);line-height:1.5}
    .command-panel{margin-top:0;display:grid;grid-template-columns:minmax(0,1.3fr) minmax(280px,.7fr);gap:14px;align-items:stretch}
    .command-card{border:1px solid var(--line);border-radius:var(--radius-md);padding:17px 18px;background:var(--card);box-shadow:var(--shadow-soft);min-width:0}
    .command-card h2{margin:0;font-size:.7rem;letter-spacing:.1em;text-transform:uppercase;color:var(--muted);font-weight:600}
    .command-card.critical h2{color:var(--critical)}
    .command-card .big{margin-top:9px;font-size:1.6rem;line-height:1.08;font-weight:600;letter-spacing:-.02em;overflow-wrap:anywhere;color:var(--ink)}
    .command-card p{margin:9px 0 0;color:var(--muted);font-size:.85rem;line-height:1.5}
    .decision-steps{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:9px;margin-top:14px}
    .decision-steps.single{grid-template-columns:1fr}
    .decision-proof{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:9px;margin-top:14px}
    .proof-chip{padding:10px 11px;border-radius:var(--radius-sm);background:var(--card);border:1px solid var(--line);min-width:0}
    .proof-chip b{display:block;font-size:.64rem;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);font-weight:500}
    .proof-chip span{display:block;margin-top:5px;font-size:.92rem;font-weight:600;color:var(--ink);overflow-wrap:anywhere;font-family:var(--font-mono)}
    .step{padding:11px 12px;border-radius:var(--radius-sm);background:var(--bg-soft);border:1px solid var(--line);min-width:0}
    .step b{display:block;font-size:.66rem;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);font-weight:500}
    .step span{display:block;margin-top:5px;font-size:.86rem;font-weight:500;overflow-wrap:anywhere;color:var(--ink)}
    .anomaly-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,230px),1fr));gap:11px;margin-top:0}
    .anomaly{border:1px solid var(--line);border-radius:var(--radius-sm);background:var(--card);padding:13px 14px;box-shadow:var(--shadow-soft);min-width:0}
    .anomaly .label{font-size:.66rem;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);font-weight:500}
    .anomaly .value{margin-top:6px;font-size:1.2rem;font-weight:600;letter-spacing:-.01em;font-family:var(--font-mono);color:var(--ink)}
    .anomaly.critical .value{color:var(--critical)}
    .anomaly.warning .value{color:var(--warning)}
    .anomaly .text{margin-top:6px;font-size:.79rem;color:var(--muted);line-height:1.45}
    .interpretation{margin-top:14px;display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,260px),1fr));gap:11px}
    .interpretation .item{padding:13px 14px;border-radius:var(--radius-sm);background:var(--card);border:1px solid var(--line);color:var(--muted);font-size:.82rem;line-height:1.5}
    .interpretation b{display:block;color:var(--ink);margin-bottom:5px;font-weight:600;font-size:.78rem}
    @media (max-width:1460px){
      .cards{grid-template-columns:repeat(auto-fit,minmax(min(100%,180px),1fr))}
      .grid2{grid-template-columns:repeat(auto-fit,minmax(min(100%,380px),1fr))}
    }
    @media (max-width:1240px){
      .layout{padding:14px;grid-template-columns:1fr}
      .sidebar{position:relative;top:auto;height:auto;max-height:none;order:2}
      .content{order:1}
      .btn-top{bottom:12px;right:12px}
    }
    @media (max-width:860px){
      .content{padding:0 0 16px}
      .header h1{font-size:1.3rem}
      .grid2{grid-template-columns:1fr}
      .cards{grid-template-columns:repeat(2,minmax(120px,1fr));gap:8px}
      .card.primary .v{font-size:1.8rem}
      .command-panel{grid-template-columns:1fr}
      .decision-steps{grid-template-columns:1fr}
      .decision-proof{grid-template-columns:repeat(2,minmax(0,1fr))}
      .toolbar input{min-width:100%}
      .pager{width:100%;justify-content:flex-start;margin-left:0}
      .table-wrap{max-height:520px}
      table{min-width:760px}
    }
    @media print{
      body{background:#fff;color:#111}
      .sidebar,.top-nav,.btn-top,.btn-icon,.btn-secondary,.filter-state{display:none !important}
      .layout{grid-template-columns:1fr;padding:0}
      .section,.card,.chart-box,.command-card{box-shadow:none}
      .chart-tooltip{display:none !important}
      .section{page-break-inside:avoid}
    }
    @media (prefers-reduced-motion: reduce){
      *,*::before,*::after{animation-duration:.01ms !important;animation-iteration-count:1 !important;transition-duration:.01ms !important;scroll-behavior:auto !important}
      html{scroll-behavior:auto}
    }
  </style>
</head>
<body>
<a href="#mainContent" class="skip-link">Saltar al panel principal</a>
<div class="layout">
  <aside class="sidebar" id="globalFilters" aria-label="Filtros globales">
    <div class="brand">
      <span class="eyebrow">Centro de decisión</span>
      <b>Inteligencia de Mantenimiento Ferroviario</b>
      <span>Panel de control para salud de activos, priorización de taller y valor CBM.</span>
    </div>
    <h2>Filtros Globales</h2>
    <p>Aplican sobre gráficos, ranking operativo y tabla de detalle. Los KPI superiores permanecen gobernados por la capa oficial.</p>
    <div class="filter-group"><label for="f_flota">Flota</label><select id="f_flota" name="flota" autocomplete="off"></select></div>
    <div class="filter-group"><label for="f_unidad">Unidad</label><select id="f_unidad" name="unidad" autocomplete="off"></select></div>
    <div class="filter-group"><label for="f_deposito">Depósito recomendado</label><select id="f_deposito" name="deposito" autocomplete="off"></select></div>
    <div class="filter-group"><label for="f_familia">Familia componente</label><select id="f_familia" name="familia" autocomplete="off"></select></div>
    <div class="filter-group"><label for="f_sistema">Sistema principal</label><select id="f_sistema" name="sistema" autocomplete="off"></select></div>
    <div class="filter-group"><label for="f_riesgo">Nivel de riesgo</label><select id="f_riesgo" name="riesgo" autocomplete="off"></select></div>
    <div class="filter-group"><label for="f_intervencion">Tipo intervención</label><select id="f_intervencion" name="intervencion" autocomplete="off"></select></div>
    <div class="filter-group"><label for="f_ventana">Ventana temporal</label><select id="f_ventana" name="ventana" autocomplete="off"></select></div>
    <div class="filter-group"><label for="f_estrategia">Estrategia mantenimiento</label><select id="f_estrategia" name="estrategia" autocomplete="off"></select></div>
    <div class="side-actions">
      <button type="button" class="btn btn-reset" id="btnReset">Resetear filtros</button>
    </div>
    <div class="sidebar-stats">
      <div><b id="s_count_rows">0</b> componentes filtrados</div>
      <div><b id="s_count_units">0</b> unidades filtradas</div>
      <div><b id="s_count_high">0</b> unidades prioridad >=70</div>
    </div>
  </aside>

  <main class="content" id="mainContent">
    <section class="header">
      <div class="header-row">
        <div class="header-main">
          <span class="eyebrow">Mantenimiento basado en condición</span>
          <h1>Centro de control CBM ferroviario</h1>
          <div class="sub">Estado de la flota, riesgo técnico y operativo, y la entrada a taller que debe ejecutarse primero.</div>
        </div>
        <div class="header-actions">
          <button type="button" class="btn btn-secondary" id="btnFilters" aria-controls="globalFilters" aria-expanded="true">Ocultar filtros</button>
          <button type="button" class="btn btn-icon" id="btnTheme" aria-label="Cambiar tema" title="Cambiar tema claro/oscuro">◐</button>
          <button type="button" class="btn btn-print" id="btnPrint">Imprimir</button>
        </div>
      </div>
      <div class="meta">
        <span class="pill">Cobertura: __COVERAGE_START__ a __COVERAGE_END__</span>
        <span class="pill">Flotas: __N_FLOTAS__</span>
        <span class="pill">Unidades: __N_UNIDADES__</span>
        <span class="pill">Depósitos: __N_DEPOSITOS__</span>
        <span class="pill">Componentes: __N_COMPONENTES__</span>
      </div>
      <div class="insight" id="headerInsight">Resumen operativo: salud de activos, riesgo operativo y prioridades de taller para la ventana seleccionada.</div>
      <nav class="top-nav" aria-label="Navegación del dashboard">
        <a href="#kpiCardsPrimary">Resumen</a>
        <a href="#sec_saude">Riesgo técnico</a>
        <a href="#sec_taller">Backlog y capacidad</a>
        <a href="#sec_alertas">Drivers</a>
        <a href="#sec_action">Qué hacer ahora</a>
        <a href="#sec_estrategica">Escenarios</a>
        <a href="#sec_tabela">Detalle</a>
      </nav>
      <div class="filter-state" id="filterState" aria-live="polite">Vista completa sin filtros activos.</div>
    </section>

    <section class="cards cards-primary" id="kpiCardsPrimary">
      <div class="card primary"><div class="k">Disponibilidad de flota</div><div class="v" id="k_avail">-</div><div class="delta flat" id="k_avail_delta">—</div></div>
      <div class="card primary risk"><div class="k">Unidades prioridad ≥70</div><div class="v" id="k_uhr">-</div><div class="s">Compiten por entrada prioritaria a taller.</div></div>
      <div class="card primary capacity"><div class="k">Backlog crítico físico</div><div class="v" id="k_bcf">-</div><div class="s" id="k_bcf_sub">Carga prioritaria pendiente.</div></div>
      <div class="card primary value pos"><div class="k">Diferencial CBM vs reactivo</div><div class="v" id="k_ahorro">-</div><div class="s" id="k_ahorro_sub">Coste operativo proxy frente al escenario reactivo.</div></div>
    </section>

    <section class="cards cards-ribbon" id="kpiCards" aria-label="Métricas operativas de detalle">
      <div class="card"><div class="k">MTBF</div><div class="v" id="k_mtbf">-</div><div class="s">horas</div></div>
      <div class="card"><div class="k">MTTR</div><div class="v" id="k_mttr">-</div><div class="s">horas</div></div>
      <div class="card"><div class="k">Backlog físico</div><div class="v" id="k_bf">-</div><div class="s">órdenes abiertas</div></div>
      <div class="card"><div class="k">Backlog vencido</div><div class="v" id="k_bv">-</div><div class="s">fuera de ventana</div></div>
      <div class="card"><div class="k">Riesgo diferir alto</div><div class="v" id="k_drh">-</div><div class="s">casos score ≥70</div></div>
      <div class="card"><div class="k">Correctivas evitables</div><div class="v" id="k_ce">-</div><div class="s">migrables a plan</div></div>
      <div class="card"><div class="k">Componentes riesgo ≥65%</div><div class="v" id="k_alertas">-</div><div class="s">fallo a 30 días</div></div>
      <div class="card"><div class="k">Saturación media taller</div><div class="v" id="k_sat">-</div><div class="s" id="k_sat_sub">capacidad usada</div></div>
    </section>
    <section class="section explainer-block" id="sec_saude">
      <div class="section-head">
        <div>
          <span class="eyebrow">Riesgo técnico</span>
          <h3>La prioridad se explica por deterioro, probabilidad de fallo y RUL</h3>
          <p>Estas vistas muestran si la decisión nace de degradación real, ventana corta o concentración de familias críticas.</p>
        </div>
      </div>
      <div class="grid2">
        <div class="chart-box">
          <div class="chart-head"><h4>Deterioro y riesgo por familia</h4></div>
          <div class="chart-note">Compara presión de riesgo frente a salud media para detectar familias con peor equilibrio.</div>
          <div class="chart-question">Pregunta: ¿qué familia concentra degradación accionable?</div>
          <div class="chart-legend"><span class="legend-chip"><span class="legend-dot" style="background:var(--c-danger)"></span>Riesgo</span><span class="legend-chip"><span class="legend-dot" style="background:var(--c-positive)"></span>Salud</span></div>
          <div id="ch_family" class="svg-chart"></div>
        </div>
        <div class="chart-box">
          <div class="chart-head"><h4>Distribución de RUL por ventana de intervención</h4></div>
          <div class="chart-note">Identifica concentración de componentes en ventanas cortas y tensión del frente de trabajo.</div>
          <div class="chart-question">Pregunta: ¿cuántos componentes entran en ventana corta?</div>
          <div id="ch_rul" class="svg-chart"></div>
        </div>
      </div>
    </section>

    <section class="section explainer-block" id="sec_operacao">
      <div class="section-head">
        <div>
          <span class="eyebrow">Impacto operativo</span>
          <h3>La cola se ordena por urgencia y daño evitado al servicio</h3>
          <p>Contrasta prioridad, riesgo de diferimiento e impacto operacional para confirmar que la primera acción es coherente.</p>
        </div>
      </div>
      <div class="grid2">
        <div class="chart-box"><div class="chart-head"><h4>Top unidades por prioridad de intervención</h4></div><div class="chart-note">Ordena las unidades con mayor urgencia relativa en la cartera filtrada.</div><div class="chart-question">Pregunta: ¿qué unidad entra primero?</div><div id="ch_top_units" class="svg-chart"></div></div>
        <div class="chart-box"><div class="chart-head"><h4>Impacto en servicio por unidad</h4></div><div class="chart-note">Muestra dónde una intervención temprana evita mayor daño al servicio.</div><div class="chart-question">Pregunta: ¿dónde se protege más servicio?</div><div id="ch_service" class="svg-chart"></div></div>
        <div class="chart-box"><div class="chart-head"><h4>Prioridad vs riesgo de diferimiento</h4></div><div class="chart-note">Separa componentes urgentes por decisión de cola de los casos que no deben aplazarse.</div><div class="chart-question">Pregunta: ¿qué casos son prioridad alta y no diferibles?</div><div id="ch_priority_deferral" class="svg-chart"></div></div>
      </div>
    </section>

    <section class="section explainer-block" id="sec_taller">
      <div class="section-head">
        <div>
          <span class="eyebrow">Restricciones de ejecución</span>
          <h3>El backlog y la capacidad explican qué puede ejecutarse realmente</h3>
          <p>Separar carga física, vencimiento y saturación evita confundir riesgo analítico con cola real de taller.</p>
        </div>
      </div>
      <div class="grid2">
        <div class="chart-box"><div class="chart-head"><h4>Backlog físico, vencido y crítico por depósito</h4></div><div class="chart-note">Usa el último snapshot operativo válido para mostrar dónde está la crisis real de cola.</div><div class="chart-question">Pregunta: ¿qué depósito está bloqueando la recuperación?</div><div id="ch_backlog_depot" class="svg-chart"></div></div>
        <div class="chart-box"><div class="chart-head"><h4>Saturación por depósito</h4></div><div class="chart-note">Prioriza depósitos donde la capacidad disponible se acerca al límite operativo.</div><div class="chart-question">Pregunta: ¿dónde falta capacidad de taller?</div><div id="ch_depot" class="svg-chart"></div></div>
        <div class="chart-box"><div class="chart-head"><h4>Cola de decisiones operativas</h4></div><div class="chart-note">Visualiza el mix entre inspección, intervención, observación y escalado.</div><div class="chart-question">Pregunta: ¿qué tipo de decisión domina la cola?</div><div id="ch_decisions" class="svg-chart"></div></div>
      </div>
    </section>

    <section class="section explainer-block" id="sec_alertas">
      <div class="section-head">
        <div>
          <span class="eyebrow">Factores causales</span>
          <h3>Qué señales están empujando la situación actual</h3>
          <p>Inspección automática y drivers dominantes ayudan a separar anomalías, repetitividad, backlog y degradación.</p>
        </div>
      </div>
      <div class="grid2">
        <div class="chart-box"><div class="chart-head"><h4>Calidad de inspección por familia</h4></div><div class="chart-note">Cobertura y valor pre-falla para priorizar despliegue donde la detección añade más utilidad.</div><div class="chart-question">Pregunta: ¿la señal CBM llega antes de la falla?</div><div id="ch_inspection" class="svg-chart"></div></div>
        <div class="chart-box"><div class="chart-head"><h4>Drivers principales del riesgo</h4></div><div class="chart-note">Resume qué señales dominan la criticidad: anomalías, repetitividad o degradación.</div><div class="chart-question">Pregunta: ¿por qué sube el riesgo?</div><div id="ch_drivers" class="svg-chart"></div></div>
      </div>
    </section>

    <section class="section priority-block" id="sec_action">
      <div class="section-head">
        <div>
          <span class="eyebrow">Decisión</span>
          <h3>Qué hacer ahora</h3>
          <p>La prioridad anterior se traduce en la entrada a taller que debe ejecutarse primero y las señales que la sostienen.</p>
        </div>
      </div>
      <div class="command-panel">
        <div class="command-card critical">
          <h2>Acción inmediata</h2>
          <div class="big" id="exec_action" aria-live="polite">-</div>
          <p id="exec_action_note">-</p>
          <div class="decision-proof" aria-label="Criterios de decisión">
            <div class="proof-chip"><b>Score prioridad</b><span id="proof_priority">-</span></div>
            <div class="proof-chip"><b>Impacto servicio</b><span id="proof_service">-</span></div>
            <div class="proof-chip"><b>Salud/RUL</b><span id="proof_health">-</span></div>
            <div class="proof-chip"><b>Riesgo diferir</b><span id="proof_deferral">-</span></div>
          </div>
          <div hidden>
            <p><strong>Unidad que debe entrar primero:</strong> __TOP_UNIT__</p>
            <p><strong>Componente prioritario:</strong> __TOP_COMPONENT__</p>
          </div>
          <div class="decision-steps">
            <div class="step"><b>1. Entrar</b><span id="exec_step_unit">-</span></div>
            <div class="step"><b>2. Proteger</b><span id="exec_step_backlog">-</span></div>
            <div class="step"><b>3. No diferir</b><span id="exec_step_deferral">-</span></div>
          </div>
        </div>
        <div class="command-card">
          <h2>Estado operativo</h2>
          <p id="exec_state" aria-live="polite">-</p>
          <div class="decision-steps single">
            <div class="step"><b>Snapshot backlog</b><span id="exec_snapshot">-</span></div>
            <div class="step"><b>Bloqueo principal</b><span id="exec_bottleneck">-</span></div>
          </div>
        </div>
      </div>
      <div class="anomaly-grid" id="anomalyGrid"></div>
      <div class="interpretation">
        <div class="item"><b>Lectura de backlog</b><span id="interp_backlog">-</span></div>
        <div class="item"><b>Lectura de riesgo</b><span id="interp_risk">-</span></div>
        <div class="item"><b>Lectura de capacidad</b><span id="interp_capacity">-</span></div>
      </div>
    </section>

    <section class="section detail-block" id="sec_estrategica">
      <div class="section-head">
        <div>
          <span class="eyebrow">Escenarios y sensibilidad</span>
          <h3>Qué cambia al aplazar, replanificar o cambiar de estrategia</h3>
          <p>Estas vistas sirven para inspección profunda: escenarios, sensibilidad de diferimiento y capacidad de planificación.</p>
        </div>
      </div>
      <div class="insight" id="strategyInsight"></div>
      <div class="grid2">
        <div class="chart-box"><div class="chart-head"><h4>Reactivo vs Preventivo vs CBM</h4></div><div class="chart-note">Comparación de disponibilidad para leer la posición relativa de cada estrategia.</div><div class="chart-question">Pregunta: ¿qué estrategia protege más disponibilidad?</div><div id="ch_strategy" class="svg-chart"></div></div>
        <div class="chart-box"><div class="chart-head"><h4>Trade-off de diferimiento</h4></div><div class="chart-note">Evolución conjunta del coste y la indisponibilidad al aplazar intervención.</div><div class="chart-question">Pregunta: ¿cuánto cuesta aplazar?</div><div class="chart-legend"><span class="legend-chip"><span class="legend-dot" style="background:var(--c-danger)"></span>Coste</span><span class="legend-chip"><span class="legend-dot" style="background:var(--c-s1)"></span>Indisponibilidad</span></div><div id="ch_deferral" class="svg-chart"></div></div>
        <div class="chart-box"><div class="chart-head"><h4>Scheduling: baseline vs heurística rediseñada</h4></div><div class="chart-note">Mide si la cola se convierte en acción o queda bloqueada por capacidad/repuestos.</div><div class="chart-question">Pregunta: ¿la planificación reduce riesgo residual?</div><div id="ch_scheduling" class="svg-chart"></div></div>
      </div>
    </section>

    <section class="section detail-block" id="sec_tabela">
      <div class="section-head">
        <div>
          <span class="eyebrow">Inspección granular</span>
          <h3>Componentes, scores y trazabilidad caso a caso</h3>
          <p>Use la tabla para revisar distribuciones, segmentaciones y registros específicos después de entender la prioridad.</p>
        </div>
      </div>
      <div class="toolbar">
        <input id="searchBox" name="busqueda" autocomplete="off" aria-label="Buscar unidad, componente o driver" placeholder="Buscar unidad, componente, driver…" />
        <span class="count" id="resultCount" aria-live="polite">0 resultados</span>
        <div class="pager">
          <button type="button" class="btn" id="btnPrevPage">Anterior</button>
          <span class="page-info" id="pageInfo">Página 1/1</span>
          <button type="button" class="btn" id="btnNextPage">Siguiente</button>
          <label for="pageSize" class="pager-label">Filas
            <select id="pageSize" name="filas_por_pagina" autocomplete="off">
              <option value="50">50</option>
              <option value="100" selected>100</option>
              <option value="200">200</option>
            </select>
          </label>
        </div>
      </div>
      <div class="table-wrap">
        <table id="mainTable"><thead><tr id="tableHead"></tr></thead><tbody id="tableBody"></tbody></table>
      </div>
      <div class="footer-note">Dashboard autocontenido y listo para presentación. Los KPI superiores consumen métricas gobernadas; los filtros gobiernan gráficos, ranking y tabla sin dependencias externas.</div>
    </section>
  </main>
</div>
<button type="button" class="btn btn-top" id="btnTop" aria-label="Volver arriba">↑ Arriba</button>
<div id="chartTooltip" class="chart-tooltip" hidden></div>

<script>
const payload = __PAYLOAD__;
const baseRows = payload.rows.slice();
const fleetWeek = payload.fleet_week.slice();
const depotLatest = payload.depot_latest.slice();
const strategyData = payload.strategy.slice();
const deferralData = payload.deferral.slice();
const inspectionData = payload.inspection.slice();
const schedulingMetrics = payload.scheduling_metrics.slice();
const anomaliesData = payload.anomalies.slice();
const metricSnapshot = payload.metric_snapshot || {};
const meta = payload.meta || {};
const FILTERS_STATE_KEY = "dashboard_filters_collapsed_v2";

const filters = [
  ["f_flota", "flota_id"],
  ["f_unidad", "unidad_id"],
  ["f_deposito", "deposito_recomendado"],
  ["f_familia", "component_family"],
  ["f_sistema", "sistema_principal"],
  ["f_riesgo", "risk_level"],
  ["f_intervencion", "decision_type"],
  ["f_ventana", "time_window"],
  ["f_estrategia", "estrategia_mantenimiento_actual"]
];

let filteredRows = baseRows.slice();
let sortKey = null;
let sortAsc = false;
let currentPage = 1;
let pageSize = 100;

const tableColumns = [
  "unidad_id","componente_id","flota_id","deposito_recomendado","component_family","sistema_principal","risk_level",
  "decision_type","recommended_action_initial","time_window","intervention_priority_score","deferral_risk_score",
  "service_impact_score","workshop_fit_score","health_score","prob_fallo_30d","component_rul_estimate","main_risk_driver",
  "confidence_flag","estrategia_mantenimiento_actual"
];
const tableLabels = {
  unidad_id:"Unidad", componente_id:"Componente", flota_id:"Flota", deposito_recomendado:"Depósito recomendado",
  component_family:"Familia", sistema_principal:"Sistema", risk_level:"Nivel de riesgo", decision_type:"Decisión operacional",
  recommended_action_initial:"Acción recomendada", time_window:"Ventana", intervention_priority_score:"Prioridad",
  deferral_risk_score:"Riesgo de diferimiento", service_impact_score:"Impacto en servicio", workshop_fit_score:"Ajuste al taller",
  health_score:"Salud", prob_fallo_30d:"Riesgo de fallo 30d", component_rul_estimate:"RUL (días)", main_risk_driver:"Factor principal",
  confidence_flag:"Confianza", estrategia_mantenimiento_actual:"Estrategia"
};

function toNum(v){ const n = Number(v); return Number.isFinite(n) ? n : 0; }
function uniq(vals){ return ["Todos", ...Array.from(new Set(vals.filter(v => String(v).trim() !== ""))).sort()]; }
function fmt1(n){ return Number(n).toLocaleString("es-ES", {minimumFractionDigits:1, maximumFractionDigits:1}); }
function fmt2(n){ return Number(n).toLocaleString("es-ES", {minimumFractionDigits:2, maximumFractionDigits:2}); }
function fmt0(n){ return Math.round(Number(n)).toLocaleString("es-ES"); }
function fmtMoneyCompact(n){
  const value = Number(n);
  if(!Number.isFinite(value)) return "€0";
  const sign = value < 0 ? "−" : "";
  const abs = Math.abs(value);
  if(abs >= 1_000_000) return `${sign}€${fmt1(abs/1_000_000)}M`;
  if(abs >= 1_000) return `${sign}€${fmt0(abs/1_000)}k`;
  return `${sign}€${fmt0(abs)}`;
}
function mean(arr){ return arr.length ? arr.reduce((a,b)=>a+b,0)/arr.length : 0; }
function sum(arr){ return arr.reduce((a,b)=>a+b,0); }
function esc(v){
  return String(v ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function badge(level){
  const cls = level==="Critico"?"badge-critico":(level==="Alto"?"badge-alto":(level==="Medio"?"badge-medio":"badge-bajo"));
  return `<span class="badge ${cls}">${esc(level)}</span>`;
}

function setFiltersCollapsed(collapsed, persist){
  document.body.classList.toggle("filters-collapsed", collapsed);
  const btn = document.getElementById("btnFilters");
  if(btn){
    btn.textContent = collapsed ? "Mostrar filtros" : "Ocultar filtros";
    btn.setAttribute("aria-expanded", collapsed ? "false" : "true");
  }
  if(persist === false) return;
  try {
    window.localStorage.setItem(FILTERS_STATE_KEY, collapsed ? "1" : "0");
  } catch (_err) {
    // Ignorar errores de persistencia no críticos.
  }
}

function syncFloatingControls(){
  const btnTop = document.getElementById("btnTop");
  if(btnTop){
    btnTop.classList.toggle("visible", window.scrollY > 360);
  }
}

function renderFilterState(){
  const el = document.getElementById("filterState");
  if(!el) return;
  const active = filters
    .map(([id, key]) => {
      const value = activeFilterValue(id);
      if(value === "Todos") return null;
      return `${tableLabels[key] || key}: ${value}`;
    })
    .filter(Boolean);
  const search = document.getElementById("searchBox")?.value?.trim() || "";
  if(search){
    active.push(`Búsqueda: ${search}`);
  }
  if(active.length){
    el.textContent = `Filtros activos: ${active.join(" · ")}`;
    el.hidden = false;
  } else {
    el.textContent = "";
    el.hidden = true;
  }
}

function initFilters(){
  filters.forEach(([id,key]) => {
    const sel = document.getElementById(id);
    sel.innerHTML = uniq(baseRows.map(r => r[key])).map(v => `<option value="${v}">${v}</option>`).join("");
    sel.addEventListener("change", applyFilters);
  });
  document.getElementById("searchBox").addEventListener("input", applyFilters);
  document.getElementById("btnReset").addEventListener("click", resetFilters);
  document.getElementById("btnPrint").addEventListener("click", () => window.print());
  document.getElementById("btnTop").addEventListener("click", () => window.scrollTo({top:0, behavior:"smooth"}));
  document.getElementById("btnFilters").addEventListener("click", () => {
    const collapsed = !document.body.classList.contains("filters-collapsed");
    setFiltersCollapsed(collapsed);
  });
  document.getElementById("btnPrevPage").addEventListener("click", () => {
    if(currentPage > 1){ currentPage -= 1; renderAll(); }
  });
  document.getElementById("btnNextPage").addEventListener("click", () => {
    const totalPages = Math.max(1, Math.ceil(filteredRows.length / pageSize));
    if(currentPage < totalPages){ currentPage += 1; renderAll(); }
  });
  document.getElementById("pageSize").addEventListener("change", (ev) => {
    pageSize = Math.max(10, toNum(ev.target.value));
    currentPage = 1;
    renderAll();
  });
  let storedFilters = null;
  try { storedFilters = window.localStorage.getItem(FILTERS_STATE_KEY); } catch (_err) { storedFilters = null; }
  if(storedFilters === "1" || storedFilters === "0"){
    setFiltersCollapsed(storedFilters === "1");
  } else {
    const narrow = window.matchMedia && window.matchMedia("(max-width: 860px)").matches;
    setFiltersCollapsed(narrow, false);
  }
  syncFloatingControls();
}

function activeFilterValue(id){
  const el = document.getElementById(id);
  return el ? el.value : "Todos";
}

function applyFilters(){
  const search = document.getElementById("searchBox").value.toLowerCase().trim();
  filteredRows = baseRows.filter(r => {
    const passFilters = filters.every(([id,key]) => {
      const val = activeFilterValue(id);
      return val === "Todos" || String(r[key]) === String(val);
    });
    const passSearch = !search || Object.values(r).join(" ").toLowerCase().includes(search);
    return passFilters && passSearch;
  });
  currentPage = 1;
  document.getElementById("resultCount").textContent = `${filteredRows.length.toLocaleString("es-ES")} resultados`;
  renderFilterState();
  renderAll();
}

function resetFilters(){
  filters.forEach(([id]) => {
    const el = document.getElementById(id);
    if(el) el.value = "Todos";
  });
  const s = document.getElementById("searchBox");
  if(s) s.value = "";
  applyFilters();
}

function groupedMean(rows,key,val){
  const map = new Map();
  rows.forEach(r => {
    const k = String(r[key]);
    const arr = map.get(k) || [];
    arr.push(toNum(r[val]));
    map.set(k, arr);
  });
  return Array.from(map.entries()).map(([k,v]) => ({label:k, value:mean(v)}));
}

function groupedCount(rows,key){
  const map = new Map();
  rows.forEach(r => {
    const k = String(r[key]);
    map.set(k, (map.get(k)||0)+1);
  });
  return Array.from(map.entries()).map(([k,v]) => ({label:k, value:v}));
}

function rulBucketLabel(days){
  const d = toNum(days);
  if(d <= 14) return "<=14";
  if(d <= 30) return "15-30";
  if(d <= 60) return "31-60";
  if(d <= 90) return "61-90";
  if(d <= 180) return "91-180";
  return ">180";
}

function topN(rows, keyMetric, n=10){
  const byUnit = new Map();
  rows.forEach(r => {
    const u = String(r.unidad_id);
    const cur = byUnit.get(u) || [];
    cur.push(toNum(r[keyMetric]));
    byUnit.set(u, cur);
  });
  return Array.from(byUnit.entries())
    .map(([u,vals]) => ({label:u, value:mean(vals)}))
    .sort((a,b)=>b.value-a.value)
    .slice(0,n);
}

function filterFleetWeekByCurrentSelection(){
  const flotaSel = activeFilterValue("f_flota");
  if(flotaSel !== "Todos") return fleetWeek.filter(r => String(r.flota_id) === String(flotaSel));
  const selectedFlotas = new Set(filteredRows.map(r => String(r.flota_id)));
  return selectedFlotas.size ? fleetWeek.filter(r => selectedFlotas.has(String(r.flota_id))) : fleetWeek.slice();
}

function filterDepotByCurrentSelection(){
  const depSel = activeFilterValue("f_deposito");
  if(depSel !== "Todos") return depotLatest.filter(r => String(r.deposito_id) === String(depSel));
  const depSet = new Set(filteredRows.map(r => String(r.deposito_recomendado)));
  return depSet.size ? depotLatest.filter(r => depSet.has(String(r.deposito_id))) : depotLatest.slice();
}

function computeDerived(){
  const rows = filteredRows;
  const fleetSlice = filterFleetWeekByCurrentSelection();
  const depotSlice = filterDepotByCurrentSelection();

  const uniqueUnits = new Set(rows.map(r => String(r.unidad_id)));
  const highRiskUnits = new Set(rows.filter(r => toNum(r.intervention_priority_score)>=70).map(r => String(r.unidad_id)));
  const highDeferral = rows.filter(r => toNum(r.deferral_risk_score)>=70).length;

  const familyRisk = groupedMean(rows, "component_family", "prob_fallo_30d")
    .sort((a,b)=>b.value-a.value)
    .slice(0,8);
  const familyHealth = groupedMean(rows, "component_family", "health_score");
  const familyMapHealth = new Map(familyHealth.map(x => [x.label, x.value]));

  const rulBucket = groupedCount(
    rows.map(r => ({...r, rul_bucket: rulBucketLabel(r.component_rul_estimate)})),
    "rul_bucket"
  );
  const decisionDist = groupedCount(rows, "decision_type").sort((a,b)=>b.value-a.value);
  const driverDist = groupedCount(rows, "main_risk_driver").sort((a,b)=>b.value-a.value).slice(0,8);
  const topPriority = topN(rows, "intervention_priority_score", 10);
  const topService = topN(rows, "impact_on_service_proxy", 10);
  const priorityDeferral = rows
    .slice()
    .sort((a,b)=>(toNum(b.intervention_priority_score)+toNum(b.deferral_risk_score))-(toNum(a.intervention_priority_score)+toNum(a.deferral_risk_score)))
    .slice(0,180)
    .map(r => ({
      label:`${r.unidad_id}/${r.componente_id}`,
      x:toNum(r.intervention_priority_score),
      y:toNum(r.deferral_risk_score),
      size:Math.max(3, Math.min(10, 3 + toNum(r.service_impact_score)/18)),
      color:toNum(r.deferral_risk_score)>=70 ? cssVar("--c-danger","#b42318") : (toNum(r.intervention_priority_score)>=70 ? cssVar("--c-warning","#b45309") : cssVar("--c-s2","#64748b")),
      tip:`${r.unidad_id}/${r.componente_id} · Prioridad ${fmt1(r.intervention_priority_score)} · Diferimiento ${fmt1(r.deferral_risk_score)} · Servicio ${fmt1(r.service_impact_score)}`
    }));

  const avgAvail = mean(fleetSlice.map(r => toNum(r.availability_rate))) * 100;
  const avgMtbf = mean(fleetSlice.map(r => toNum(r.mtbf_proxy)));
  const avgMttr = mean(fleetSlice.map(r => toNum(r.mttr_proxy)));

  const bf = sum(depotSlice.map(r => toNum(r.backlog_physical_items)));
  const bv = sum(depotSlice.map(r => toNum(r.backlog_overdue_items)));
  const bcf = sum(depotSlice.map(r => toNum(r.backlog_critical_items)));
  const bea = mean(depotSlice.map(r => toNum(r.backlog_exposure_adjusted_score)));
  const sat = mean(depotSlice.map(r => toNum(r.saturation_ratio))) * 100;

  const strategySel = activeFilterValue("f_estrategia");
  const strategySlice = strategySel === "Todos"
    ? strategyData.slice()
    : strategyData.filter(r => String(r.estrategia) === String(strategySel) || String(r.estrategia_mantenimiento_actual||"") === String(strategySel));
  const cbm = strategyData.find(r => String(r.estrategia) === "basada_en_condicion");
  const react = strategyData.find(r => String(r.estrategia) === "reactiva");
  const savings = react && cbm ? (toNum(react.coste_operativo_proxy)-toNum(cbm.coste_operativo_proxy)) : 0;

  return {
    rows, uniqueUnits, highRiskUnits, highDeferral,
    familyRisk, familyMapHealth, rulBucket, decisionDist, driverDist, topPriority, topService,
    priorityDeferral, avgAvail, avgMtbf, avgMttr, bf, bv, bcf, bea, sat, strategySlice, savings, depotSlice
  };
}

function setText(id, txt){ const el = document.getElementById(id); if(el) el.textContent = txt; }

function renderKPIs(d){
  const avail = toNum(metricSnapshot.fleet_availability_pct);
  const mtbf = toNum(metricSnapshot.mtbf_proxy_hours);
  const mttr = toNum(metricSnapshot.mttr_proxy_hours);
  const highPriorityUnits = d.highRiskUnits.size;
  const backlogPhysical = toNum(metricSnapshot.backlog_physical_items_count);
  const backlogOverdue = toNum(metricSnapshot.backlog_overdue_items_count);
  const backlogCritical = toNum(metricSnapshot.backlog_critical_physical_count);
  const deferralHigh = toNum(metricSnapshot.high_deferral_risk_cases_count);
  const exposure = toNum(metricSnapshot.backlog_exposure_adjusted_mean);
  const downtimeDeferral14d = toNum(metricSnapshot.deferral_downtime_delta_14d_h);
  const avoidableCorrectives = toNum(metricSnapshot.avoidable_correctives_inspection);
  const savings = toNum(metricSnapshot.cbm_operational_savings_eur);
  const alerts = toNum(metricSnapshot.early_warnings_active_count);
  const sat = toNum(metricSnapshot.mean_depot_saturation_pct);
  const topDepot = metricSnapshot.top_depot_by_saturation || "n/a";
  const topDepotSat = toNum(metricSnapshot.top_depot_saturation_pct);
  const probPositive = toNum(metricSnapshot.cbm_prob_positive_savings) * 100;

  setText("k_avail", `${fmt1(avail)}%`);
  setText("k_mtbf", fmt0(mtbf));
  setText("k_mttr", fmt1(mttr));
  setText("k_uhr", fmt0(highPriorityUnits));
  setText("k_bf", fmt0(backlogPhysical));
  setText("k_bv", fmt0(backlogOverdue));
  setText("k_bcf", fmt0(backlogCritical));
  setText("k_drh", fmt0(deferralHigh));
  setText("k_bea", fmt1(exposure));
  setText("k_he", `${fmt1(downtimeDeferral14d)} h`);
  setText("k_ce", fmt0(avoidableCorrectives));
  setText("k_ahorro", fmtMoneyCompact(savings));
  setText("k_alertas", fmt0(alerts));
  setText("k_sat", `${fmt0(sat)}%`);

  // Disponibilidad: delta firmado frente al objetivo operativo del 90%.
  const availTarget = 90;
  const availDelta = avail - availTarget;
  const availEl = document.getElementById("k_avail_delta");
  if(availEl){
    const up = availDelta >= 0;
    availEl.className = `delta ${up ? "up" : "down"}`;
    availEl.textContent = `${up ? "▲" : "▼"} ${up ? "+" : "−"}${fmt1(Math.abs(availDelta))} pp vs objetivo 90%`;
  }

  // Backlog crítico: cuota sobre el backlog físico para leer la severidad de la cola.
  const criticalShare = backlogPhysical > 0 ? (backlogCritical / backlogPhysical) * 100 : 0;
  setText("k_bcf_sub", `${fmt0(criticalShare)}% del backlog físico es crítico`);

  // Saturación: contexto del depósito más tensionado.
  setText("k_sat_sub", topDepot && topDepot !== "n/a" ? `máx. ${fmt0(topDepotSat)}% · ${topDepot}` : "capacidad usada");

  // Diferencial CBM: el color y el signo deben coincidir (verde=ahorro, rojo=coste).
  const valueCard = document.querySelector(".card.primary.value");
  if(valueCard){
    valueCard.classList.toggle("pos", savings >= 0);
    valueCard.classList.toggle("neg", savings < 0);
  }
  setText("k_ahorro_sub", savings >= 0
    ? "Ahorro operativo proxy frente a reactivo."
    : "Sobrecoste operativo proxy frente a reactivo.");
  setText("s_count_rows", fmt0(d.rows.length));
  setText("s_count_units", fmt0(d.uniqueUnits.size));
  setText("s_count_high", fmt0(d.highRiskUnits.size));
}

function renderActionPanel(d){
  const top = d.rows.slice().sort((a,b)=>toNum(b.intervention_priority_score)-toNum(a.intervention_priority_score))[0];
  const backlogCritical = toNum(metricSnapshot.backlog_critical_physical_count);
  const backlogOverdue = toNum(metricSnapshot.backlog_overdue_items_count);
  const backlogPhysical = toNum(metricSnapshot.backlog_physical_items_count);
  const deferralHigh = toNum(metricSnapshot.high_deferral_risk_cases_count);
  const pendingCapacity = schedulingMetrics.find(r => String(r.scenario)==="heuristica_redisenada_35d");
  const pendingCapacityPct = pendingCapacity ? toNum(pendingCapacity.pendiente_capacidad_pct) : 0;
  const actionablePct = pendingCapacity ? toNum(pendingCapacity.actionable_pct) : 0;
  if(top){
    setText("exec_action", `${top.unidad_id} / ${top.componente_id}`);
    setText("exec_action_note", `Enviar a ${top.deposito_recomendado} en la próxima ventana. ${top.decision_type}; prioridad ${fmt1(top.intervention_priority_score)}, impacto servicio ${fmt1(top.service_impact_score)}, salud ${fmt1(top.health_score)} y diferimiento ${fmt1(top.deferral_risk_score)}.`);
    setText("exec_step_unit", `${top.unidad_id} · ${top.deposito_recomendado}`);
    setText("proof_priority", `${fmt1(top.intervention_priority_score)} / 100`);
    setText("proof_service", `${fmt1(top.service_impact_score)} impacto`);
    setText("proof_health", `${fmt1(top.health_score)} salud / ${fmt1(top.component_rul_estimate)}d RUL`);
    setText("proof_deferral", `${fmt1(top.deferral_risk_score)} / 100`);
  } else {
    setText("exec_action", "Sin decisión activa");
    setText("exec_action_note", "Ajusta filtros para recuperar una recomendación.");
    setText("exec_step_unit", "-");
    setText("proof_priority", "-");
    setText("proof_service", "-");
    setText("proof_health", "-");
    setText("proof_deferral", "-");
  }
  setText("exec_step_backlog", `${fmt0(backlogCritical)} críticos / ${fmt0(backlogPhysical)} físicos`);
  setText("exec_step_deferral", `${fmt0(deferralHigh)} casos alto riesgo`);
  setText("exec_state", `Crisis activa: ${fmt1(backlogPhysical ? backlogOverdue/backlogPhysical*100 : 0)}% del backlog físico está vencido. La heurística deja ${fmt1(pendingCapacityPct)}% pendiente por capacidad y convierte ${fmt1(actionablePct)}% de casos en acción.`);
  const snapshotLabel = meta.latest_depot_valid_date
    ? `${meta.latest_depot_valid_date} válido${meta.latest_depot_calendar_zero_backlog ? " · calendario reciente sin carga física" : ""}`
    : "n/a";
  setText("exec_snapshot", snapshotLabel);
  setText("exec_bottleneck", pendingCapacityPct >= 40 ? "Capacidad de taller" : "Repuestos / secuenciación");
  setText("interp_backlog", `${fmt0(backlogOverdue)} de ${fmt0(backlogPhysical)} pendientes están vencidos; la cola es una crisis física, no solo un score de riesgo.`);
  setText("interp_risk", `${fmt0(deferralHigh)} componentes cruzan umbral de diferimiento alto; son los casos que no deben aplazarse sin decisión explícita.`);
  setText("interp_capacity", `${fmt1(pendingCapacityPct)}% queda pendiente por capacidad tras rediseñar scheduling; el cuello de botella es operativo, no de visualización.`);
}

function renderAnomalies(){
  const grid = document.getElementById("anomalyGrid");
  if(!grid) return;
  grid.innerHTML = anomaliesData.map(a => `
    <div class="anomaly ${esc(a.severity)}">
      <div class="label">${esc(a.title)}</div>
      <div class="value">${esc(a.value)}</div>
      <div class="text">${esc(a.description)}</div>
    </div>
  `).join("");
}

function makeSvg(containerId){
  const el = document.getElementById(containerId);
  if(!el) return null;
  const w = el.clientWidth || 420;
  const h = el.clientHeight || 240;
  const label = el.closest(".chart-box")?.querySelector("h4")?.textContent || containerId;
  el.innerHTML = `<svg width="${w}" height="${h}" viewBox="0 0 ${w} ${h}" role="img" aria-label="${esc(label)}"></svg>`;
  return el.querySelector("svg");
}

function bindSvgTooltip(svg){
  const tip = document.getElementById("chartTooltip");
  if(!svg || !tip) return;
  const targets = svg.querySelectorAll("[data-tip]");
  targets.forEach(node => {
    node.style.cursor = "pointer";
    node.addEventListener("mousemove", (ev) => {
      const txt = node.getAttribute("data-tip");
      if(!txt) return;
      tip.hidden = false;
      tip.textContent = txt;
      tip.style.left = `${ev.clientX + 12}px`;
      tip.style.top = `${ev.clientY + 12}px`;
      node.style.opacity = "0.82";
    });
    node.addEventListener("mouseleave", () => {
      tip.hidden = true;
      node.style.opacity = "1";
    });
  });
}

function cssVar(name, fallback){
  const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return v || fallback;
}
function chartInk(){
  return {
    grid: cssVar("--c-grid", "#ececed"),
    axis: cssVar("--c-axis", "#a1a1aa"),
    ink: cssVar("--c-ink", "#3f3f46"),
    empty: cssVar("--muted", "#71717a"),
    card: cssVar("--card", "#ffffff"),
    s1: cssVar("--c-s1", "#2f6ae4"),
    s2: cssVar("--c-s2", "#64748b"),
    danger: cssVar("--c-danger", "#b42318"),
    warning: cssVar("--c-warning", "#b45309"),
    positive: cssVar("--c-positive", "#15803d"),
    neutral: cssVar("--c-neutral", "#94a3b8"),
    criticalLine: cssVar("--critical-line", "#f3cfc9")
  };
}

function drawBars(containerId, items, color, valueFmt=(v)=>fmt1(v), showLabels=true, baseline=0){
  const svg = makeSvg(containerId);
  if(!svg) return;
  const CK = chartInk();
  if(!color) color = CK.s1;
  const w = svg.viewBox.baseVal.width, h = svg.viewBox.baseVal.height;
  const m = {l:40,r:10,t:10,b:48};
  const iw = w-m.l-m.r, ih = h-m.t-m.b;
  if(!items.length){
    svg.innerHTML = `<text x="${w/2}" y="${h/2}" text-anchor="middle" fill="${CK.empty}" font-size="12">Sin datos para filtros</text>`;
    return;
  }
  const maxV = Math.max(...items.map(d=>toNum(d.value)), baseline + 1);
  const span = (maxV - baseline) || 1;
  const bw = Math.max(6, iw/items.length - 4);
  const dense = items.length > 8;
  const showValue = !dense;
  const labelStep = items.length > 14 ? Math.ceil(items.length / 7) : 1;
  const labelRotation = items.length > 7 ? 28 : 0;
  let out = `<line x1="${m.l}" y1="${m.t+ih}" x2="${m.l+iw}" y2="${m.t+ih}" stroke="${CK.axis}"/>`;
  [0.25,0.5,0.75].forEach(p => {
    const yy = m.t + ih - ih*p;
    out += `<line x1="${m.l}" y1="${yy}" x2="${m.l+iw}" y2="${yy}" stroke="${CK.grid}" />`;
  });
  if(baseline > 0){
    out += `<text x="${m.l-6}" y="${m.t+ih+3}" text-anchor="end" fill="${CK.axis}" font-size="10">${valueFmt(baseline)}</text>`;
  }
  items.forEach((d,i)=>{
    const v = toNum(d.value);
    const bh = ih * (Math.max(0, v - baseline)/span);
    const x = m.l + i*(iw/items.length) + 2;
    const y = m.t + ih - bh;
    out += `<rect x="${x}" y="${y}" width="${bw}" height="${bh}" fill="${color}" rx="2" data-tip="${esc(d.label)}: ${valueFmt(v)}"></rect>`;
    if(showValue){
      out += `<text x="${x+bw/2}" y="${y-3}" text-anchor="middle" fill="${CK.ink}" font-size="10">${valueFmt(v)}</text>`;
    }
    if(showLabels && (i % labelStep === 0 || i === items.length - 1)){
      const lbl = esc(String(d.label).slice(0,12));
      if(labelRotation){
        out += `<text x="${x+bw/2}" y="${m.t+ih+12}" text-anchor="middle" fill="${CK.axis}" font-size="10" transform="rotate(${labelRotation} ${x+bw/2},${m.t+ih+12})">${lbl}</text>`;
      } else {
        out += `<text x="${x+bw/2}" y="${m.t+ih+12}" text-anchor="middle" fill="${CK.axis}" font-size="10">${lbl}</text>`;
      }
    }
  });
  svg.innerHTML = out;
  bindSvgTooltip(svg);
}

function drawDualBars(containerId, labels, v1, v2, c1, c2){
  const items1 = labels.map((l,i)=>({label:l, value:toNum(v1[i])}));
  const items2 = labels.map((l,i)=>({label:l, value:toNum(v2[i])}));
  const svg = makeSvg(containerId); if(!svg) return;
  const CK = chartInk();
  c1 = c1 || CK.danger; c2 = c2 || CK.positive;
  const w = svg.viewBox.baseVal.width, h = svg.viewBox.baseVal.height;
  const m = {l:40,r:10,t:34,b:56}; const iw=w-m.l-m.r, ih=h-m.t-m.b;
  if(!labels.length){ svg.innerHTML=`<text x="${w/2}" y="${h/2}" text-anchor="middle" fill="${CK.empty}" font-size="12">Sin datos para filtros</text>`; return; }
  const maxV = Math.max(...items1.map(d=>d.value), ...items2.map(d=>d.value), 1);
  const slot = iw/labels.length, bw = Math.max(6,(slot-6)/2);
  let out = `<line x1="${m.l}" y1="${m.t+ih}" x2="${m.l+iw}" y2="${m.t+ih}" stroke="${CK.axis}"/>`;
  items1.forEach((d,i)=>{
    const x0 = m.l + i*slot + 2;
    const h1 = ih*(d.value/maxV), y1 = m.t+ih-h1;
    const h2 = ih*(items2[i].value/maxV), y2 = m.t+ih-h2;
    out += `<rect x="${x0}" y="${y1}" width="${bw}" height="${h1}" fill="${c1}" rx="2" data-tip="${esc(d.label)} · Riesgo: ${fmt1(d.value)}"/>`;
    out += `<rect x="${x0+bw+2}" y="${y2}" width="${bw}" height="${h2}" fill="${c2}" rx="2" data-tip="${esc(d.label)} · Salud: ${fmt1(items2[i].value)}"/>`;
    const lbl = esc(String(d.label).slice(0,12));
    out += `<text x="${x0+bw}" y="${m.t+ih+12}" text-anchor="middle" fill="${CK.axis}" font-size="10" transform="rotate(24 ${x0+bw},${m.t+ih+12})">${lbl}</text>`;
  });
  svg.innerHTML = out;
  bindSvgTooltip(svg);
}

function drawLine(containerId, xVals, y1, y2, c1, c2){
  const svg = makeSvg(containerId); if(!svg) return;
  const CK = chartInk();
  c1 = c1 || CK.danger; c2 = c2 || CK.s1;
  const w = svg.viewBox.baseVal.width, h = svg.viewBox.baseVal.height;
  const m = {l:40,r:10,t:12,b:36}; const iw=w-m.l-m.r, ih=h-m.t-m.b;
  if(!xVals.length){ svg.innerHTML=`<text x="${w/2}" y="${h/2}" text-anchor="middle" fill="${CK.empty}" font-size="12">Sin datos</text>`; return; }
  // Escala independiente por serie: el gráfico compara la forma (ambos costes crecen al diferir),
  // no magnitudes en un eje común. Cada serie se normaliza con base en 0 para preservar el origen.
  const rangeOf = (arr)=>{ const mx=Math.max(...arr.map(toNum),0); const mn=Math.min(...arr.map(toNum),0); return [mn, mx-mn || 1]; };
  const [n1, d1] = rangeOf(y1); const [n2, d2] = rangeOf(y2);
  const scaleY1 = (v)=> m.t + ih - ((toNum(v)-n1)/d1)*ih*0.92;
  const scaleY2 = (v)=> m.t + ih - ((toNum(v)-n2)/d2)*ih*0.92;
  const scaleX = (i)=> m.l + (xVals.length===1? iw/2 : i*(iw/(xVals.length-1)));
  const path = (arr,sy)=> arr.map((v,i)=>`${i===0?'M':'L'} ${scaleX(i)} ${sy(v)}`).join(" ");
  let out = `<line x1="${m.l}" y1="${m.t+ih}" x2="${m.l+iw}" y2="${m.t+ih}" stroke="${CK.axis}"/>`;
  out += `<path d="${path(y1,scaleY1)}" fill="none" stroke="${c1}" stroke-width="2.2"/>`;
  out += `<path d="${path(y2,scaleY2)}" fill="none" stroke="${c2}" stroke-width="2.2"/>`;
  y1.forEach((v,i)=>{ out += `<circle cx="${scaleX(i)}" cy="${scaleY1(v)}" r="3" fill="${c1}" data-tip="Ventana ${esc(xVals[i])}d · Coste: ${fmt2(v)}M€"/>`; });
  y2.forEach((v,i)=>{ out += `<circle cx="${scaleX(i)}" cy="${scaleY2(v)}" r="3" fill="${c2}" data-tip="Ventana ${esc(xVals[i])}d · Indisponibilidad: ${fmt1(v)}h"/>`; });
  const tickStep = Math.max(1, Math.ceil(xVals.length / 6));
  xVals.forEach((x,i)=>{ if(i%tickStep===0 || i===xVals.length-1){ out += `<text x="${scaleX(i)}" y="${m.t+ih+12}" text-anchor="middle" fill="${CK.axis}" font-size="10">${esc(x)}</text>`; }});
  svg.innerHTML = out;
  bindSvgTooltip(svg);
}

function drawScatter(containerId, points){
  const svg = makeSvg(containerId); if(!svg) return;
  const CK = chartInk();
  const w = svg.viewBox.baseVal.width, h = svg.viewBox.baseVal.height;
  const m = {l:44,r:16,t:14,b:38}; const iw=w-m.l-m.r, ih=h-m.t-m.b;
  if(!points.length){ svg.innerHTML=`<text x="${w/2}" y="${h/2}" text-anchor="middle" fill="${CK.empty}" font-size="12">Sin datos para filtros</text>`; return; }
  const xMin = Math.min(...points.map(p=>p.x), 0), xMax = Math.max(...points.map(p=>p.x), 100);
  const yMin = Math.min(...points.map(p=>p.y), 0), yMax = Math.max(...points.map(p=>p.y), 100);
  const sx = v => m.l + ((toNum(v)-xMin)/(xMax-xMin || 1))*iw;
  const sy = v => m.t + ih - ((toNum(v)-yMin)/(yMax-yMin || 1))*ih;
  let out = `<line x1="${m.l}" y1="${m.t+ih}" x2="${m.l+iw}" y2="${m.t+ih}" stroke="${CK.axis}"/>`;
  out += `<line x1="${m.l}" y1="${m.t}" x2="${m.l}" y2="${m.t+ih}" stroke="${CK.axis}"/>`;
  const x70 = sx(70), y70 = sy(70);
  out += `<line x1="${x70}" y1="${m.t}" x2="${x70}" y2="${m.t+ih}" stroke="${CK.criticalLine}" stroke-dasharray="4 4"/>`;
  out += `<line x1="${m.l}" y1="${y70}" x2="${m.l+iw}" y2="${y70}" stroke="${CK.criticalLine}" stroke-dasharray="4 4"/>`;
  out += `<text x="${m.l+iw}" y="${m.t+12}" text-anchor="end" fill="${CK.danger}" font-size="10">alta prioridad + no diferir</text>`;
  points.forEach(p => {
    out += `<circle cx="${sx(p.x)}" cy="${sy(p.y)}" r="${p.size}" fill="${p.color}" fill-opacity=".78" stroke="${CK.card}" stroke-width="1" data-tip="${esc(p.tip)}"></circle>`;
  });
  out += `<text x="${m.l+iw}" y="${h-8}" text-anchor="end" fill="${CK.axis}" font-size="10">Prioridad intervención</text>`;
  out += `<text x="12" y="${m.t+10}" fill="${CK.axis}" font-size="10" transform="rotate(-90 12,${m.t+10})">Riesgo diferimiento</text>`;
  svg.innerHTML = out;
  bindSvgTooltip(svg);
}

function drawBacklogBars(containerId, rows){
  const svg = makeSvg(containerId); if(!svg) return;
  const CK = chartInk();
  const w = svg.viewBox.baseVal.width, h = svg.viewBox.baseVal.height;
  const m = {l:42,r:14,t:12,b:48}; const iw=w-m.l-m.r, ih=h-m.t-m.b;
  const items = rows.slice().sort((a,b)=>toNum(b.backlog_critical_items)-toNum(a.backlog_critical_items)).slice(0,10);
  if(!items.length){ svg.innerHTML=`<text x="${w/2}" y="${h/2}" text-anchor="middle" fill="${CK.empty}" font-size="12">Sin backlog válido</text>`; return; }
  const maxV = Math.max(...items.flatMap(r => [toNum(r.backlog_physical_items), toNum(r.backlog_overdue_items), toNum(r.backlog_critical_items)]), 1);
  const slot = iw/items.length;
  const bw = Math.max(5, (slot-8)/3);
  let out = `<line x1="${m.l}" y1="${m.t+ih}" x2="${m.l+iw}" y2="${m.t+ih}" stroke="${CK.axis}"/>`;
  items.forEach((r,i)=>{
    const x = m.l + i*slot + 3;
    const vals = [
      ["Físico", toNum(r.backlog_physical_items), CK.s2],
      ["Vencido", toNum(r.backlog_overdue_items), CK.warning],
      ["Crítico", toNum(r.backlog_critical_items), CK.danger],
    ];
    vals.forEach(([name,val,color],j)=>{
      const bh = ih*(val/maxV);
      const y = m.t+ih-bh;
      out += `<rect x="${x+j*(bw+2)}" y="${y}" width="${bw}" height="${bh}" fill="${color}" rx="2" data-tip="${esc(r.deposito_id)} · ${name}: ${fmt0(val)}"></rect>`;
    });
    out += `<text x="${x+bw}" y="${m.t+ih+12}" text-anchor="middle" fill="${CK.axis}" font-size="10">${esc(r.deposito_id)}</text>`;
  });
  out += `<text x="${m.l+iw}" y="${m.t+10}" text-anchor="end" fill="${CK.axis}" font-size="10">Snapshot ${esc(meta.latest_depot_valid_date || "")}</text>`;
  svg.innerHTML = out;
  bindSvgTooltip(svg);
}

function drawSchedulingBars(containerId){
  const scenarios = schedulingMetrics.map(r => ({
    label:String(r.scenario).replace("heuristica_redisenada_35d","rediseñada").replace("baseline_greedy_21d","baseline"),
    programada:toNum(r.programadas_pct),
    programable:toNum(r.programables_proxima_ventana_pct),
    pendiente:toNum(r.pendientes_total_pct),
    residual:toNum(r.riesgo_residual_no_atendido_pct)
  }));
  const svg = makeSvg(containerId); if(!svg) return;
  const CK = chartInk();
  const w = svg.viewBox.baseVal.width, h = svg.viewBox.baseVal.height;
  const m = {l:42,r:14,t:34,b:42}; const iw=w-m.l-m.r, ih=h-m.t-m.b;
  if(!scenarios.length){ svg.innerHTML=`<text x="${w/2}" y="${h/2}" text-anchor="middle" fill="${CK.empty}" font-size="12">Sin datos de scheduling</text>`; return; }
  const slot = iw/scenarios.length, bw = Math.min(84, slot*.46);
  let out = `<line x1="${m.l}" y1="${m.t+ih}" x2="${m.l+iw}" y2="${m.t+ih}" stroke="${CK.axis}"/>`;
  scenarios.forEach((s,i)=>{
    const x = m.l+i*slot+slot/2-bw/2;
    let y = m.t+ih;
    [["programada",s.programada,CK.positive],["programable",s.programable,CK.s1],["pendiente",s.pendiente,CK.danger]].forEach(([name,val,color])=>{
      const bh = ih*(val/100);
      y -= bh;
      out += `<rect x="${x}" y="${y}" width="${bw}" height="${bh}" fill="${color}" data-tip="${esc(s.label)} · ${name}: ${fmt1(val)}%"></rect>`;
    });
    out += `<circle cx="${x+bw+13}" cy="${m.t+ih-ih*(s.residual/100)}" r="4" fill="${CK.ink}" data-tip="${esc(s.label)} · riesgo residual no atendido: ${fmt1(s.residual)}%"></circle>`;
    out += `<text x="${x+bw/2}" y="${m.t+ih+14}" text-anchor="middle" fill="${CK.axis}" font-size="10">${esc(s.label)}</text>`;
  });
  let lx = m.l;
  [["programada",CK.positive],["programable",CK.s1],["pendiente",CK.danger],["riesgo residual",CK.ink]].forEach(([name,color],i)=>{
    if(i===3){ out += `<circle cx="${lx+4}" cy="11" r="4" fill="${color}"/>`; }
    else { out += `<rect x="${lx}" y="7" width="8" height="8" rx="1.5" fill="${color}"/>`; }
    out += `<text x="${lx+13}" y="14" fill="${CK.ink}" font-size="10">${name}</text>`;
    lx += 13 + name.length*5.7 + 16;
  });
  svg.innerHTML = out;
  bindSvgTooltip(svg);
}

function renderInsights(d){
  const cbm = strategyData.find(r => String(r.estrategia)==="basada_en_condicion");
  const react = strategyData.find(r => String(r.estrategia)==="reactiva");
  const savings = react&&cbm ? toNum(react.coste_operativo_proxy)-toNum(cbm.coste_operativo_proxy) : 0;
  const prob = cbm ? toNum(cbm.prob_ahorro_positivo)*100 : 0;
  const rmin = cbm ? toNum(cbm.rango_plausible_valor_min)/1e6 : 0;
  const rmax = cbm ? toNum(cbm.rango_plausible_valor_max)/1e6 : 0;
  const valueReading = savings >= 0
    ? `preserva valor por ~${fmt0(savings)} EUR`
    : `presenta un coste incremental de ~${fmt0(Math.abs(savings))} EUR`;
  document.getElementById("strategyInsight").textContent =
    `Lectura estratégica: CBM vs reactiva ${valueReading} en escenario base, con probabilidad de ahorro positivo del ${fmt1(prob)}% y rango plausible de ${fmt2(rmin)}M€ a ${fmt2(rmax)}M€.`;
  const topUnit = metricSnapshot.top_unit_by_priority || "n/a";
  const topComp = metricSnapshot.top_component_by_priority || "n/a";
  document.getElementById("headerInsight").innerHTML =
    `Prioridad nº1: <b>${esc(topUnit)} · ${esc(topComp)}</b>. ${fmt0(d.highRiskUnits.size)} unidades con prioridad ≥70 compiten por entrada a taller en la vista actual.`;
}

function renderCharts(d){
  const CK = chartInk();
  const labelsFam = d.familyRisk.map(x=>x.label);
  const riskVals = d.familyRisk.map(x=>x.value*100);
  const healthVals = labelsFam.map(l => d.familyMapHealth.get(l) || 0);
  drawDualBars("ch_family", labelsFam, riskVals, healthVals);

  const rulOrder = {"<=14":0,"15-30":1,"31-60":2,"61-90":3,"91-180":4,">180":5};
  drawBars("ch_rul", d.rulBucket.sort((a,b)=>(rulOrder[a.label] ?? 99) - (rulOrder[b.label] ?? 99)).map(x=>({label:x.label,value:x.value})), CK.neutral, (v)=>fmt0(v), true);
  drawBars("ch_top_units", d.topPriority, CK.danger, (v)=>fmt1(v), true);
  drawBars("ch_service", d.topService, CK.warning, (v)=>fmt1(v), true);
  drawScatter("ch_priority_deferral", d.priorityDeferral);

  const depotBars = d.depotSlice.map(x => ({label:String(x.deposito_id), value:toNum(x.saturation_ratio)*100}));
  drawBacklogBars("ch_backlog_depot", d.depotSlice);
  drawBars("ch_depot", depotBars.sort((a,b)=>b.value-a.value).slice(0,10), CK.s1, (v)=>fmt1(v), true);
  drawBars("ch_decisions", d.decisionDist, CK.positive, (v)=>fmt0(v), true);
  drawBars("ch_drivers", d.driverDist, CK.s2, (v)=>fmt0(v), true);

  const insp = inspectionData.map(r => ({label:String(r.family||r.familia||"family"), value:toNum(r.pre_failure_detection_rate||r.coverage_pre_falla||0)}));
  drawBars("ch_inspection", insp, CK.positive, (v)=>fmt2(v), true);

  const stratName = {basada_en_condicion:"CBM", preventiva_rigida:"Preventiva", reactiva:"Reactiva"};
  const stratBars = d.strategySlice.map(r => ({label:stratName[String(r.estrategia)] || String(r.estrategia), value:toNum(r.fleet_availability)}));
  const stratMin = Math.min(...stratBars.map(b=>toNum(b.value)), 100);
  const stratFloor = Math.max(0, Math.floor((stratMin - 1.5) * 2) / 2);
  drawBars("ch_strategy", stratBars, CK.s1, (v)=>fmt1(v), true, stratFloor);

  drawLine(
    "ch_deferral",
    deferralData.map(r=>String(r.defer_dias)),
    deferralData.map(r=>toNum(r.costo_total_eur)/1e6),
    deferralData.map(r=>toNum(r.downtime_total_h))
  );
  drawSchedulingBars("ch_scheduling");
}

function renderTable(d){
  const head = document.getElementById("tableHead");
  const body = document.getElementById("tableBody");
  if(!head.dataset.ready){
    head.innerHTML = tableColumns.map(c => `<th scope="col" data-col="${c}">${tableLabels[c] || c}</th>`).join("");
    head.querySelectorAll("th").forEach(th => {
      th.addEventListener("click", () => {
        const c = th.dataset.col;
        if(sortKey===c){ sortAsc=!sortAsc; } else { sortKey=c; sortAsc=true; }
        filteredRows.sort((a,b)=>{
          const va=a[c], vb=b[c];
          const na=toNum(va), nb=toNum(vb);
          const bothNum = !(Number.isNaN(na)||Number.isNaN(nb));
          if(bothNum) return sortAsc ? na-nb : nb-na;
          return sortAsc ? String(va).localeCompare(String(vb)) : String(vb).localeCompare(String(va));
        });
        renderAll();
      });
    });
    head.dataset.ready = "1";
  }
  if(!d.rows.length){
    body.innerHTML = `<tr><td colspan="${tableColumns.length}" class="empty-row">Sin resultados para los filtros actuales</td></tr>`;
    const info = document.getElementById("pageInfo");
    if(info) info.textContent = "Página 0/0 · 0 filas";
    const prev = document.getElementById("btnPrevPage");
    const next = document.getElementById("btnNextPage");
    if(prev) prev.disabled = true;
    if(next) next.disabled = true;
    return;
  }
  const totalPages = Math.max(1, Math.ceil(d.rows.length / pageSize));
  currentPage = Math.min(currentPage, totalPages);
  const start = (currentPage - 1) * pageSize;
  const pageRows = d.rows.slice(start, start + pageSize);

  const numCols = new Set(["intervention_priority_score","deferral_risk_score","service_impact_score","workshop_fit_score","health_score","prob_fallo_30d","component_rul_estimate"]);
  body.innerHTML = pageRows.map(r => `
    <tr>${tableColumns.map(c => c==="risk_level" ? `<td>${badge(r[c])}</td>` : `<td${numCols.has(c)?' class="num"':''}>${esc(r[c])}</td>`).join("")}</tr>
  `).join("");

  const info = document.getElementById("pageInfo");
  if(info){
    info.textContent = `Página ${currentPage}/${totalPages} · ${fmt0(d.rows.length)} filas`;
  }
  const prev = document.getElementById("btnPrevPage");
  const next = document.getElementById("btnNextPage");
  if(prev) prev.disabled = currentPage <= 1;
  if(next) next.disabled = currentPage >= totalPages;
}

function renderAll(){
  const d = computeDerived();
  renderKPIs(d);
  renderActionPanel(d);
  renderAnomalies();
  renderInsights(d);
  renderCharts(d);
  renderTable(d);
}

const THEME_KEY = "dashboard_theme_v1";
function applyTheme(theme){
  document.documentElement.setAttribute("data-theme", theme);
  const btn = document.getElementById("btnTheme");
  if(btn){
    const dark = theme === "dark";
    btn.textContent = dark ? "○" : "●";
    btn.setAttribute("aria-label", dark ? "Activar tema claro" : "Activar tema oscuro");
    btn.setAttribute("title", dark ? "Tema claro" : "Tema oscuro");
  }
}
function initTheme(){
  let theme = null;
  try { theme = window.localStorage.getItem(THEME_KEY); } catch(_e){ theme = null; }
  if(theme !== "light" && theme !== "dark"){
    theme = window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
  }
  applyTheme(theme);
  const btn = document.getElementById("btnTheme");
  if(btn){
    btn.addEventListener("click", () => {
      const next = document.documentElement.getAttribute("data-theme") === "dark" ? "light" : "dark";
      applyTheme(next);
      try { window.localStorage.setItem(THEME_KEY, next); } catch(_e){}
      renderCharts(computeDerived());
    });
  }
}

initTheme();
initFilters();
document.getElementById("pageSize").value = String(pageSize);
applyFilters();

let resizeTimer = null;
window.addEventListener("resize", () => {
  clearTimeout(resizeTimer);
  resizeTimer = setTimeout(() => renderAll(), 180);
});
window.addEventListener("scroll", syncFloatingControls, { passive:true });
</script>
</body>
</html>
"""

    html = (
        template.replace("__FONT_FACES__", _embedded_font_faces())
        .replace("__PAYLOAD__", json.dumps(payload, ensure_ascii=False))
        .replace("__COVERAGE_START__", coverage_start)
        .replace("__COVERAGE_END__", coverage_end)
        .replace("__N_FLOTAS__", str(int(float(metrics.get("n_flotas", flotas["flota_id"].nunique())))))
        .replace("__N_UNIDADES__", str(int(float(metrics.get("n_unidades", unidades["unidad_id"].nunique())))))
        .replace("__N_DEPOSITOS__", str(int(float(metrics.get("n_depositos", depositos["deposito_id"].nunique())))))
        .replace("__N_COMPONENTES__", str(int(float(metrics.get("n_componentes", componentes["componente_id"].nunique())))))
        .replace("__TOP_UNIT__", str(metrics.get("top_unit_by_priority", "n/a")))
        .replace("__TOP_COMPONENT__", str(metrics.get("top_component_by_priority", "n/a")))
        .replace("__DASHBOARD_VERSION__", dashboard_version)
        .replace("__PAYLOAD_SIGNATURE__", payload_signature)
    )

    branded_path = OUTPUTS_DASHBOARD_DIR / DASHBOARD_SLUG
    branded_path.write_text(html, encoding="utf-8")
    def _redirect_html(target_url: str) -> str:
        return "\n".join(
            [
                "<!DOCTYPE html>",
                "<html lang=\"es\">",
                "<head>",
                "  <meta charset=\"UTF-8\" />",
                "  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />",
                f"  <meta http-equiv=\"refresh\" content=\"0; url={target_url}\" />",
                "  <title>Centro de Control CBM Ferroviario</title>",
                "  <style>",
                "    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }",
                "    html, body { height: 100%; font-family: -apple-system, BlinkMacSystemFont, \"Segoe UI\", system-ui, sans-serif; background: #f4f4f5; color: #18181b; }",
                "    body { display: flex; align-items: center; justify-content: center; padding: 24px; }",
                "    main { text-align: center; max-width: 420px; }",
                "    .label { font-size: .75rem; font-weight: 700; letter-spacing: .08em; text-transform: uppercase; color: #2f6ae4; margin-bottom: 1rem; }",
                "    h1 { font-size: 1.5rem; font-weight: 650; color: #18181b; margin-bottom: .5rem; }",
                "    p { font-size: .92rem; color: #71717a; margin-bottom: 1.5rem; line-height: 1.5; }",
                "    a { color: #2f6ae4; font-weight: 650; text-decoration: none; }",
                "    a:hover { text-decoration: underline; }",
                "    .spinner { width: 24px; height: 24px; border: 2px solid #e4e4e7; border-top-color: #2f6ae4; border-radius: 50%; animation: spin .7s linear infinite; margin: 0 auto 1rem; }",
                "    @keyframes spin { to { transform: rotate(360deg); } }",
                "    @media (prefers-color-scheme: dark) { html, body { background: #111113; color: #f4f4f5; } h1 { color: #f4f4f5; } .spinner { border-color: #27272a; border-top-color: #4d8bf0; } }",
                "  </style>",
                "</head>",
                "<body>",
                "  <main>",
                "    <div class=\"label\">Mantenimiento basado en condición</div>",
                "    <h1>Centro de control CBM ferroviario</h1>",
                "    <p>Redirigiendo al dashboard operativo.</p>",
                "    <div class=\"spinner\" aria-hidden=\"true\"></div>",
                f"    <p><a href=\"{target_url}\">Abrir dashboard</a></p>",
                "  </main>",
                "</body>",
                "</html>",
            ]
        )

    root_index = ROOT_DIR / "index.html"
    root_index.write_text(_redirect_html(f"outputs/dashboard/{DASHBOARD_SLUG}") + "\n", encoding="utf-8")
    (ROOT_DIR / ".nojekyll").write_text("", encoding="utf-8")
    return str(branded_path)


if __name__ == "__main__":
    build_dashboard()
