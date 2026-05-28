from __future__ import annotations

import json
from hashlib import sha1
from datetime import datetime

import numpy as np
import pandas as pd

from src.config import DATA_PROCESSED_DIR, DATA_RAW_DIR, OUTPUTS_DASHBOARD_DIR, DOCS_DIR, ROOT_DIR
from src.reporting_governance import load_or_compute_narrative_metrics

DASHBOARD_SLUG = "centro-control-mantenimiento-ferroviario.html"
PAGES_BASE_URL = "https://mfidalgomartins.github.io/inteligencia-mantenimiento-ferroviario-cbm/"


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
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    flotas = pd.read_csv(DATA_RAW_DIR / "flotas.csv")
    unidades = pd.read_csv(DATA_RAW_DIR / "unidades.csv")
    depositos = pd.read_csv(DATA_RAW_DIR / "depositos.csv")
    componentes = pd.read_csv(DATA_RAW_DIR / "componentes_criticos.csv")
    backlog_raw = pd.read_csv(
        DATA_RAW_DIR / "backlog_mantenimiento.csv",
        usecols=["fecha", "deposito_id", "unidad_id", "componente_id"],
    )

    fleet_week = pd.read_csv(DATA_PROCESSED_DIR / "fleet_week_features.csv")
    unit_day = pd.read_csv(DATA_PROCESSED_DIR / "unit_day_features.csv")
    unit_risk = pd.read_csv(DATA_PROCESSED_DIR / "unit_unavailability_risk_score.csv")
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

    dashboard_version = datetime.now().strftime("%Y%m%d-%H%M")
    payload_signature = sha1(
        f"{len(base)}|{coverage_start}|{coverage_end}|{base['unidad_id'].nunique()}|{base['componente_id'].nunique()}".encode(
            "utf-8"
        )
    ).hexdigest()[:10]

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
    backlog_key = ["fecha", "deposito_id", "unidad_id", "componente_id"]
    backlog_duplicate_count = int(backlog_raw.duplicated(backlog_key).sum())
    duplicate_backlog_issue = (
        f"{backlog_duplicate_count} duplicados sobre key={backlog_key}"
        if backlog_duplicate_count > 0
        else "sin duplicados en la key operativa de backlog"
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
            "severity": "warning",
            "title": "Calidad de datos",
            "value": f"{priority_non_null_rate:.1f}%",
            "description": duplicate_backlog_issue or "cobertura completa en campos críticos de priorización.",
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
  <meta name="theme-color" content="#f6f8fb" />
  <style>
    :root{
      --bg:#f3f6f8;--bg-soft:#f8fafc;--bg-shell:#eef3f6;--card:#ffffff;--card-soft:#f7f9fb;--ink:#151c24;--ink-soft:#293845;
      --muted:#64717f;--muted-soft:#87929d;--line:#dbe3ea;--line-strong:#c3cfd9;--white-rgb:255,255,255;
      --blue:#24546f;--blue-strong:#153247;--blue-soft:#d9e8ef;--red:#b23a48;--green:#25766f;--amber:#b86b24;--slate:#5b6773;--navy:#0d1b2a;
      --critical:#b23a48;--warning:#b86b24;--positive:#25766f;--info:#24546f;--violet:#65578f;
      --shadow:0 22px 48px rgba(15, 31, 45, .10);--shadow-soft:0 10px 24px rgba(15, 31, 45, .07);--sidebar-width:282px;
      --radius-lg:14px;--radius-md:10px;--radius-sm:8px;
    }
    *{box-sizing:border-box}
    html{scroll-behavior:smooth}
    body{margin:0;background:
      radial-gradient(900px 420px at 8% -8%, rgba(36,84,111,.10) 0%, transparent 62%),
      radial-gradient(760px 360px at 96% 0%, rgba(37,118,111,.08) 0%, transparent 52%),
      linear-gradient(180deg,#f9fbfd 0%, #f1f5f8 42%, #eef3f6 100%),
      var(--bg);
      color:var(--ink);font-family:"Inter","Avenir Next","Segoe UI Variable","IBM Plex Sans","Trebuchet MS",sans-serif;overflow-x:hidden;line-height:1.45}
    body,button,input,select{font-variant-numeric:tabular-nums}
    button,select,input,a{touch-action:manipulation}
    button:focus-visible,select:focus-visible,input:focus-visible,a:focus-visible{outline:3px solid rgba(34,87,122,.38);outline-offset:2px}
    .skip-link{position:fixed;left:12px;top:12px;z-index:100;background:#fff;color:#123047;padding:9px 12px;border-radius:10px;border:1px solid var(--line);box-shadow:var(--shadow-soft);transform:translateY(-160%);transition:transform .16s ease}
    .skip-link:focus-visible{transform:translateY(0)}
    .layout{display:grid;grid-template-columns:minmax(260px,var(--sidebar-width)) minmax(0,1fr);gap:16px;min-height:100svh;width:100%;max-width:none;align-items:start;padding:16px}
    body.filters-collapsed .layout{grid-template-columns:minmax(0,1fr)}
    .sidebar{background:
      radial-gradient(380px 220px at 18% 0%, rgba(116,173,219,.16) 0%, transparent 68%),
      linear-gradient(180deg,#0b1f34 0%, #102a45 52%, #14365a 100%);
      color:#edf4fb;padding:18px 16px;position:relative;top:auto;height:auto;overflow:visible;border:1px solid rgba(255,255,255,.08);border-radius:var(--radius-lg);box-shadow:var(--shadow-soft);align-self:start}
    body.filters-collapsed .sidebar{display:none}
    .sidebar h2{margin:0 0 8px;font-size:1.2rem;letter-spacing:-.01em}
    .sidebar p{margin:0 0 16px;font-size:.86rem;color:#c0d2e5;line-height:1.5}
    .sidebar .brand{padding:14px;border:1px solid rgba(255,255,255,.12);border-radius:var(--radius-md);background:linear-gradient(180deg,rgba(255,255,255,.10),rgba(255,255,255,.05));margin-bottom:16px;box-shadow:var(--shadow-soft);backdrop-filter:blur(10px)}
    .sidebar .brand b{display:block;font-size:1.04rem;letter-spacing:.01em;line-height:1.25}
    .sidebar .brand span{font-size:.81rem;color:#d5e4f2;line-height:1.45}
    .sidebar .eyebrow{display:block;font-size:.67rem;text-transform:uppercase;letter-spacing:.16em;color:#8fc0e9;margin-bottom:8px;font-weight:700}
    .filter-group{margin-bottom:14px}
    .filter-group label{display:block;font-size:.74rem;margin-bottom:6px;color:#ebf3fb;font-weight:700;letter-spacing:.04em}
    .filter-group select,.filter-group input{
      width:100%;padding:10px 34px 10px 12px;border-radius:var(--radius-md);border:1px solid rgba(149,182,214,.26);background:linear-gradient(180deg,rgba(14,38,61,.92),rgba(15,42,68,.84));color:#eef4ff;
      -webkit-appearance:none;appearance:none;background-image:
      linear-gradient(45deg, transparent 50%, #b9d0ea 50%),
      linear-gradient(135deg, #b9d0ea 50%, transparent 50%);
      background-position: calc(100% - 16px) calc(50% - 2px), calc(100% - 11px) calc(50% - 2px);
      background-size: 5px 5px, 5px 5px;background-repeat:no-repeat;
    }
    .filter-group select:focus,.filter-group input:focus{outline:3px solid rgba(147,192,230,.22);outline-offset:1px;border-color:#93c0e6;box-shadow:0 0 0 3px rgba(147,192,230,.16)}
    .side-actions{display:flex;gap:8px;margin:14px 0 12px}
    .btn{border:1px solid transparent;border-radius:var(--radius-sm);padding:8px 12px;font-size:.78rem;cursor:pointer;transition:transform .14s ease, box-shadow .14s ease, background .14s ease}
    .btn:hover{transform:translateY(-1px)}
    .btn-reset{background:linear-gradient(180deg,#ffb166,#f3a458);color:#102a45;font-weight:800;box-shadow:0 10px 20px rgba(242,166,90,.22)}
    .btn-top{position:fixed;right:18px;bottom:18px;background:linear-gradient(180deg,#123e67,#113555);color:#fff;box-shadow:var(--shadow);z-index:30;opacity:0;pointer-events:none;transform:translateY(10px);transition:opacity .18s ease, transform .18s ease}
    .btn-top.visible{opacity:1;pointer-events:auto;transform:translateY(0)}
    .sidebar-stats{margin-top:14px;padding:12px;border-radius:var(--radius-md);background:rgba(255,255,255,.07);border:1px solid rgba(255,255,255,.12);font-size:.79rem;backdrop-filter:blur(6px)}
    .sidebar-stats b{color:#fff}
    .content{padding:0 0 28px;min-width:0;overflow-x:hidden;max-width:1700px;width:100%}
    .header{position:relative;overflow:hidden;background:
      linear-gradient(180deg,#ffffff 0%, #f8fbfd 100%);
      color:var(--ink);border-radius:var(--radius-lg);padding:18px;box-shadow:var(--shadow);border:1px solid var(--line)}
    .header-row{display:flex;justify-content:space-between;gap:18px;align-items:flex-start;flex-wrap:wrap}
    .header-main{max-width:920px}
    .header .eyebrow{display:block;font-size:.69rem;text-transform:uppercase;letter-spacing:.18em;color:#4d7693;font-weight:800;margin-bottom:8px}
    .header h1,.section-head h3{font-family:"Inter","Avenir Next","Segoe UI Variable","IBM Plex Sans","Trebuchet MS",sans-serif;letter-spacing:-.02em}
    .header h1{margin:0;font-size:1.48rem;line-height:1.08;max-width:980px;text-wrap:balance}
    .header-actions{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
    .btn-print{background:rgba(255,255,255,.92);border:1px solid rgba(255,255,255,.46);color:#12385e;font-weight:800;box-shadow:var(--shadow-soft)}
    .btn-secondary{background:#eef5fb;border-color:#d3e1ed;color:#17364f;font-weight:800}
    .sub{margin-top:8px;color:#526677;font-size:.93rem;max-width:880px;line-height:1.5}
    .meta{display:flex;gap:9px;flex-wrap:wrap;margin-top:14px;min-width:0}
    .pill{font-size:.76rem;background:#f1f6fa;border:1px solid #d8e4ee;color:#29445c;padding:6px 10px;border-radius:999px;backdrop-filter:blur(8px);font-weight:700}
    .hero-strip{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,220px),1fr));gap:12px;margin-top:16px}
    .hero-panel{background:linear-gradient(180deg,#ffffff,#f8fbfd);border:1px solid var(--line);border-radius:var(--radius-md);padding:13px;min-width:0;box-shadow:var(--shadow-soft)}
    .hero-panel .label{font-size:.66rem;letter-spacing:.16em;text-transform:uppercase;color:#607386;font-weight:900}
    .hero-panel .value{margin-top:7px;font-size:1.2rem;font-weight:800;line-height:1.15;letter-spacing:-.02em}
    .hero-panel .note{margin-top:7px;font-size:.8rem;color:#526677;line-height:1.45}
    .top-nav{display:flex;gap:8px;flex-wrap:wrap;margin-top:14px;min-width:0}
    .top-nav a{font-size:.77rem;text-decoration:none;color:#133758;background:#f5f9fc;border:1px solid #d4e1ef;padding:8px 12px;border-radius:999px;font-weight:800;box-shadow:0 8px 18px rgba(16,52,86,.08)}
    .top-nav a:hover{background:#dceafa}
    .insight{margin-top:12px;padding:14px 16px;border-radius:18px;background:linear-gradient(180deg,#eef5fc,#eaf2fb);border:1px solid #d2e2f3;font-size:.88rem;color:#173a58;font-weight:700;box-shadow:var(--shadow-soft);line-height:1.5}
    .filter-state{margin-top:12px;padding:10px 12px;border-radius:var(--radius-md);background:#f7fafc;border:1px solid #dfe8f0;font-size:.8rem;color:#4d6274;line-height:1.5}
    .cards{margin-top:16px;display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,205px),1fr));gap:14px;min-width:0}
    .cards.cards-primary{grid-template-columns:repeat(auto-fit,minmax(min(100%,240px),1fr))}
    .card{overflow:hidden;background:linear-gradient(180deg,#ffffff 0%, #fbfdff 100%);border:1px solid var(--line);border-radius:var(--radius-md);padding:14px;box-shadow:inset 0 1px 0 rgba(var(--white-rgb),.88), var(--shadow-soft);min-width:0}
    .card:hover{transform:translateY(-1px);transition:transform .15s ease, box-shadow .15s ease;box-shadow:0 16px 30px rgba(13,44,78,.10)}
    .card.primary{padding:18px 18px 17px;border-top:4px solid var(--blue-strong)}
    .card.risk{border-top-color:var(--critical)}
    .card.capacity{border-top-color:var(--warning)}
    .card.value{border-top-color:var(--positive)}
    .card .k{font-size:.69rem;color:var(--muted);text-transform:uppercase;letter-spacing:.12em;font-weight:800}
    .card .v{margin-top:8px;font-size:1.46rem;font-weight:800;line-height:1.05;letter-spacing:-.03em}
    .card.primary .v{font-size:1.7rem}
    .card .s{margin-top:6px;font-size:.81rem;color:#587083;line-height:1.42}
    .card .rule{margin-top:8px;padding-top:8px;border-top:1px solid #edf2f6;font-size:.72rem;color:#3f5f79;line-height:1.35;font-weight:800}
    .section{margin-top:18px;background:linear-gradient(180deg,rgba(255,255,255,.96),rgba(251,253,255,.98));border:1px solid var(--line);border-radius:var(--radius-lg);padding:16px;box-shadow:var(--shadow-soft);min-width:0;overflow:hidden;scroll-margin-top:18px}
    .section{content-visibility:auto;contain-intrinsic-size:760px}
    .section.priority-block{margin-top:22px;padding:18px;border-color:#ead2d5;background:linear-gradient(180deg,#fffafa,#fff7f7)}
    .section.explainer-block{margin-top:18px}
    .section.detail-block{margin-top:26px;background:linear-gradient(180deg,#ffffff,#f7fafc)}
    .section-head{display:flex;justify-content:space-between;gap:14px;align-items:flex-end;flex-wrap:wrap;margin-bottom:12px}
    .section-head .eyebrow{display:block;font-size:.68rem;text-transform:uppercase;letter-spacing:.16em;color:#597ea4;font-weight:800;margin-bottom:5px}
    .section-head h3{margin:0;font-size:1.28rem;line-height:1.08;text-wrap:balance}
    .section-head p{margin:6px 0 0;font-size:.84rem;color:#5e7387;max-width:760px;line-height:1.5}
    .grid2{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,440px),1fr));gap:12px;min-width:0}
    .chart-box{background:linear-gradient(180deg,#ffffff,#f9fbfd);border:1px solid var(--line);border-radius:var(--radius-md);padding:16px;min-width:0;overflow:hidden;box-shadow:inset 0 1px 0 rgba(255,255,255,.7)}
    .chart-head{display:flex;justify-content:space-between;gap:10px;align-items:flex-start;flex-wrap:wrap}
    .chart-box h4{margin:0;font-size:1rem;color:#1f3e5b;line-height:1.28;letter-spacing:-.015em}
    .chart-note{margin-top:6px;font-size:.79rem;color:#667b90;line-height:1.48}
    .chart-question{display:inline-flex;align-items:center;margin-top:8px;padding:5px 8px;border-radius:8px;background:#fff7ed;border:1px solid #f1d2ad;color:#7a4313;font-size:.74rem;font-weight:800;line-height:1.25}
    .chart-legend{display:flex;gap:8px;flex-wrap:wrap;margin-top:10px}
    .legend-chip{display:inline-flex;align-items:center;gap:6px;font-size:.74rem;color:#42566b;background:#f5f8fb;border:1px solid #dbe5ef;padding:5px 9px;border-radius:999px}
    .legend-dot{display:inline-block;width:9px;height:9px;border-radius:999px}
    .svg-chart{width:100%;height:clamp(244px,28vh,320px);min-height:244px;border-top:1px dashed #eef2f7;overflow:hidden;margin-top:12px;background:linear-gradient(180deg,rgba(247,250,253,.72),rgba(255,255,255,0))}
    .svg-chart text{font-family:"Avenir Next","Segoe UI Variable","IBM Plex Sans","Trebuchet MS",sans-serif;font-size:12px}
    .svg-chart rect,.svg-chart circle,.svg-chart path{transition:opacity .12s ease}
    .chart-tooltip{
      position:fixed;z-index:80;pointer-events:none;max-width:260px;background:#0f2438;color:#f8fbff;
      border:1px solid rgba(255,255,255,.2);border-radius:8px;padding:7px 9px;font-size:.75rem;line-height:1.35;
      box-shadow:0 8px 20px rgba(12,28,45,.25)
    }
    .toolbar{display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin:12px 0 10px}
    .toolbar input{padding:11px 13px;border:1px solid var(--line);border-radius:var(--radius-md);min-width:280px;max-width:100%;background:linear-gradient(180deg,#ffffff,#fbfdff);box-shadow:var(--shadow-soft)}
    .toolbar .count{font-size:.79rem;color:#35526f;background:#f1f7fd;padding:7px 10px;border-radius:999px;border:1px solid #d4e4f5;font-weight:700}
    .pager{display:flex;gap:6px;align-items:center;flex-wrap:wrap;margin-left:auto}
    .pager .btn{background:#eff5fb;border-color:#cfe0f2;color:#1c3d5d;font-weight:700}
    .pager .btn[disabled]{opacity:.45;cursor:not-allowed}
    .pager select{padding:7px 8px;border-radius:9px;border:1px solid var(--line);background:#fff;color:#1f3348}
    .pager .page-info{font-size:.78rem;color:#475569;background:#f8fbff;border:1px solid #dbe8f6;padding:6px 9px;border-radius:999px;font-weight:700}
    .pager-label{font-size:.78rem;color:#526479}
    .table-wrap{margin-top:10px;border:1px solid var(--line);border-radius:var(--radius-lg);overflow:auto;max-height:620px;max-width:100%;background:linear-gradient(180deg,#ffffff,#fcfdff);box-shadow:var(--shadow-soft)}
    .empty-row{text-align:center;color:#6b7280;padding:20px}
    table{width:100%;border-collapse:collapse;min-width:980px}
    th,td{padding:10px 11px;border-bottom:1px solid #edf1f5;font-size:.82rem;text-align:left;white-space:nowrap}
    th{position:sticky;top:0;background:linear-gradient(180deg,#10253b,#12304c);color:#fff;cursor:pointer;z-index:2;letter-spacing:.01em}
    tbody tr:nth-child(even) td{background:#fbfdff}
    tr:hover td{background:#f4f9ff}
    .badge{padding:3px 8px;border-radius:999px;font-weight:700;font-size:.73rem}
    .badge-critico{background:#ffd6d9;color:#8f0014}
    .badge-alto{background:#ffe4c7;color:#9b4a00}
    .badge-medio{background:#fff3bf;color:#5f4b00}
    .badge-bajo{background:#d6f5ea;color:#0b5b46}
    .footer-note{font-size:.78rem;color:#6b7280;margin-top:12px;background:linear-gradient(180deg,#f7f9fc,#f2f6fb);border:1px solid #e3ebf3;padding:11px 13px;border-radius:14px;line-height:1.5}
    .command-panel{margin-top:14px;display:grid;grid-template-columns:minmax(0,1.25fr) minmax(280px,.75fr);gap:12px;align-items:stretch}
    .command-card{border:1px solid var(--line);border-radius:var(--radius-lg);padding:16px;background:linear-gradient(180deg,#ffffff,#f7fafc);box-shadow:var(--shadow-soft);min-width:0}
    .command-card.critical{color:#fff;background:linear-gradient(180deg,rgba(178,58,72,.92),rgba(139,40,51,.92));border-color:rgba(255,255,255,.22)}
    .command-card h2{margin:0;font-size:1rem;letter-spacing:.01em}
    .command-card .big{margin-top:8px;font-size:2rem;line-height:1.02;font-weight:900;letter-spacing:-.03em;overflow-wrap:anywhere}
    .command-card p{margin:8px 0 0;color:#43586a;font-size:.86rem;line-height:1.45}
    .command-card.critical p{color:#fff2f3}
    .decision-steps{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px;margin-top:12px}
    .decision-steps.single{grid-template-columns:1fr}
    .decision-proof{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px;margin-top:12px}
    .proof-chip{padding:9px;border-radius:var(--radius-md);background:rgba(255,255,255,.12);border:1px solid rgba(255,255,255,.18);min-width:0}
    .proof-chip b{display:block;font-size:.66rem;text-transform:uppercase;letter-spacing:.1em;color:#c7e4f7}
    .proof-chip span{display:block;margin-top:5px;font-size:.86rem;font-weight:900;color:#fff;overflow-wrap:anywhere}
    .step{padding:10px;border-radius:var(--radius-md);background:#f2f6fa;border:1px solid #dce7ef;min-width:0}
    .command-card.critical .step{background:rgba(255,255,255,.10);border-color:rgba(255,255,255,.16)}
    .step b{display:block;font-size:.7rem;text-transform:uppercase;letter-spacing:.11em;color:#5b7183}
    .command-card.critical .step b{color:#c7e4f7}
    .step span{display:block;margin-top:5px;font-size:.9rem;font-weight:800;overflow-wrap:anywhere}
    .anomaly-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,230px),1fr));gap:10px;margin-top:14px}
    .anomaly{border:1px solid var(--line);border-left:5px solid var(--info);border-radius:14px;background:#fff;padding:12px;box-shadow:var(--shadow-soft);min-width:0}
    .anomaly.critical{border-left-color:var(--critical);background:#fff7f7}
    .anomaly.warning{border-left-color:var(--warning);background:#fffaf2}
    .anomaly.info{border-left-color:var(--info);background:#f5faff}
    .anomaly .label{font-size:.68rem;text-transform:uppercase;letter-spacing:.12em;color:#5f6f7d;font-weight:900}
    .anomaly .value{margin-top:5px;font-size:1.22rem;font-weight:900;letter-spacing:-.02em}
    .anomaly .text{margin-top:5px;font-size:.8rem;color:#5b6875;line-height:1.42}
    .interpretation{margin-top:12px;display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,260px),1fr));gap:10px}
    .interpretation .item{padding:12px 13px;border-radius:14px;background:#f7fafc;border:1px solid #dde7ef;color:#2d4051;font-size:.82rem;line-height:1.45}
    .interpretation b{display:block;color:#17202a;margin-bottom:4px}
    @media (max-width:1460px){
      .cards{grid-template-columns:repeat(auto-fit,minmax(min(100%,185px),1fr))}
      .grid2{grid-template-columns:repeat(auto-fit,minmax(min(100%,380px),1fr))}
    }
    @media (max-width:1240px){
      .layout{padding:12px}
      .layout{grid-template-columns:1fr}
      .sidebar{position:relative;height:auto;max-height:none;order:2}
      .content{order:1}
      .btn-top{bottom:12px;right:12px}
    }
    @media (max-width:860px){
      .content{padding:0 0 14px}
      .header h1{font-size:1.22rem}
      .hero-strip{grid-template-columns:1fr}
      .grid2{grid-template-columns:1fr}
      .cards{grid-template-columns:repeat(2,minmax(120px,1fr));gap:8px}
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
      .sidebar,.top-nav,.btn-top,.btn-print,.btn-secondary,.filter-state{display:none !important}
      .layout{grid-template-columns:1fr}
      .header{box-shadow:none;border:1px solid #e1e6ef}
      .section,.card,.chart-box{box-shadow:none}
      .insight{background:#f2f5fb;border-color:#d9e2ef}
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
          <div class="sub">Primero decide la entrada a taller; después explica riesgo, backlog, capacidad y valor de diferimiento.</div>
        </div>
        <div class="header-actions">
          <button type="button" class="btn btn-secondary" id="btnFilters" aria-controls="globalFilters" aria-expanded="true">Ocultar filtros</button>
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
      <div class="command-panel" id="sec_action">
        <div class="command-card critical">
          <h2>Qué hacer ahora</h2>
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
            <p><strong>Componente que debe sustituirse primero:</strong> __TOP_COMPONENT__</p>
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
      <div class="hero-strip">
        <div class="hero-panel">
          <div class="label">Prioridad inmediata</div>
          <div class="value" id="hero_priority_value">-</div>
          <div class="note" id="hero_priority_note">Unidad y componente con mayor prioridad operativa.</div>
        </div>
        <div class="hero-panel">
          <div class="label">Exposición operativa</div>
          <div class="value" id="hero_exposure_value">-</div>
          <div class="note" id="hero_exposure_note">Backlog crítico, vencimiento y riesgo de diferimiento.</div>
        </div>
        <div class="hero-panel">
          <div class="label">Palanca estratégica</div>
          <div class="value" id="hero_value_value">-</div>
          <div class="note" id="hero_value_note">Ahorro proxy y saturación media del taller.</div>
        </div>
      </div>
      <div class="insight" id="headerInsight">Resumen operativo: salud de activos, riesgo operativo y prioridades de taller para la ventana seleccionada.</div>
      <nav class="top-nav" aria-label="Navegación del dashboard">
        <a href="#sec_action">Acción ahora</a>
        <a href="#sec_saude">Riesgo</a>
        <a href="#sec_taller">Backlog</a>
        <a href="#sec_alertas">Drivers</a>
        <a href="#sec_estrategica">Escenarios</a>
        <a href="#sec_tabela">Detalle</a>
      </nav>
      <div class="filter-state" id="filterState" aria-live="polite">Vista completa sin filtros activos.</div>
    </section>

    <section class="cards cards-primary" id="kpiCardsPrimary">
      <div class="card primary"><div class="k">Disponibilidad de flota</div><div class="v" id="k_avail">-</div><div class="s">Lectura agregada de continuidad operacional.</div><div class="rule">Regla: estable si >=90%; investigar si cae por debajo de 88%.</div></div>
      <div class="card primary risk"><div class="k">Unidades prioridad >=70</div><div class="v" id="k_uhr">-</div><div class="s">Unidades que compiten por entrada prioritaria.</div><div class="rule">Regla: si >0, ordenar por score y ventana, no por promedio de flota.</div></div>
      <div class="card primary capacity"><div class="k">Backlog crítico físico</div><div class="v" id="k_bcf">-</div><div class="s">Carga física prioritaria pendiente de taller.</div><div class="rule">Regla: crisis si críticos/físicos supera 80%.</div></div>
      <div class="card primary value"><div class="k">Ahorro operativo proxy CBM</div><div class="v" id="k_ahorro">-</div><div class="s">Valor incremental frente al escenario reactivo.</div><div class="rule">Regla: defender CBM si probabilidad de ahorro positivo >=75%.</div></div>
    </section>

    <section class="cards" id="kpiCards">
      <div class="card"><div class="k">MTBF</div><div class="v" id="k_mtbf">-</div><div class="s">Fiabilidad media observada.</div><div class="rule">Interpretación: menor MTBF reduce margen para diferir intervención.</div></div>
      <div class="card"><div class="k">MTTR</div><div class="v" id="k_mttr">-</div><div class="s">Recuperación media tras incidencia.</div><div class="rule">Interpretación: MTTR alto convierte fallos en pérdida directa de servicio.</div></div>
      <div class="card"><div class="k">Backlog físico</div><div class="v" id="k_bf">-</div><div class="s">Órdenes reales aún abiertas.</div><div class="rule">Interpretación: volumen físico define capacidad necesaria, no solo riesgo.</div></div>
      <div class="card"><div class="k">Backlog vencido</div><div class="v" id="k_bv">-</div><div class="s">Carga fuera de ventana objetivo.</div><div class="rule">Interpretación: vencimiento alto indica cola no absorbida.</div></div>
      <div class="card"><div class="k">Riesgo diferimiento alto</div><div class="v" id="k_drh">-</div><div class="s">Casos donde aplazar destruye valor.</div><div class="rule">Regla: umbral operativo score >=70.</div></div>
      <div class="card"><div class="k">Exposición backlog-ajustada</div><div class="v" id="k_bea">-</div><div class="s">Presión estructural combinada del backlog.</div><div class="rule">Interpretación: >80 exige decisión de capacidad o secuenciación.</div></div>
      <div class="card"><div class="k">Downtime extra por diferir 14d</div><div class="v" id="k_he">-</div><div class="s">Indisponibilidad incremental si se aplaza la intervención.</div><div class="rule">Interpretación: horas que justifican actuar antes de la próxima ventana larga.</div></div>
      <div class="card"><div class="k">Correctivas evitables</div><div class="v" id="k_ce">-</div><div class="s">Intervenciones reactivas que puede absorber CBM.</div><div class="rule">Interpretación: mide carga que debería migrar a planificación.</div></div>
      <div class="card"><div class="k">Alertas tempranas activas</div><div class="v" id="k_alertas">-</div><div class="s">Señal de vigilancia activa en la cartera.</div><div class="rule">Regla: probabilidad de fallo a 30 días >=65%.</div></div>
      <div class="card"><div class="k">Saturación media taller</div><div class="v" id="k_sat">-</div><div class="s">Presión media sobre capacidad disponible.</div><div class="rule">Interpretación: la media oculta depósitos críticos; revisar saturación máxima.</div></div>
    </section>
    <section class="section priority-block" id="sec_anomalias">
      <div class="section-head">
        <div>
          <span class="eyebrow">Señales que sostienen la acción</span>
          <h3>Por qué esta unidad y este componente no deben esperar</h3>
          <p>La lectura combina backlog físico, riesgo de diferimiento y capacidad real de absorción para validar la prioridad.</p>
        </div>
      </div>
      <div class="anomaly-grid" id="anomalyGrid"></div>
      <div class="interpretation">
        <div class="item"><b>Lectura de backlog</b><span id="interp_backlog">-</span></div>
        <div class="item"><b>Lectura de riesgo</b><span id="interp_risk">-</span></div>
        <div class="item"><b>Lectura de capacidad</b><span id="interp_capacity">-</span></div>
      </div>
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
          <div class="chart-legend"><span class="legend-chip"><span class="legend-dot" style="background:#bc4749"></span>Riesgo</span><span class="legend-chip"><span class="legend-dot" style="background:#2a9d8f"></span>Salud</span></div>
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
          <p>Contrasta prioridad, riesgo de diferimiento e impacto operacional para confirmar que la primera acción es defendible.</p>
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
        <div class="chart-box"><div class="chart-head"><h4>Trade-off de diferimiento</h4></div><div class="chart-note">Evolución conjunta del coste y la indisponibilidad al aplazar intervención.</div><div class="chart-question">Pregunta: ¿cuánto cuesta aplazar?</div><div class="chart-legend"><span class="legend-chip"><span class="legend-dot" style="background:#bc4749"></span>Coste</span><span class="legend-chip"><span class="legend-dot" style="background:#1d4e89"></span>Indisponibilidad</span></div><div id="ch_deferral" class="svg-chart"></div></div>
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
function fmt1(n){ return Number(n).toFixed(1); }
function fmt2(n){ return Number(n).toFixed(2); }
function fmt0(n){ return Math.round(Number(n)).toLocaleString("es-ES"); }
function fmtMoneyCompact(n){
  const value = Number(n);
  if(!Number.isFinite(value)) return "€0";
  if(Math.abs(value) >= 1_000_000) return `€${(value/1_000_000).toFixed(1)}M`;
  if(Math.abs(value) >= 1_000) return `€${(value/1_000).toFixed(0)}k`;
  return `€${fmt0(value)}`;
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

function setFiltersCollapsed(collapsed){
  document.body.classList.toggle("filters-collapsed", collapsed);
  const btn = document.getElementById("btnFilters");
  if(btn){
    btn.textContent = collapsed ? "Mostrar filtros" : "Ocultar filtros";
    btn.setAttribute("aria-expanded", collapsed ? "false" : "true");
  }
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
  el.textContent = active.length
    ? `Filtros activos: ${active.join(" · ")}`
    : "Vista completa sin filtros activos.";
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
  try {
    setFiltersCollapsed(window.localStorage.getItem(FILTERS_STATE_KEY) === "1");
  } catch (_err) {
    setFiltersCollapsed(false);
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
      color:toNum(r.deferral_risk_score)>=70 ? "#b23a48" : (toNum(r.intervention_priority_score)>=70 ? "#c27a2c" : "#22577a"),
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

  setText("k_avail", `${fmt2(avail)}%`);
  setText("k_mtbf", fmt2(mtbf));
  setText("k_mttr", fmt2(mttr));
  setText("k_uhr", fmt0(highPriorityUnits));
  setText("k_bf", fmt0(backlogPhysical));
  setText("k_bv", fmt0(backlogOverdue));
  setText("k_bcf", fmt0(backlogCritical));
  setText("k_drh", fmt0(deferralHigh));
  setText("k_bea", fmt1(exposure));
  setText("k_he", `${fmt1(downtimeDeferral14d)} h`);
  setText("k_ce", fmt1(avoidableCorrectives));
  setText("k_ahorro", `€${fmt0(savings)}`);
  setText("k_alertas", fmt0(alerts));
  setText("k_sat", `${fmt1(sat)}%`);
  setText("s_count_rows", fmt0(d.rows.length));
  setText("s_count_units", fmt0(d.uniqueUnits.size));
  setText("s_count_high", fmt0(d.highRiskUnits.size));

  setText("hero_priority_value", `${metricSnapshot.top_unit_by_priority || "n/a"} / ${metricSnapshot.top_component_by_priority || "n/a"}`);
  setText("hero_priority_note", `${fmt0(highPriorityUnits)} unidades con prioridad >=70 en la vista actual; score oficial ${fmt1(metricSnapshot.top_priority_score)} en familia ${metricSnapshot.top_component_family_by_priority || "n/a"}.`);
  setText("hero_exposure_value", `${fmt0(backlogCritical)} backlog crítico · ${fmt0(deferralHigh)} diferimientos altos`);
  setText("hero_exposure_note", `Exposición backlog-ajustada ${fmt1(exposure)}; cuello de botella ${topDepot} con ${fmt1(topDepotSat)}% de saturación.`);
  setText("hero_value_value", `${fmtMoneyCompact(savings)} potencial`);
  setText("hero_value_note", `${fmt1(probPositive)}% prob. ahorro positivo; ${fmt1(downtimeDeferral14d)} h extra si se difiere 14d.`);
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

function drawBars(containerId, items, color="#1d4e89", valueFmt=(v)=>fmt1(v), showLabels=true){
  const svg = makeSvg(containerId);
  if(!svg) return;
  const w = svg.viewBox.baseVal.width, h = svg.viewBox.baseVal.height;
  const m = {l:40,r:10,t:10,b:48};
  const iw = w-m.l-m.r, ih = h-m.t-m.b;
  if(!items.length){
    svg.innerHTML = `<text x="${w/2}" y="${h/2}" text-anchor="middle" fill="#6b7280" font-size="12">Sin datos para filtros</text>`;
    return;
  }
  const maxV = Math.max(...items.map(d=>toNum(d.value)), 1);
  const bw = Math.max(6, iw/items.length - 4);
  const dense = items.length > 8;
  const showValue = !dense;
  const labelStep = items.length > 14 ? Math.ceil(items.length / 7) : 1;
  const labelRotation = items.length > 7 ? 28 : 0;
  let out = `<line x1="${m.l}" y1="${m.t+ih}" x2="${m.l+iw}" y2="${m.t+ih}" stroke="#cbd5e1"/>`;
  [0.25,0.5,0.75].forEach(p => {
    const yy = m.t + ih - ih*p;
    out += `<line x1="${m.l}" y1="${yy}" x2="${m.l+iw}" y2="${yy}" stroke="#edf2f7" />`;
  });
  items.forEach((d,i)=>{
    const v = toNum(d.value);
    const bh = ih * (v/maxV);
    const x = m.l + i*(iw/items.length) + 2;
    const y = m.t + ih - bh;
    out += `<rect x="${x}" y="${y}" width="${bw}" height="${bh}" fill="${color}" rx="2" data-tip="${esc(d.label)}: ${valueFmt(v)}"></rect>`;
    if(showValue){
      out += `<text x="${x+bw/2}" y="${y-3}" text-anchor="middle" fill="#334155" font-size="10">${valueFmt(v)}</text>`;
    }
    if(showLabels && (i % labelStep === 0 || i === items.length - 1)){
      const lbl = esc(String(d.label).slice(0,12));
      if(labelRotation){
        out += `<text x="${x+bw/2}" y="${m.t+ih+12}" text-anchor="middle" fill="#475569" font-size="10" transform="rotate(${labelRotation} ${x+bw/2},${m.t+ih+12})">${lbl}</text>`;
      } else {
        out += `<text x="${x+bw/2}" y="${m.t+ih+12}" text-anchor="middle" fill="#475569" font-size="10">${lbl}</text>`;
      }
    }
  });
  svg.innerHTML = out;
  bindSvgTooltip(svg);
}

function drawDualBars(containerId, labels, v1, v2, c1="#bc4749", c2="#2a9d8f"){
  const items1 = labels.map((l,i)=>({label:l, value:toNum(v1[i])}));
  const items2 = labels.map((l,i)=>({label:l, value:toNum(v2[i])}));
  const svg = makeSvg(containerId); if(!svg) return;
  const w = svg.viewBox.baseVal.width, h = svg.viewBox.baseVal.height;
  const m = {l:40,r:10,t:34,b:56}; const iw=w-m.l-m.r, ih=h-m.t-m.b;
  if(!labels.length){ svg.innerHTML=`<text x="${w/2}" y="${h/2}" text-anchor="middle" fill="#6b7280" font-size="12">Sin datos para filtros</text>`; return; }
  const maxV = Math.max(...items1.map(d=>d.value), ...items2.map(d=>d.value), 1);
  const slot = iw/labels.length, bw = Math.max(6,(slot-6)/2);
  let out = `<line x1="${m.l}" y1="${m.t+ih}" x2="${m.l+iw}" y2="${m.t+ih}" stroke="#cbd5e1"/>`;
  items1.forEach((d,i)=>{
    const x0 = m.l + i*slot + 2;
    const h1 = ih*(d.value/maxV), y1 = m.t+ih-h1;
    const h2 = ih*(items2[i].value/maxV), y2 = m.t+ih-h2;
    out += `<rect x="${x0}" y="${y1}" width="${bw}" height="${h1}" fill="${c1}" rx="2" data-tip="${esc(d.label)} · Riesgo: ${fmt1(d.value)}"/>`;
    out += `<rect x="${x0+bw+2}" y="${y2}" width="${bw}" height="${h2}" fill="${c2}" rx="2" data-tip="${esc(d.label)} · Salud: ${fmt1(items2[i].value)}"/>`;
    const lbl = esc(String(d.label).slice(0,12));
    out += `<text x="${x0+bw}" y="${m.t+ih+12}" text-anchor="middle" fill="#475569" font-size="10" transform="rotate(24 ${x0+bw},${m.t+ih+12})">${lbl}</text>`;
  });
  svg.innerHTML = out;
  bindSvgTooltip(svg);
}

function drawLine(containerId, xVals, y1, y2, c1="#bc4749", c2="#1d4e89"){
  const svg = makeSvg(containerId); if(!svg) return;
  const w = svg.viewBox.baseVal.width, h = svg.viewBox.baseVal.height;
  const m = {l:40,r:10,t:12,b:36}; const iw=w-m.l-m.r, ih=h-m.t-m.b;
  if(!xVals.length){ svg.innerHTML=`<text x="${w/2}" y="${h/2}" text-anchor="middle" fill="#6b7280" font-size="12">Sin datos</text>`; return; }
  const maxV = Math.max(...y1.map(toNum), ...y2.map(toNum), 1);
  const minV = Math.min(...y1.map(toNum), ...y2.map(toNum), 0);
  const scaleY = (v)=> m.t + ih - ((toNum(v)-minV)/(maxV-minV || 1))*ih;
  const scaleX = (i)=> m.l + (xVals.length===1? iw/2 : i*(iw/(xVals.length-1)));
  const path = (arr)=> arr.map((v,i)=>`${i===0?'M':'L'} ${scaleX(i)} ${scaleY(v)}`).join(" ");
  let out = `<line x1="${m.l}" y1="${m.t+ih}" x2="${m.l+iw}" y2="${m.t+ih}" stroke="#cbd5e1"/>`;
  out += `<path d="${path(y1)}" fill="none" stroke="${c1}" stroke-width="2.2"/>`;
  out += `<path d="${path(y2)}" fill="none" stroke="${c2}" stroke-width="2.2"/>`;
  y1.forEach((v,i)=>{ out += `<circle cx="${scaleX(i)}" cy="${scaleY(v)}" r="3" fill="${c1}" data-tip="Ventana ${esc(xVals[i])}d · Coste: ${fmt2(v)}"/>`; });
  y2.forEach((v,i)=>{ out += `<circle cx="${scaleX(i)}" cy="${scaleY(v)}" r="3" fill="${c2}" data-tip="Ventana ${esc(xVals[i])}d · Indisponibilidad: ${fmt1(v)}"/>`; });
  const tickStep = Math.max(1, Math.ceil(xVals.length / 6));
  xVals.forEach((x,i)=>{ if(i%tickStep===0 || i===xVals.length-1){ out += `<text x="${scaleX(i)}" y="${m.t+ih+12}" text-anchor="middle" fill="#475569" font-size="10">${esc(x)}</text>`; }});
  svg.innerHTML = out;
  bindSvgTooltip(svg);
}

function drawScatter(containerId, points){
  const svg = makeSvg(containerId); if(!svg) return;
  const w = svg.viewBox.baseVal.width, h = svg.viewBox.baseVal.height;
  const m = {l:44,r:16,t:14,b:38}; const iw=w-m.l-m.r, ih=h-m.t-m.b;
  if(!points.length){ svg.innerHTML=`<text x="${w/2}" y="${h/2}" text-anchor="middle" fill="#6b7280" font-size="12">Sin datos para filtros</text>`; return; }
  const xMin = Math.min(...points.map(p=>p.x), 0), xMax = Math.max(...points.map(p=>p.x), 100);
  const yMin = Math.min(...points.map(p=>p.y), 0), yMax = Math.max(...points.map(p=>p.y), 100);
  const sx = v => m.l + ((toNum(v)-xMin)/(xMax-xMin || 1))*iw;
  const sy = v => m.t + ih - ((toNum(v)-yMin)/(yMax-yMin || 1))*ih;
  let out = `<line x1="${m.l}" y1="${m.t+ih}" x2="${m.l+iw}" y2="${m.t+ih}" stroke="#cbd5e1"/>`;
  out += `<line x1="${m.l}" y1="${m.t}" x2="${m.l}" y2="${m.t+ih}" stroke="#cbd5e1"/>`;
  const x70 = sx(70), y70 = sy(70);
  out += `<line x1="${x70}" y1="${m.t}" x2="${x70}" y2="${m.t+ih}" stroke="#e8b4b8" stroke-dasharray="4 4"/>`;
  out += `<line x1="${m.l}" y1="${y70}" x2="${m.l+iw}" y2="${y70}" stroke="#e8b4b8" stroke-dasharray="4 4"/>`;
  out += `<text x="${m.l+iw}" y="${m.t+12}" text-anchor="end" fill="#7a2e38" font-size="10">alta prioridad + no diferir</text>`;
  points.forEach(p => {
    out += `<circle cx="${sx(p.x)}" cy="${sy(p.y)}" r="${p.size}" fill="${p.color}" fill-opacity=".76" stroke="#fff" stroke-width="1" data-tip="${esc(p.tip)}"></circle>`;
  });
  out += `<text x="${m.l+iw}" y="${h-8}" text-anchor="end" fill="#475569" font-size="10">Prioridad intervención</text>`;
  out += `<text x="12" y="${m.t+10}" fill="#475569" font-size="10" transform="rotate(-90 12,${m.t+10})">Riesgo diferimiento</text>`;
  svg.innerHTML = out;
  bindSvgTooltip(svg);
}

function drawBacklogBars(containerId, rows){
  const svg = makeSvg(containerId); if(!svg) return;
  const w = svg.viewBox.baseVal.width, h = svg.viewBox.baseVal.height;
  const m = {l:42,r:14,t:12,b:48}; const iw=w-m.l-m.r, ih=h-m.t-m.b;
  const items = rows.slice().sort((a,b)=>toNum(b.backlog_critical_items)-toNum(a.backlog_critical_items)).slice(0,10);
  if(!items.length){ svg.innerHTML=`<text x="${w/2}" y="${h/2}" text-anchor="middle" fill="#6b7280" font-size="12">Sin backlog válido</text>`; return; }
  const maxV = Math.max(...items.flatMap(r => [toNum(r.backlog_physical_items), toNum(r.backlog_overdue_items), toNum(r.backlog_critical_items)]), 1);
  const slot = iw/items.length;
  const bw = Math.max(5, (slot-8)/3);
  let out = `<line x1="${m.l}" y1="${m.t+ih}" x2="${m.l+iw}" y2="${m.t+ih}" stroke="#cbd5e1"/>`;
  items.forEach((r,i)=>{
    const x = m.l + i*slot + 3;
    const vals = [
      ["Físico", toNum(r.backlog_physical_items), "#22577a"],
      ["Vencido", toNum(r.backlog_overdue_items), "#c27a2c"],
      ["Crítico", toNum(r.backlog_critical_items), "#b23a48"],
    ];
    vals.forEach(([name,val,color],j)=>{
      const bh = ih*(val/maxV);
      const y = m.t+ih-bh;
      out += `<rect x="${x+j*(bw+2)}" y="${y}" width="${bw}" height="${bh}" fill="${color}" rx="2" data-tip="${esc(r.deposito_id)} · ${name}: ${fmt0(val)}"></rect>`;
    });
    out += `<text x="${x+bw}" y="${m.t+ih+12}" text-anchor="middle" fill="#475569" font-size="10">${esc(r.deposito_id)}</text>`;
  });
  out += `<text x="${m.l+iw}" y="${m.t+10}" text-anchor="end" fill="#475569" font-size="10">Snapshot ${esc(meta.latest_depot_valid_date || "")}</text>`;
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
  const w = svg.viewBox.baseVal.width, h = svg.viewBox.baseVal.height;
  const m = {l:42,r:14,t:18,b:42}; const iw=w-m.l-m.r, ih=h-m.t-m.b;
  if(!scenarios.length){ svg.innerHTML=`<text x="${w/2}" y="${h/2}" text-anchor="middle" fill="#6b7280" font-size="12">Sin datos de scheduling</text>`; return; }
  const slot = iw/scenarios.length, bw = Math.min(84, slot*.46);
  let out = `<line x1="${m.l}" y1="${m.t+ih}" x2="${m.l+iw}" y2="${m.t+ih}" stroke="#cbd5e1"/>`;
  scenarios.forEach((s,i)=>{
    const x = m.l+i*slot+slot/2-bw/2;
    let y = m.t+ih;
    [["programada",s.programada,"#25766f"],["programable",s.programable,"#22577a"],["pendiente",s.pendiente,"#b23a48"]].forEach(([name,val,color])=>{
      const bh = ih*(val/100);
      y -= bh;
      out += `<rect x="${x}" y="${y}" width="${bw}" height="${bh}" fill="${color}" data-tip="${esc(s.label)} · ${name}: ${fmt1(val)}%"></rect>`;
    });
    out += `<circle cx="${x+bw+13}" cy="${m.t+ih-ih*(s.residual/100)}" r="4" fill="#111827" data-tip="${esc(s.label)} · riesgo residual no atendido: ${fmt1(s.residual)}%"></circle>`;
    out += `<text x="${x+bw/2}" y="${m.t+ih+14}" text-anchor="middle" fill="#475569" font-size="10">${esc(s.label)}</text>`;
  });
  out += `<text x="${m.l}" y="${m.t+10}" fill="#25766f" font-size="10">programada</text><text x="${m.l+76}" y="${m.t+10}" fill="#22577a" font-size="10">programable</text><text x="${m.l+168}" y="${m.t+10}" fill="#b23a48" font-size="10">pendiente</text>`;
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
  document.getElementById("strategyInsight").textContent =
    `Lectura estratégica: CBM vs reactiva preserva valor por ~${fmt0(savings)} EUR en escenario base, con probabilidad de ahorro positivo del ${fmt1(prob)}% y rango plausible de ${fmt2(rmin)}M€ a ${fmt2(rmax)}M€.`;
  document.getElementById("headerInsight").textContent =
    `Resumen operativo del recorte actual: ${d.rows.length} componentes, ${d.uniqueUnits.size} unidades y ${d.highRiskUnits.size} unidades con prioridad >=70 en la vista filtrada. La prioridad oficial sigue siendo ${metricSnapshot.top_unit_by_priority}/${metricSnapshot.top_component_by_priority}.`;
}

function renderCharts(d){
  const labelsFam = d.familyRisk.map(x=>x.label);
  const riskVals = d.familyRisk.map(x=>x.value*100);
  const healthVals = labelsFam.map(l => d.familyMapHealth.get(l) || 0);
  drawDualBars("ch_family", labelsFam, riskVals, healthVals);

  const rulOrder = {"<=14":0,"15-30":1,"31-60":2,"61-90":3,"91-180":4,">180":5};
  drawBars("ch_rul", d.rulBucket.sort((a,b)=>(rulOrder[a.label] ?? 99) - (rulOrder[b.label] ?? 99)).map(x=>({label:x.label,value:x.value})), "#6d597a", (v)=>fmt0(v), true);
  drawBars("ch_top_units", d.topPriority, "#bc4749", (v)=>fmt1(v), true);
  drawBars("ch_service", d.topService, "#f4a261", (v)=>fmt1(v), true);
  drawScatter("ch_priority_deferral", d.priorityDeferral);

  const depotBars = d.depotSlice.map(x => ({label:String(x.deposito_id), value:toNum(x.saturation_ratio)*100}));
  drawBacklogBars("ch_backlog_depot", d.depotSlice);
  drawBars("ch_depot", depotBars.sort((a,b)=>b.value-a.value).slice(0,10), "#1d4e89", (v)=>fmt1(v), true);
  drawBars("ch_decisions", d.decisionDist, "#2a9d8f", (v)=>fmt0(v), true);
  drawBars("ch_drivers", d.driverDist, "#64748b", (v)=>fmt0(v), true);

  const insp = inspectionData.map(r => ({label:String(r.family||r.familia||"family"), value:toNum(r.pre_failure_detection_rate||r.coverage_pre_falla||0)}));
  drawBars("ch_inspection", insp, "#2a9d8f", (v)=>fmt2(v), true);

  const stratBars = d.strategySlice.map(r => ({label:String(r.estrategia), value:toNum(r.fleet_availability)}));
  drawBars("ch_strategy", stratBars, "#334155", (v)=>fmt1(v), true);

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

  body.innerHTML = pageRows.map(r => `
    <tr>${tableColumns.map(c => c==="risk_level" ? `<td>${badge(r[c])}</td>` : `<td>${esc(r[c])}</td>`).join("")}</tr>
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
        template.replace("__PAYLOAD__", json.dumps(payload, ensure_ascii=False))
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
    redirect_html = "\n".join(
        [
            "<!DOCTYPE html>",
            "<html lang=\"es\">",
            "<head>",
            "  <meta charset=\"UTF-8\" />",
            f"  <meta http-equiv=\"refresh\" content=\"0; url={PAGES_BASE_URL}outputs/dashboard/{DASHBOARD_SLUG}\" />",
            "  <title>Dashboard Operativo</title>",
            "</head>",
            "<body>",
            "  <p>Redirigiendo al dashboard operativo...</p>",
            "</body>",
            "</html>",
        ]
    )
    docs_index = DOCS_DIR / "index.html"
    docs_index.write_text(redirect_html, encoding="utf-8")
    root_index = ROOT_DIR / "index.html"
    root_index.write_text(
        redirect_html.replace(
            f"{PAGES_BASE_URL}outputs/dashboard/{DASHBOARD_SLUG}",
            f"outputs/dashboard/{DASHBOARD_SLUG}",
        ),
        encoding="utf-8",
    )
    (DOCS_DIR / ".nojekyll").write_text("", encoding="utf-8")
    (ROOT_DIR / ".nojekyll").write_text("", encoding="utf-8")
    return str(branded_path)


if __name__ == "__main__":
    build_dashboard()
