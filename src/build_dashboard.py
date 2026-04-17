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

    metrics = load_or_compute_narrative_metrics(force_recompute=False)

    fleet_week["week_start"] = pd.to_datetime(fleet_week["week_start"], errors="coerce")
    unit_day["fecha"] = pd.to_datetime(unit_day["fecha"], errors="coerce")
    depot_pressure["fecha"] = pd.to_datetime(depot_pressure["fecha"], errors="coerce")

    latest_unit_day = _latest_frame(unit_day, "fecha")
    latest_depot = _latest_frame(depot_pressure, "fecha")
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
        "metric_snapshot": {
            "fleet_availability_pct": float(metrics.get("fleet_availability_pct", 0.0)),
            "mtbf_proxy_hours": float(metrics.get("mtbf_proxy_hours", 0.0)),
            "mttr_proxy_hours": float(metrics.get("mttr_proxy_hours", 0.0)),
            "high_risk_units_count": float(metrics.get("high_risk_units_count", 0.0)),
            "backlog_physical_items_count": float(metrics.get("backlog_physical_items_count", 0.0)),
            "backlog_overdue_items_count": float(metrics.get("backlog_overdue_items_count", 0.0)),
            "backlog_critical_physical_count": float(metrics.get("backlog_critical_physical_count", 0.0)),
            "cbm_operational_savings_eur": float(metrics.get("cbm_operational_savings_eur", 0.0)),
            "deferral_downtime_delta_14d_h": float(metrics.get("deferral_downtime_delta_14d_h", 0.0)),
            "avoidable_correctives_inspection": float(metrics.get("avoidable_correctives_inspection", 0.0)),
            "mean_depot_saturation_pct": float(metrics.get("mean_depot_saturation_pct", 0.0)),
            "high_deferral_risk_cases_count": float(metrics.get("high_deferral_risk_cases_count", 0.0)),
            "backlog_exposure_adjusted_mean": float(metrics.get("backlog_exposure_adjusted_mean", 0.0)),
            "top_unit_by_priority": str(metrics.get("top_unit_by_priority", "n/a")),
            "top_component_by_priority": str(metrics.get("top_component_by_priority", "n/a")),
            "early_warnings_active_count": float((base["prob_fallo_30d"] >= 0.65).sum()),
            "row_count_components": float(len(base)),
            "row_count_units": float(base["unidad_id"].nunique()),
        },
        "meta": {
            "dashboard_version": dashboard_version,
            "payload_signature": payload_signature,
            "rows": int(len(base)),
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
  <style>
    :root{
      --bg:#edf2f8;--bg-soft:#f7f9fc;--card:#ffffff;--card-soft:#f3f7fb;--ink:#102031;--muted:#5d7084;--line:#d7e2ee;
      --blue:#184e77;--blue-strong:#103c64;--red:#b13b45;--green:#1f8a70;--amber:#d99036;--slate:#607287;--navy:#0b1f34;
      --critical:#aa2e39;--warning:#b66a18;--positive:#207567;--info:#245c92;
      --shadow:0 14px 34px rgba(12, 34, 58, .10);--shadow-soft:0 8px 20px rgba(12, 34, 58, .06);--sidebar-width:300px;
      --radius-lg:22px;--radius-md:16px;--radius-sm:12px;
    }
    *{box-sizing:border-box}
    html{scroll-behavior:smooth}
    body{margin:0;background:
      radial-gradient(1100px 520px at -12% -8%, rgba(24,78,119,.14) 0%, transparent 58%),
      radial-gradient(860px 420px at 112% -12%, rgba(31,138,112,.09) 0%, transparent 46%),
      linear-gradient(180deg,#f8fbfe 0%, #edf2f8 36%, #edf2f8 100%),
      var(--bg);
      color:var(--ink);font-family:"Avenir Next","Segoe UI Variable","Trebuchet MS",sans-serif;overflow-x:hidden;line-height:1.45}
    .layout{display:grid;grid-template-columns:minmax(260px,var(--sidebar-width)) minmax(0,1fr);min-height:100svh;width:100%;max-width:none;align-items:start}
    .sidebar{background:linear-gradient(180deg,#0c2237,#112c48 54%, #143356 100%);color:#edf4fb;padding:22px 16px;position:sticky;top:0;height:100dvh;overflow:auto;border-right:1px solid rgba(255,255,255,.08);box-shadow:inset -1px 0 0 rgba(255,255,255,.05)}
    .sidebar h2{margin:0 0 8px;font-size:1.16rem;letter-spacing:.01em}
    .sidebar p{margin:0 0 15px;font-size:.85rem;color:#bed0e4}
    .sidebar .brand{padding:14px;border:1px solid rgba(255,255,255,.12);border-radius:16px;background:linear-gradient(180deg,rgba(255,255,255,.08),rgba(255,255,255,.04));margin-bottom:14px;box-shadow:var(--shadow-soft)}
    .sidebar .brand b{display:block;font-size:1rem;letter-spacing:.01em}
    .sidebar .brand span{font-size:.8rem;color:#d3e2f2}
    .sidebar .eyebrow{display:block;font-size:.68rem;text-transform:uppercase;letter-spacing:.12em;color:#86b8e2;margin-bottom:6px;font-weight:700}
    .filter-group{margin-bottom:12px}
    .filter-group label{display:block;font-size:.77rem;margin-bottom:5px;color:#e4eef8;font-weight:600}
    .filter-group select,.filter-group input{
      width:100%;padding:10px 30px 10px 12px;border-radius:12px;border:1px solid #37577b;background:#102942;color:#eef4ff;
      -webkit-appearance:none;appearance:none;background-image:
      linear-gradient(45deg, transparent 50%, #b9d0ea 50%),
      linear-gradient(135deg, #b9d0ea 50%, transparent 50%);
      background-position: calc(100% - 16px) calc(50% - 2px), calc(100% - 11px) calc(50% - 2px);
      background-size: 5px 5px, 5px 5px;background-repeat:no-repeat;
    }
    .filter-group select:focus,.filter-group input:focus{outline:none;border-color:#80b3df;box-shadow:0 0 0 3px rgba(128,179,223,.16)}
    .side-actions{display:flex;gap:8px;margin:12px 0 10px}
    .btn{border:1px solid transparent;border-radius:12px;padding:8px 12px;font-size:.78rem;cursor:pointer;transition:transform .14s ease, box-shadow .14s ease, background .14s ease}
    .btn:hover{transform:translateY(-1px)}
    .btn-reset{background:#f2a65a;color:#102a45;font-weight:700}
    .btn-top{position:fixed;right:18px;bottom:18px;background:#12385e;color:#fff;box-shadow:var(--shadow);z-index:30}
    .sidebar-stats{margin-top:12px;padding:12px;border-radius:16px;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.11);font-size:.78rem}
    .sidebar-stats b{color:#fff}
    .content{padding:20px 24px 34px;min-width:0;overflow-x:hidden}
    .header{background:linear-gradient(135deg,#103d65 0%, #184e77 52%, #205b87 100%);color:#fff;border-radius:var(--radius-lg);padding:24px 24px 22px;box-shadow:var(--shadow);border:1px solid rgba(255,255,255,.14)}
    .header-row{display:flex;justify-content:space-between;gap:14px;align-items:flex-start;flex-wrap:wrap}
    .header-main{max-width:920px}
    .header .eyebrow{display:block;font-size:.71rem;text-transform:uppercase;letter-spacing:.16em;color:#a9d0ef;font-weight:700;margin-bottom:10px}
    .header h1{margin:0;font-size:2rem;line-height:1.05;max-width:980px}
    .header-actions{display:flex;gap:8px;align-items:center}
    .btn-print{background:#eef5fc;border:1px solid rgba(255,255,255,.4);color:#12385e;font-weight:700;box-shadow:var(--shadow-soft)}
    .sub{margin-top:10px;color:#d7e8ff;font-size:1rem;max-width:880px}
    .meta{display:flex;gap:8px;flex-wrap:wrap;margin-top:16px;min-width:0}
    .pill{font-size:.78rem;background:rgba(255,255,255,.14);border:1px solid rgba(255,255,255,.24);padding:6px 10px;border-radius:999px}
    .hero-strip{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,240px),1fr));gap:12px;margin-top:18px}
    .hero-panel{background:linear-gradient(180deg,rgba(255,255,255,.16),rgba(255,255,255,.10));border:1px solid rgba(255,255,255,.18);border-radius:18px;padding:14px 15px;min-width:0;backdrop-filter:blur(6px)}
    .hero-panel .label{font-size:.7rem;letter-spacing:.12em;text-transform:uppercase;color:#b4d7f4;font-weight:700}
    .hero-panel .value{margin-top:6px;font-size:1.22rem;font-weight:800;line-height:1.15}
    .hero-panel .note{margin-top:6px;font-size:.8rem;color:#d9ebfb}
    .top-nav{display:flex;gap:8px;flex-wrap:wrap;margin-top:16px;min-width:0}
    .top-nav a{font-size:.79rem;text-decoration:none;color:#10304f;background:#eef5fc;border:1px solid #d5e3f2;padding:8px 12px;border-radius:999px;font-weight:600}
    .top-nav a:hover{background:#dbeaf9}
    .insight{margin-top:12px;padding:14px 16px;border-radius:16px;background:#edf5fd;border:1px solid #d4e4f4;font-size:.88rem;color:#163754;font-weight:700;box-shadow:var(--shadow-soft)}
    .cards{margin-top:18px;display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,190px),1fr));gap:12px;min-width:0}
    .cards.cards-primary{grid-template-columns:repeat(auto-fit,minmax(min(100%,240px),1fr))}
    .card{background:linear-gradient(180deg,#ffffff,#fbfdff);border:1px solid var(--line);border-radius:16px;padding:14px 14px 13px;box-shadow:var(--shadow-soft);min-width:0}
    .card:hover{transform:translateY(-1px);transition:all .15s ease;box-shadow:0 12px 24px rgba(13,44,78,.10)}
    .card.primary{padding:16px 16px 15px;border-top:4px solid var(--blue-strong)}
    .card.risk{border-top-color:var(--critical)}
    .card.capacity{border-top-color:var(--warning)}
    .card.value{border-top-color:var(--positive)}
    .card .k{font-size:.71rem;color:var(--muted);text-transform:uppercase;letter-spacing:.08em;font-weight:700}
    .card .v{margin-top:6px;font-size:1.38rem;font-weight:800;line-height:1.1}
    .card .s{margin-top:5px;font-size:.8rem;color:#536678}
    .section{margin-top:18px;background:linear-gradient(180deg,#ffffff,#fbfdff);border:1px solid var(--line);border-radius:18px;padding:16px;box-shadow:var(--shadow-soft);min-width:0;overflow:hidden}
    .section-head{display:flex;justify-content:space-between;gap:14px;align-items:flex-end;flex-wrap:wrap;margin-bottom:12px}
    .section-head .eyebrow{display:block;font-size:.69rem;text-transform:uppercase;letter-spacing:.14em;color:#557ca4;font-weight:700;margin-bottom:4px}
    .section-head h3{margin:0;font-size:1.12rem;line-height:1.15}
    .section-head p{margin:4px 0 0;font-size:.83rem;color:#5b6f83;max-width:760px}
    .grid2{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,440px),1fr));gap:12px;min-width:0}
    .chart-box{background:linear-gradient(180deg,#ffffff,#fbfdff);border:1px solid var(--line);border-radius:16px;padding:14px;min-width:0;overflow:hidden}
    .chart-head{display:flex;justify-content:space-between;gap:10px;align-items:flex-start;flex-wrap:wrap}
    .chart-box h4{margin:0;font-size:.96rem;color:#223d59;line-height:1.25}
    .chart-note{margin-top:5px;font-size:.78rem;color:#64788e}
    .chart-legend{display:flex;gap:8px;flex-wrap:wrap;margin-top:8px}
    .legend-chip{display:inline-flex;align-items:center;gap:6px;font-size:.74rem;color:#42566b;background:#f4f7fb;border:1px solid #dbe5ef;padding:4px 8px;border-radius:999px}
    .legend-dot{display:inline-block;width:9px;height:9px;border-radius:999px}
    .svg-chart{width:100%;height:clamp(250px,30vh,330px);min-height:250px;border-top:1px dashed #eef2f7;overflow:hidden;margin-top:10px}
    .svg-chart text{font-family:"Avenir Next","Segoe UI Variable","Trebuchet MS",sans-serif;font-size:12px}
    .svg-chart rect,.svg-chart circle,.svg-chart path{transition:opacity .12s ease}
    .chart-tooltip{
      position:fixed;z-index:80;pointer-events:none;max-width:260px;background:#0f2438;color:#f8fbff;
      border:1px solid rgba(255,255,255,.2);border-radius:8px;padding:7px 9px;font-size:.75rem;line-height:1.35;
      box-shadow:0 8px 20px rgba(12,28,45,.25)
    }
    .decision{border:1px solid #f0d5d8;background:linear-gradient(180deg,#fff8f8,#fff2f3);padding:14px;border-radius:16px;margin-top:10px;font-size:.9rem}
    .decision-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,220px),1fr));gap:12px}
    .decision-item{background:rgba(255,255,255,.72);border:1px solid #efd8db;border-radius:14px;padding:12px}
    .decision-item .label{font-size:.7rem;text-transform:uppercase;letter-spacing:.12em;color:#8f5660;font-weight:700}
    .decision-item .value{margin-top:6px;font-size:1rem;font-weight:800;color:#172b41}
    .decision-item .note{margin-top:6px;font-size:.8rem;color:#5b6775}
    .toolbar{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin:10px 0 8px}
    .toolbar input{padding:10px 12px;border:1px solid var(--line);border-radius:11px;min-width:260px;max-width:100%;background:#fff}
    .toolbar .count{font-size:.8rem;color:#35526f;background:#f1f7fd;padding:6px 9px;border-radius:999px;border:1px solid #d4e4f5}
    .pager{display:flex;gap:6px;align-items:center;flex-wrap:wrap;margin-left:auto}
    .pager .btn{background:#eff5fb;border-color:#cfe0f2;color:#1c3d5d}
    .pager .btn[disabled]{opacity:.45;cursor:not-allowed}
    .pager select{padding:7px 8px;border-radius:8px;border:1px solid var(--line);background:#fff;color:#1f3348}
    .pager .page-info{font-size:.78rem;color:#475569;background:#f8fbff;border:1px solid #dbe8f6;padding:5px 8px;border-radius:999px}
    .table-wrap{margin-top:8px;border:1px solid var(--line);border-radius:14px;overflow:auto;max-height:620px;max-width:100%;background:#fff}
    table{width:100%;border-collapse:collapse;min-width:980px}
    th,td{padding:9px 10px;border-bottom:1px solid #edf1f5;font-size:.82rem;text-align:left;white-space:nowrap}
    th{position:sticky;top:0;background:#10253b;color:#fff;cursor:pointer;z-index:2}
    tr:hover td{background:#f7fbff}
    .badge{padding:2px 8px;border-radius:999px;font-weight:600;font-size:.74rem}
    .badge-critico{background:#ffd6d9;color:#8f0014}
    .badge-alto{background:#ffe4c7;color:#9b4a00}
    .badge-medio{background:#fff3bf;color:#5f4b00}
    .badge-bajo{background:#d6f5ea;color:#0b5b46}
    .footer-note{font-size:.78rem;color:#6b7280;margin-top:10px;background:#f7f9fc;border:1px solid #e3ebf3;padding:10px 12px;border-radius:12px}
    @media (max-width:1460px){
      .cards{grid-template-columns:repeat(auto-fit,minmax(min(100%,185px),1fr))}
      .grid2{grid-template-columns:repeat(auto-fit,minmax(min(100%,380px),1fr))}
    }
    @media (max-width:1240px){
      .layout{grid-template-columns:1fr}
      .sidebar{position:relative;height:auto;max-height:none}
      .btn-top{bottom:12px;right:12px}
    }
    @media (max-width:860px){
      .content{padding:10px}
      .header h1{font-size:1.22rem}
      .hero-strip{grid-template-columns:1fr}
      .grid2{grid-template-columns:1fr}
      .cards{grid-template-columns:repeat(2,minmax(120px,1fr));gap:8px}
      .toolbar input{min-width:100%}
      .pager{width:100%;justify-content:flex-start;margin-left:0}
      .table-wrap{max-height:520px}
      table{min-width:760px}
    }
    @media print{
      body{background:#fff;color:#111}
      .sidebar,.top-nav,.btn-top,.btn-print{display:none !important}
      .layout{grid-template-columns:1fr}
      .header{box-shadow:none;border:1px solid #e1e6ef}
      .section,.card,.chart-box{box-shadow:none}
      .insight{background:#f2f5fb;border-color:#d9e2ef}
      .chart-tooltip{display:none !important}
      .section{page-break-inside:avoid}
    }
  </style>
</head>
<body>
<div class="layout">
  <aside class="sidebar">
    <div class="brand">
      <span class="eyebrow">Centro de decisión</span>
      <b>Inteligencia de Mantenimiento Ferroviario</b>
      <span>Panel ejecutivo para salud de activos, priorización de taller y valor CBM.</span>
    </div>
    <h2>Filtros Globales</h2>
    <p>Aplican sobre gráficos, ranking operativo y tabla de detalle. Los KPI superiores permanecen gobernados por la capa oficial.</p>
    <div class="filter-group"><label>Flota</label><select id="f_flota"></select></div>
    <div class="filter-group"><label>Unidad</label><select id="f_unidad"></select></div>
    <div class="filter-group"><label>Depósito recomendado</label><select id="f_deposito"></select></div>
    <div class="filter-group"><label>Familia componente</label><select id="f_familia"></select></div>
    <div class="filter-group"><label>Sistema principal</label><select id="f_sistema"></select></div>
    <div class="filter-group"><label>Nivel de riesgo</label><select id="f_riesgo"></select></div>
    <div class="filter-group"><label>Tipo intervención</label><select id="f_intervencion"></select></div>
    <div class="filter-group"><label>Ventana temporal</label><select id="f_ventana"></select></div>
    <div class="filter-group"><label>Estrategia mantenimiento</label><select id="f_estrategia"></select></div>
    <div class="side-actions">
      <button class="btn btn-reset" id="btnReset">Resetear filtros</button>
    </div>
    <div class="sidebar-stats">
      <div><b id="s_count_rows">0</b> componentes filtrados</div>
      <div><b id="s_count_units">0</b> unidades filtradas</div>
      <div><b id="s_count_high">0</b> unidades de alto riesgo</div>
    </div>
  </aside>

  <main class="content">
    <section class="header">
      <div class="header-row">
        <div class="header-main">
          <span class="eyebrow">Mantenimiento basado en condición</span>
          <h1>Sistema de Inteligencia de Mantenimiento Basado en Condición</h1>
          <div class="sub">Riesgo operativo, priorización de taller y valor estratégico CBM para una lectura ejecutiva clara y accionable.</div>
        </div>
        <div class="header-actions">
          <button class="btn btn-print" id="btnPrint">Imprimir</button>
        </div>
      </div>
      <div class="meta">
        <span class="pill">Cobertura: __COVERAGE_START__ a __COVERAGE_END__</span>
        <span class="pill">Flotas: __N_FLOTAS__</span>
        <span class="pill">Unidades: __N_UNIDADES__</span>
        <span class="pill">Depósitos: __N_DEPOSITOS__</span>
        <span class="pill">Componentes: __N_COMPONENTES__</span>
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
          <div class="note" id="hero_exposure_note">Riesgo alto, backlog crítico y presión de diferimiento.</div>
        </div>
        <div class="hero-panel">
          <div class="label">Palanca estratégica</div>
          <div class="value" id="hero_value_value">-</div>
          <div class="note" id="hero_value_note">Ahorro proxy y saturación media del taller.</div>
        </div>
      </div>
      <div class="insight" id="headerInsight">Resumen ejecutivo: salud de activos, riesgo operativo y prioridades de taller para la ventana seleccionada.</div>
      <div class="top-nav">
        <a href="#sec_saude">Salud</a>
        <a href="#sec_operacao">Servicio</a>
        <a href="#sec_taller">Taller</a>
        <a href="#sec_alertas">Inspección</a>
        <a href="#sec_estrategica">Estrategia</a>
        <a href="#sec_decisao">Decisión</a>
        <a href="#sec_tabela">Tabla</a>
      </div>
    </section>

    <section class="cards cards-primary" id="kpiCardsPrimary">
      <div class="card primary"><div class="k">Disponibilidad de flota</div><div class="v" id="k_avail">-</div><div class="s">Lectura agregada de continuidad operacional.</div></div>
      <div class="card primary risk"><div class="k">Unidades de alto riesgo</div><div class="v" id="k_uhr">-</div><div class="s">Casos que exigen vigilancia táctica inmediata.</div></div>
      <div class="card primary capacity"><div class="k">Backlog crítico físico</div><div class="v" id="k_bcf">-</div><div class="s">Carga física prioritaria pendiente de taller.</div></div>
      <div class="card primary value"><div class="k">Ahorro operativo proxy CBM</div><div class="v" id="k_ahorro">-</div><div class="s">Valor incremental frente al escenario reactivo.</div></div>
    </section>

    <section class="cards" id="kpiCards">
      <div class="card"><div class="k">MTBF</div><div class="v" id="k_mtbf">-</div><div class="s">Fiabilidad media observada.</div></div>
      <div class="card"><div class="k">MTTR</div><div class="v" id="k_mttr">-</div><div class="s">Recuperación media tras incidencia.</div></div>
      <div class="card"><div class="k">Backlog físico</div><div class="v" id="k_bf">-</div><div class="s">Órdenes reales aún abiertas.</div></div>
      <div class="card"><div class="k">Backlog vencido</div><div class="v" id="k_bv">-</div><div class="s">Carga fuera de ventana objetivo.</div></div>
      <div class="card"><div class="k">Riesgo diferimiento alto</div><div class="v" id="k_drh">-</div><div class="s">Casos donde aplazar destruye valor.</div></div>
      <div class="card"><div class="k">Exposición backlog-ajustada</div><div class="v" id="k_bea">-</div><div class="s">Presión estructural combinada del backlog.</div></div>
      <div class="card"><div class="k">Horas indisponibles evitables</div><div class="v" id="k_he">-</div><div class="s">Downtime adicional evitable por actuar antes.</div></div>
      <div class="card"><div class="k">Correctivas evitables</div><div class="v" id="k_ce">-</div><div class="s">Intervenciones reactivas que puede absorber CBM.</div></div>
      <div class="card"><div class="k">Alertas tempranas activas</div><div class="v" id="k_alertas">-</div><div class="s">Señal de vigilancia activa en la cartera.</div></div>
      <div class="card"><div class="k">Saturación media taller</div><div class="v" id="k_sat">-</div><div class="s">Presión media sobre capacidad disponible.</div></div>
    </section>

    <section class="section" id="sec_saude">
      <div class="section-head">
        <div>
          <span class="eyebrow">Salud de activos</span>
          <h3>Qué familias se degradan antes y con qué urgencia</h3>
          <p>Señales para decidir dónde intervenir primero y qué ventanas de RUL requieren protección.</p>
        </div>
      </div>
      <div class="grid2">
        <div class="chart-box">
          <div class="chart-head"><h4>Deterioro y riesgo por familia</h4></div>
          <div class="chart-note">Compara presión de riesgo frente a salud media para detectar familias con peor equilibrio.</div>
          <div class="chart-legend"><span class="legend-chip"><span class="legend-dot" style="background:#bc4749"></span>Riesgo</span><span class="legend-chip"><span class="legend-dot" style="background:#2a9d8f"></span>Salud</span></div>
          <div id="ch_family" class="svg-chart"></div>
        </div>
        <div class="chart-box">
          <div class="chart-head"><h4>Distribución de RUL por ventana de intervención</h4></div>
          <div class="chart-note">Identifica concentración de componentes en ventanas cortas y tensión del frente de trabajo.</div>
          <div id="ch_rul" class="svg-chart"></div>
        </div>
      </div>
    </section>

    <section class="section" id="sec_operacao">
      <div class="section-head">
        <div>
          <span class="eyebrow">Servicio</span>
          <h3>Qué unidades concentran más impacto operacional</h3>
          <p>Ranking filtrado para priorizar intervención donde se evita más pérdida de servicio.</p>
        </div>
      </div>
      <div class="grid2">
        <div class="chart-box"><div class="chart-head"><h4>Top unidades por prioridad de intervención</h4></div><div class="chart-note">Ordena las unidades con mayor urgencia relativa en la cartera filtrada.</div><div id="ch_top_units" class="svg-chart"></div></div>
        <div class="chart-box"><div class="chart-head"><h4>Impacto en servicio por unidad</h4></div><div class="chart-note">Muestra dónde una intervención temprana evita mayor daño al servicio.</div><div id="ch_service" class="svg-chart"></div></div>
      </div>
    </section>

    <section class="section" id="sec_taller">
      <div class="section-head">
        <div>
          <span class="eyebrow">Taller</span>
          <h3>Dónde falta capacidad y cómo se distribuye la presión operativa</h3>
          <p>Lectura conjunta de saturación, backlog y mezcla de decisiones para rebalanceo táctico.</p>
        </div>
      </div>
      <div class="grid2">
        <div class="chart-box"><div class="chart-head"><h4>Saturación por depósito</h4></div><div class="chart-note">Prioriza depósitos donde la capacidad disponible se acerca al límite operativo.</div><div id="ch_depot" class="svg-chart"></div></div>
        <div class="chart-box"><div class="chart-head"><h4>Cola de decisiones operativas</h4></div><div class="chart-note">Visualiza el mix entre inspección, intervención, observación y escalado.</div><div id="ch_decisions" class="svg-chart"></div></div>
      </div>
    </section>

    <section class="section" id="sec_alertas">
      <div class="section-head">
        <div>
          <span class="eyebrow">Inspección</span>
          <h3>Cuánta señal útil genera la inspección automática</h3>
          <p>Calidad operativa de la señal temprana y drivers dominantes del riesgo actual.</p>
        </div>
      </div>
      <div class="grid2">
        <div class="chart-box"><div class="chart-head"><h4>Calidad de inspección por familia</h4></div><div class="chart-note">Cobertura y valor pre-falla para priorizar despliegue donde la detección añade más utilidad.</div><div id="ch_inspection" class="svg-chart"></div></div>
        <div class="chart-box"><div class="chart-head"><h4>Drivers principales del riesgo</h4></div><div class="chart-note">Resume qué señales dominan la criticidad: anomalías, repetitividad o degradación.</div><div id="ch_drivers" class="svg-chart"></div></div>
      </div>
    </section>

    <section class="section" id="sec_estrategica">
      <div class="section-head">
        <div>
          <span class="eyebrow">Estrategia</span>
          <h3>Qué valor económico y operacional captura el enfoque CBM</h3>
          <p>Comparativa de estrategias y sensibilidad del coste de diferimiento para la toma de decisión directiva.</p>
        </div>
      </div>
      <div class="insight" id="strategyInsight"></div>
      <div class="grid2">
        <div class="chart-box"><div class="chart-head"><h4>Reactivo vs Preventivo vs CBM</h4></div><div class="chart-note">Comparación de disponibilidad para leer la posición relativa de cada estrategia.</div><div id="ch_strategy" class="svg-chart"></div></div>
        <div class="chart-box"><div class="chart-head"><h4>Trade-off de diferimiento</h4></div><div class="chart-note">Evolución conjunta del coste y la indisponibilidad al aplazar intervención.</div><div class="chart-legend"><span class="legend-chip"><span class="legend-dot" style="background:#bc4749"></span>Coste</span><span class="legend-chip"><span class="legend-dot" style="background:#1d4e89"></span>Indisponibilidad</span></div><div id="ch_deferral" class="svg-chart"></div></div>
      </div>
    </section>

    <section class="section" id="sec_decisao">
      <div class="section-head">
        <div>
          <span class="eyebrow">Decisión</span>
          <h3>Qué hacer primero y por qué</h3>
          <p>Bloque ejecutivo para pasar de señal analítica a acción recomendada sin ambigüedad.</p>
        </div>
      </div>
      <div class="insight" id="dynamicInsight"></div>
      <div class="decision" id="decisionBox">
        <div hidden>
          <p><strong>Unidad que debe entrar primero:</strong> __TOP_UNIT__</p>
          <p><strong>Componente que debe sustituirse primero:</strong> __TOP_COMPONENT__</p>
        </div>
        <div class="decision-grid">
          <div class="decision-item">
            <div class="label">Unidad prioritaria</div>
            <div class="value">__TOP_UNIT__</div>
            <div class="note">Entrada recomendada en la cola ejecutiva.</div>
          </div>
          <div class="decision-item">
            <div class="label">Componente prioritario</div>
            <div class="value">__TOP_COMPONENT__</div>
            <div class="note">Elemento con mayor retorno inmediato de intervención.</div>
          </div>
          <div class="decision-item">
            <div class="label">Lectura operativa</div>
            <div class="value" id="decision_readout">-</div>
            <div class="note" id="decision_note">-</div>
          </div>
        </div>
      </div>
    </section>

    <section class="section" id="sec_tabela">
      <div class="section-head">
        <div>
          <span class="eyebrow">Detalle</span>
          <h3>Tabla final interactiva</h3>
          <p>Exploración granular para revisión técnica, contraste de casos y trazabilidad de la decisión.</p>
        </div>
      </div>
      <div class="toolbar">
        <input id="searchBox" placeholder="Buscar unidad, componente, driver..." />
        <span class="count" id="resultCount">0 resultados</span>
        <div class="pager">
          <button class="btn" id="btnPrevPage">Anterior</button>
          <span class="page-info" id="pageInfo">Página 1/1</span>
          <button class="btn" id="btnNextPage">Siguiente</button>
          <label style="font-size:.78rem;color:#526479">Filas
            <select id="pageSize">
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
<button class="btn btn-top" id="btnTop">↑ Arriba</button>
<div id="chartTooltip" class="chart-tooltip" hidden></div>

<script>
const payload = __PAYLOAD__;
const baseRows = payload.rows.slice();
const fleetWeek = payload.fleet_week.slice();
const depotLatest = payload.depot_latest.slice();
const strategyData = payload.strategy.slice();
const deferralData = payload.deferral.slice();
const inspectionData = payload.inspection.slice();
const metricSnapshot = payload.metric_snapshot || {};

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
    avgAvail, avgMtbf, avgMttr, bf, bv, bcf, bea, sat, strategySlice, savings, depotSlice
  };
}

function setText(id, txt){ const el = document.getElementById(id); if(el) el.textContent = txt; }

function renderKPIs(d){
  const avail = toNum(metricSnapshot.fleet_availability_pct);
  const mtbf = toNum(metricSnapshot.mtbf_proxy_hours);
  const mttr = toNum(metricSnapshot.mttr_proxy_hours);
  const highRiskUnits = toNum(metricSnapshot.high_risk_units_count);
  const backlogPhysical = toNum(metricSnapshot.backlog_physical_items_count);
  const backlogOverdue = toNum(metricSnapshot.backlog_overdue_items_count);
  const backlogCritical = toNum(metricSnapshot.backlog_critical_physical_count);
  const deferralHigh = toNum(metricSnapshot.high_deferral_risk_cases_count);
  const exposure = toNum(metricSnapshot.backlog_exposure_adjusted_mean);
  const avoidableDowntime = toNum(metricSnapshot.deferral_downtime_delta_14d_h);
  const avoidableCorrectives = toNum(metricSnapshot.avoidable_correctives_inspection);
  const savings = toNum(metricSnapshot.cbm_operational_savings_eur);
  const alerts = toNum(metricSnapshot.early_warnings_active_count);
  const sat = toNum(metricSnapshot.mean_depot_saturation_pct);

  setText("k_avail", `${fmt2(avail)}%`);
  setText("k_mtbf", fmt2(mtbf));
  setText("k_mttr", fmt2(mttr));
  setText("k_uhr", fmt0(highRiskUnits));
  setText("k_bf", fmt0(backlogPhysical));
  setText("k_bv", fmt0(backlogOverdue));
  setText("k_bcf", fmt0(backlogCritical));
  setText("k_drh", fmt0(deferralHigh));
  setText("k_bea", fmt1(exposure));
  setText("k_he", fmt1(avoidableDowntime));
  setText("k_ce", fmt1(avoidableCorrectives));
  setText("k_ahorro", `€${fmt0(savings)}`);
  setText("k_alertas", fmt0(alerts));
  setText("k_sat", `${fmt1(sat)}%`);
  setText("s_count_rows", fmt0(d.rows.length));
  setText("s_count_units", fmt0(d.uniqueUnits.size));
  setText("s_count_high", fmt0(d.highRiskUnits.size));

  setText("hero_priority_value", `${metricSnapshot.top_unit_by_priority || "n/a"} / ${metricSnapshot.top_component_by_priority || "n/a"}`);
  setText("hero_priority_note", `${fmt0(highRiskUnits)} unidades en vigilancia alta y foco inmediato en la cola priorizada.`);
  setText("hero_exposure_value", `${fmt0(backlogCritical)} backlog crítico · ${fmt0(deferralHigh)} diferimientos altos`);
  setText("hero_exposure_note", `Exposición backlog-ajustada ${fmt1(exposure)} y presión de taller ${fmt1(sat)}%.`);
  setText("hero_value_value", `${fmtMoneyCompact(savings)} potencial`);
  setText("hero_value_note", `${fmt1(avoidableDowntime)} h evitables y ${fmt1(avoidableCorrectives)} correctivas absorbibles.`);
}

function makeSvg(containerId){
  const el = document.getElementById(containerId);
  if(!el) return null;
  const w = el.clientWidth || 420;
  const h = el.clientHeight || 240;
  el.innerHTML = `<svg width="${w}" height="${h}" viewBox="0 0 ${w} ${h}" role="img" aria-label="${containerId}"></svg>`;
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

function renderDecision(d){
  const box = document.getElementById("decisionBox");
  const insight = document.getElementById("dynamicInsight");
  const readout = document.getElementById("decision_readout");
  const note = document.getElementById("decision_note");
  if(!d.rows.length){
    insight.textContent = "Sin registros para filtros seleccionados.";
    if(readout) readout.textContent = "Sin decisión activa";
    if(note) note.textContent = "Ajusta filtros para recuperar una recomendación operativa.";
    return;
  }
  const top = d.rows.slice().sort((a,b)=>toNum(b.intervention_priority_score)-toNum(a.intervention_priority_score))[0];
  const dep = d.depotSlice.find(x => String(x.deposito_id) === String(top.deposito_recomendado));
  const sat = dep ? toNum(dep.saturation_ratio)*100 : 0;
  const backlog = dep ? toNum(dep.backlog_critical_items) : 0;
  insight.textContent = `Acción prioritaria: intervenir ${top.unidad_id}/${top.componente_id} con score ${fmt1(top.intervention_priority_score)}. El diferimiento ya expone ${fmt1(top.deferral_risk_score)} puntos de riesgo y el depósito asociado opera al ${fmt1(sat)}% de saturación.`;
  if(readout) readout.textContent = `${esc(top.decision_type)} · score ${fmt1(top.intervention_priority_score)}`;
  if(note) note.textContent = `Impacto en servicio ${fmt1(top.service_impact_score)}, riesgo de diferimiento ${fmt1(top.deferral_risk_score)} y backlog crítico local ${fmt0(backlog)}.`;
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
    `Resumen ejecutivo del recorte actual: ${d.rows.length} componentes, ${d.uniqueUnits.size} unidades y ${d.highRiskUnits.size} unidades de alto riesgo. El panel está diseñado para responder qué intervenir primero, dónde falta capacidad y cuánto valor protege el enfoque CBM.`;
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

  const depotBars = d.depotSlice.map(x => ({label:String(x.deposito_id), value:toNum(x.saturation_ratio)*100}));
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
}

function renderTable(d){
  const head = document.getElementById("tableHead");
  const body = document.getElementById("tableBody");
  if(!head.dataset.ready){
    head.innerHTML = tableColumns.map(c => `<th data-col="${c}">${tableLabels[c] || c}</th>`).join("");
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
    body.innerHTML = `<tr><td colspan="${tableColumns.length}" style="text-align:center;color:#6b7280;padding:20px">Sin resultados para los filtros actuales</td></tr>`;
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
  renderInsights(d);
  renderDecision(d);
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
            "  <title>Dashboard Ejecutivo</title>",
            "</head>",
            "<body>",
            "  <p>Redirigiendo al dashboard ejecutivo...</p>",
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
