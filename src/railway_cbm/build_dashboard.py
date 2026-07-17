"""Genera el panel HTML autocontenido a partir de las tablas canónicas.

El módulo separa tres responsabilidades:

1. ``_build_payload`` prepara los datos canónicos y el registro oficial de métricas.
2. ``_CSS`` / ``_BODY`` / ``_SCRIPT`` describen la capa de presentación.
3. ``build_dashboard`` compone el HTML final y el redirector público.
"""

from __future__ import annotations

import base64
import json
from hashlib import sha1

import numpy as np
import pandas as pd

from railway_cbm.config import DATA_PROCESSED_DIR, DATA_RAW_DIR, OUTPUTS_DASHBOARD_DIR, ROOT_DIR
from railway_cbm.reporting_governance import load_or_compute_narrative_metrics

DASHBOARD_SLUG = "centro-control-mantenimiento-ferroviario.html"

# Columnas del payload de componentes y precisión de publicación. Redondear en origen
# evita precisión falsa en la interfaz y reduce el peso del artefacto.
_ROW_COLUMNS = [
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
    "decision_rule_id",
    "recommended_action_initial",
    "time_window",
    "suggested_window_days",
    "recommended_entry_sequence",
    "intervention_priority_score",
    "deferral_risk_score",
    "service_impact_score",
    "workshop_fit_score",
    "health_score",
    "prob_fallo_30d",
    "component_rul_estimate",
    "hours_required",
    "coste_retraso_proxy",
    "main_risk_driver",
    "confidence_flag",
    "estrategia_mantenimiento_actual",
]
_ROW_ROUNDING = {
    "intervention_priority_score": 2,
    "deferral_risk_score": 2,
    "service_impact_score": 2,
    "workshop_fit_score": 2,
    "health_score": 2,
    "prob_fallo_30d": 4,
    "component_rul_estimate": 1,
    "hours_required": 2,
    "coste_retraso_proxy": 0,
}


def _json_for_script(value: object) -> str:
    """Serializa JSON sin permitir que los datos cierren el bloque ``script``."""
    return json.dumps(value, ensure_ascii=False).replace("&", "\\u0026").replace("<", "\\u003c").replace(">", "\\u003e")


def _embedded_font_faces() -> str:
    fonts_dir = ROOT_DIR / "assets" / "fonts"
    specs = [
        ("sans.woff2", "IBM Plex Sans", "normal", "100 700"),
        ("mono500.woff2", "IBM Plex Mono", "normal", "500"),
        ("mono600.woff2", "IBM Plex Mono", "normal", "600"),
    ]
    blocks = []
    for file_name, family, style, weight in specs:
        data = (fonts_dir / file_name).read_bytes()
        b64 = base64.b64encode(data).decode("ascii")
        blocks.append(
            f"@font-face{{font-family:'{family}';font-style:{style};font-weight:{weight};font-display:swap;"
            f"src:url(data:font/woff2;base64,{b64}) format('woff2');}}"
        )
    return "\n    ".join(blocks)


def _risk_tier(score: float) -> str:
    if score >= 80:
        return "Crítico"
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
    txt = f"{row.get('sistema_principal', '')} {row.get('subsistema', '')} {row.get('tipo_componente', '')}".lower()
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
    """Crea una columna canónica con el primer candidato no nulo y elimina alternas."""
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


def _build_payload() -> tuple[dict, dict]:
    """Devuelve el payload embebido y el contexto de plantilla del panel."""
    flotas = pd.read_csv(DATA_RAW_DIR / "flotas.csv")
    unidades = pd.read_csv(DATA_RAW_DIR / "unidades.csv")
    depositos = pd.read_csv(DATA_RAW_DIR / "depositos.csv")
    componentes = pd.read_csv(DATA_RAW_DIR / "componentes_criticos.csv")
    backlog_raw = pd.read_csv(
        DATA_RAW_DIR / "backlog_mantenimiento.csv",
        usecols=["fecha", "backlog_id", "deposito_id", "unidad_id", "componente_id"],
    )

    fleet_week = pd.read_csv(DATA_PROCESSED_DIR / "fleet_week_features.csv")
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
    depot_pressure["fecha"] = pd.to_datetime(depot_pressure["fecha"], errors="coerce")

    latest_depot_calendar = _latest_frame(depot_pressure, "fecha")
    backlog_cols = ["backlog_physical_items", "backlog_overdue_items", "backlog_critical_items"]
    valid_depot_pressure = depot_pressure[depot_pressure[backlog_cols].fillna(0).sum(axis=1) > 0].copy()
    latest_depot = (
        _latest_frame(valid_depot_pressure, "fecha") if not valid_depot_pressure.empty else latest_depot_calendar.copy()
    )
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
    base = _coalesce_columns(
        base, "sistema_principal", ["sistema_principal", "sistema_principal_x", "sistema_principal_y"]
    )
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
        "nombre_deposito",
        "capacidad_taller",
        "saturation_ratio",
        "backlog_physical_items",
        "backlog_overdue_items",
        "backlog_critical_items",
        "backlog_exposure_adjusted_score",
    ]
    latest_depot = latest_depot[[c for c in dep_cols if c in latest_depot.columns]].copy()

    coverage_start = str(metrics.get("coverage_start", fleet_week["week_start"].min().date().isoformat()))
    coverage_end = str(metrics.get("coverage_end", fleet_week["week_start"].max().date().isoformat()))

    fleet_payload = fleet_week[["week_start", "flota_id", "availability_rate", "mtbf_proxy", "mttr_proxy"]].copy()
    fleet_payload["week_start"] = fleet_payload["week_start"].dt.strftime("%Y-%m-%d")
    fleet_payload["availability_rate"] = fleet_payload["availability_rate"].round(5)
    fleet_payload["mtbf_proxy"] = fleet_payload["mtbf_proxy"].round(1)
    fleet_payload["mttr_proxy"] = fleet_payload["mttr_proxy"].round(2)

    signature_cols = [
        "unidad_id",
        "componente_id",
        "intervention_priority_score",
        "deferral_risk_score",
        "health_score",
        "prob_fallo_30d",
        "component_rul_estimate",
    ]
    signature_input = base[signature_cols].sort_values(["unidad_id", "componente_id"]).to_csv(index=False) + json.dumps(
        metrics, sort_keys=True, default=str
    )
    payload_signature = sha1(signature_input.encode("utf-8"), usedforsecurity=False).hexdigest()[:10]
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
        f"{backlog_duplicate_count} órdenes duplicadas sobre clave={backlog_key}"
        if backlog_duplicate_count > 0
        else "sin órdenes duplicadas en el corte de pendientes"
    )
    redesigned = scheduling_metrics.loc[scheduling_metrics["scenario"] == "heuristica_redisenada_35d"]
    pending_capacity_pct = float(redesigned["pendiente_capacidad_pct"].iloc[0]) if not redesigned.empty else 0.0

    anomalies = [
        {
            "severity": "critical",
            "title": "Pendientes críticos físicos",
            "value": f"{int(metrics.get('backlog_critical_physical_count', 0)):,}".replace(",", "."),
            "description": f"{int(metrics.get('backlog_overdue_items_count', 0)):,}".replace(",", ".")
            + " pendientes vencidos sobre "
            + f"{int(metrics.get('backlog_physical_items_count', 0)):,}".replace(",", ".")
            + " físicos abiertos.",
        },
        {
            "severity": "warning",
            "title": "Cola no absorbida por capacidad",
            "value": f"{pending_capacity_pct:.1f}%",
            "description": "de los casos sigue sin ventana de taller con la heurística rediseñada.",
        },
        {
            "severity": "info",
            "title": "Corte válido de pendientes",
            "value": str(latest_depot_valid_date.date()) if pd.notna(latest_depot_valid_date) else "n/a",
            "description": "el calendario más reciente no trae pendientes físicos; el panel usa el último corte con carga real.",
        },
        {
            "severity": "warning" if backlog_duplicate_count > 0 else "info",
            "title": "Cobertura de priorización",
            "value": f"{priority_non_null_rate:.1f}%",
            "description": duplicate_backlog_issue,
        },
    ]

    rows = base[_ROW_COLUMNS].copy()
    for column, decimals in _ROW_ROUNDING.items():
        rows[column] = pd.to_numeric(rows[column], errors="coerce").round(decimals)
        if decimals == 0:
            rows[column] = rows[column].astype("Int64")

    payload = {
        "rows": rows.fillna("").to_dict(orient="records"),
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
            "cbm_vs_reactiva_availability_pp": float(metrics.get("cbm_vs_reactiva_availability_pp", 0.0)),
            "cbm_breakeven_value_per_service_hour_eur": float(
                metrics.get("cbm_breakeven_value_per_service_hour_eur", 0.0)
            ),
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
            "latest_depot_calendar_date": str(latest_depot_calendar_date.date())
            if pd.notna(latest_depot_calendar_date)
            else "",
            "latest_depot_calendar_zero_backlog": latest_depot_calendar_zero_backlog,
        },
    }

    context = {
        "__COVERAGE_START__": coverage_start,
        "__COVERAGE_END__": coverage_end,
        "__N_FLOTAS__": str(int(float(metrics.get("n_flotas", flotas["flota_id"].nunique())))),
        "__N_UNIDADES__": str(int(float(metrics.get("n_unidades", unidades["unidad_id"].nunique())))),
        "__N_DEPOSITOS__": str(int(float(metrics.get("n_depositos", depositos["deposito_id"].nunique())))),
        "__N_COMPONENTES__": str(int(float(metrics.get("n_componentes", componentes["componente_id"].nunique())))),
        "__TOP_UNIT__": str(metrics.get("top_unit_by_priority", "n/a")),
        "__TOP_COMPONENT__": str(metrics.get("top_component_by_priority", "n/a")),
        "__DASHBOARD_VERSION__": dashboard_version,
        "__PAYLOAD_SIGNATURE__": payload_signature,
    }
    return payload, context


# ---------------------------------------------------------------------------
# Capa de presentación
# ---------------------------------------------------------------------------

# Paleta verificada con el validador de contraste/CVD del método de visualización:
# marcas (acento, salud, aviso, crítico) superan 3:1 y ΔE>=12 en ambos modos; las
# rampas ordinales son monotónas en luminosidad y anclan al fondo de cada modo.
_CSS = """
    __FONT_FACES__
    :root{
      color-scheme:light;
      --font-sans:"IBM Plex Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
      --font-mono:"IBM Plex Mono",ui-monospace,"SF Mono",Menlo,monospace;
      --page:#f1efea;--surface:#fbfaf8;--surface-2:#f5f3ef;--surface-3:#eceae4;
      --ink:#15181c;--ink-2:#3d434b;--ink-3:#6b7280;--ink-4:#9aa0a8;
      --rule:#e2dfd7;--rule-2:#cfcbc1;
      --accent:#2c5fa8;--accent-ink:#ffffff;--accent-wash:#e9eef7;--accent-rule:#c4d3ea;
      --good:#0f7a52;--good-wash:#e6f0ea;--good-rule:#bfd8cb;
      --amber:#a66a00;--amber-wash:#f6eedd;--amber-rule:#e2d1ab;
      --critical:#b3261e;--critical-wash:#f8e9e6;--critical-rule:#ebcac4;
      --mark-mute:#8a8f98;
      --ember-1:#df9880;--ember-2:#cb7350;--ember-3:#ac4525;--ember-4:#7a2610;
      --grid:#e7e4dc;--axis:#cfcbc1;
      --tip-bg:#15181c;--tip-ink:#fbfaf8;--tip-rule:#3d434b;
      --shadow:0 1px 2px rgba(21,24,28,.05),0 8px 24px rgba(21,24,28,.08);
      --sidebar-width:268px;--radius-lg:10px;--radius-md:8px;--radius-sm:6px;
    }
    [data-theme="dark"]{
      color-scheme:dark;
      --page:#0c0e10;--surface:#16191c;--surface-2:#1c2024;--surface-3:#22262b;
      --ink:#f2f3f4;--ink-2:#c7cbd1;--ink-3:#9198a1;--ink-4:#6b727b;
      --rule:#282d33;--rule-2:#3a4046;
      --accent:#5e90de;--accent-ink:#0b0d0f;--accent-wash:#15233a;--accent-rule:#2a3f60;
      --good:#3ea878;--good-wash:#0e211a;--good-rule:#1e4433;
      --amber:#bf8a2e;--amber-wash:#231a0c;--amber-rule:#463514;
      --critical:#e8635a;--critical-wash:#2a1513;--critical-rule:#54201c;
      --mark-mute:#6e757e;
      --ember-1:#8c4028;--ember-2:#b25b3d;--ember-3:#d87e54;--ember-4:#f2ac94;
      --grid:#23272c;--axis:#343a41;
      --tip-bg:#f2f3f4;--tip-ink:#16191c;--tip-rule:#c7cbd1;
      --shadow:0 1px 2px rgba(0,0,0,.5),0 8px 24px rgba(0,0,0,.4);
    }
    *{box-sizing:border-box}
    html{scroll-behavior:smooth}
    body{margin:0;background:var(--page);color:var(--ink);font-family:var(--font-sans);font-weight:420;
      overflow-x:hidden;line-height:1.5;-webkit-font-smoothing:antialiased;font-feature-settings:"cv05","ss01"}
    button,select,input,a{touch-action:manipulation;font-family:inherit}
    button:focus-visible,select:focus-visible,input:focus-visible,a:focus-visible{outline:2px solid var(--accent);outline-offset:2px}
    .mono{font-family:var(--font-mono);font-weight:500}
    .tnum{font-variant-numeric:tabular-nums}
    .skip-link{position:fixed;left:14px;top:14px;z-index:100;background:var(--surface);color:var(--ink);padding:9px 13px;
      border-radius:var(--radius-sm);border:1px solid var(--rule-2);box-shadow:var(--shadow);transform:translateY(-180%);transition:transform .16s ease}
    .skip-link:focus-visible{transform:translateY(0)}

    /* ---------- estructura ---------- */
    .layout{display:grid;grid-template-columns:minmax(260px,var(--sidebar-width)) minmax(0,1fr);gap:22px;
      min-height:100svh;width:100%;align-items:start;padding:22px}
    body.filters-collapsed .layout{grid-template-columns:minmax(0,1fr)}
    .content{padding:0 0 40px;min-width:0;overflow-x:hidden;max-width:1560px;width:100%}
    .section{margin-top:38px;min-width:0;scroll-margin-top:16px}
    .section-head{display:grid;grid-template-columns:auto minmax(0,1fr) auto;gap:4px 14px;align-items:start;
      margin-bottom:16px;padding-bottom:11px;border-bottom:1px solid var(--rule-2)}
    .sec-index{font-family:var(--font-mono);font-weight:600;font-size:.72rem;letter-spacing:.06em;color:var(--accent);
      line-height:1.9}
    .section-head h3{margin:0;font-size:1.14rem;line-height:1.2;font-weight:600;letter-spacing:-.016em;text-wrap:balance}
    .section-head p{margin:6px 0 0;font-size:.82rem;color:var(--ink-3);max-width:74ch;line-height:1.5}
    .head-aside{font-family:var(--font-mono);font-size:.72rem;color:var(--ink-3);text-align:right;line-height:1.9;
      font-variant-numeric:tabular-nums;align-self:end}
    .head-aside b{color:var(--ink);font-weight:600}

    /* ---------- barra lateral ---------- */
    .sidebar{background:var(--surface);color:var(--ink);padding:20px 18px;position:sticky;top:22px;height:auto;
      overflow:visible;border:1px solid var(--rule);border-radius:var(--radius-lg);align-self:start;min-width:0}
    body.filters-collapsed .sidebar{display:none}
    .brand{padding-bottom:15px;border-bottom:1px solid var(--rule);margin-bottom:16px}
    .brand b{display:block;font-size:.95rem;letter-spacing:-.014em;line-height:1.3;font-weight:600;margin-top:7px}
    .brand span{display:block;font-size:.76rem;color:var(--ink-3);line-height:1.45;margin-top:5px}
    .eyebrow{display:block;font-family:var(--font-mono);font-weight:500;font-size:.65rem;text-transform:uppercase;
      letter-spacing:.12em;color:var(--accent)}
    .filter-set{margin-bottom:16px}
    .filter-set > legend,.filter-set > .set-label{display:block;font-family:var(--font-mono);font-size:.62rem;
      text-transform:uppercase;letter-spacing:.11em;color:var(--ink-4);margin-bottom:9px}
    .filter-group{margin-bottom:10px;min-width:0}
    .filter-group label{display:block;font-size:.72rem;margin-bottom:4px;color:var(--ink-2);font-weight:500}
    .filter-group select,.filter-group input{width:100%;padding:8px 30px 8px 10px;border-radius:var(--radius-sm);
      border:1px solid var(--rule-2);background:var(--surface-2);color:var(--ink);font-size:.8rem;
      -webkit-appearance:none;appearance:none;
      background-image:linear-gradient(45deg,transparent 50%,var(--ink-3) 50%),linear-gradient(135deg,var(--ink-3) 50%,transparent 50%);
      background-position:calc(100% - 14px) calc(50% - 1px),calc(100% - 10px) calc(50% - 1px);
      background-size:4px 4px,4px 4px;background-repeat:no-repeat;transition:border-color .14s ease,box-shadow .14s ease}
    .filter-group select:hover{border-color:var(--ink-4)}
    .filter-group select:focus{outline:none;border-color:var(--accent);box-shadow:0 0 0 3px var(--accent-wash)}
    .side-actions{display:flex;gap:8px;margin:4px 0 14px}
    .btn{border:1px solid var(--rule-2);background:var(--surface);color:var(--ink);border-radius:var(--radius-sm);
      padding:8px 12px;font-size:.78rem;font-weight:500;cursor:pointer;
      transition:background .14s ease,border-color .14s ease,color .14s ease}
    .btn:hover{background:var(--surface-2);border-color:var(--ink-4)}
    .btn-reset{width:100%;background:var(--accent);border-color:var(--accent);color:var(--accent-ink);font-weight:600}
    .btn-reset:hover{background:var(--accent);border-color:var(--accent);filter:brightness(1.06)}
    .btn-icon{display:inline-flex;align-items:center;justify-content:center;width:34px;height:34px;padding:0}
    .btn-icon svg{width:15px;height:15px;stroke:currentColor;fill:none;stroke-width:1.6;stroke-linecap:round;stroke-linejoin:round}
    .btn-top{position:fixed;right:20px;bottom:20px;background:var(--surface);border:1px solid var(--rule-2);color:var(--ink);
      box-shadow:var(--shadow);z-index:30;opacity:0;pointer-events:none;transform:translateY(8px);
      transition:opacity .18s ease,transform .18s ease}
    .btn-top.visible{opacity:1;pointer-events:auto;transform:translateY(0)}
    .sidebar-stats{padding-top:14px;border-top:1px solid var(--rule);display:grid;gap:8px}
    .sidebar-stats div{display:flex;justify-content:space-between;align-items:baseline;gap:8px;font-size:.76rem;color:var(--ink-3)}
    .sidebar-stats b{color:var(--ink);font-family:var(--font-mono);font-size:.9rem;font-weight:600;font-variant-numeric:tabular-nums}

    /* ---------- cabecera ---------- */
    .header{min-width:0}
    .header-row{display:flex;justify-content:space-between;gap:20px;align-items:flex-start;flex-wrap:wrap}
    .header-main{max-width:78ch;min-width:0}
    .header h1{margin:7px 0 0;font-size:1.62rem;line-height:1.12;font-weight:600;letter-spacing:-.026em;text-wrap:balance}
    .header-actions{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
    .sub{margin:9px 0 0;color:var(--ink-3);font-size:.9rem;max-width:76ch;line-height:1.55}
    .rule-track{height:9px;margin:18px 0 0;min-width:0;border-bottom:1px solid var(--rule-2);
      background-image:repeating-linear-gradient(90deg,var(--rule-2) 0 1px,transparent 1px 10px);
      background-position:bottom;background-repeat:repeat-x;background-size:100% 5px}
    .meta{display:flex;gap:18px;flex-wrap:wrap;margin-top:12px;min-width:0;font-family:var(--font-mono);font-size:.71rem;
      color:var(--ink-3);font-variant-numeric:tabular-nums}
    .meta span b{color:var(--ink-2);font-weight:600}
    .top-nav{display:flex;gap:2px;flex-wrap:wrap;margin-top:16px;min-width:0}
    .top-nav a{font-size:.77rem;text-decoration:none;color:var(--ink-3);padding:6px 10px;border-radius:var(--radius-sm);
      font-weight:500;transition:background .14s ease,color .14s ease}
    .top-nav a:hover{background:var(--surface-3);color:var(--ink)}
    .top-nav a b{font-family:var(--font-mono);font-weight:500;color:var(--ink-4);margin-right:6px;font-size:.68rem}
    .filter-state{margin-top:12px;padding:8px 11px;border-radius:var(--radius-sm);background:var(--accent-wash);
      border:1px solid var(--accent-rule);font-size:.77rem;color:var(--ink-2);line-height:1.5}

    /* ---------- decisión ---------- */
    .decision-grid{display:grid;grid-template-columns:minmax(0,1.55fr) minmax(280px,1fr);gap:14px;align-items:stretch}
    .order-card,.state-card{border:1px solid var(--rule);border-radius:var(--radius-lg);background:var(--surface);
      padding:18px 19px;min-width:0;display:flex;flex-direction:column}
    .order-card{border-left:2px solid var(--critical)}
    .order-head{display:flex;justify-content:space-between;gap:12px;align-items:baseline;flex-wrap:wrap}
    .order-tag{font-family:var(--font-mono);font-size:.65rem;text-transform:uppercase;letter-spacing:.11em;color:var(--critical)}
    .order-rule{font-family:var(--font-mono);font-size:.68rem;color:var(--ink-4);overflow-wrap:anywhere}
    .order-id{margin-top:10px;font-size:clamp(1.7rem,3vw,2.35rem);line-height:1.02;font-weight:600;letter-spacing:-.035em;
      overflow-wrap:anywhere}
    .order-note{margin:9px 0 0;color:var(--ink-3);font-size:.84rem;line-height:1.55;max-width:70ch}
    .order-route{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:1px;margin-top:15px;background:var(--rule);
      border:1px solid var(--rule);border-radius:var(--radius-md);overflow:hidden}
    .route-item{background:var(--surface-2);padding:9px 11px;min-width:0}
    .route-item b{display:block;font-family:var(--font-mono);font-size:.61rem;text-transform:uppercase;letter-spacing:.09em;
      color:var(--ink-4);font-weight:500}
    .route-item span{display:block;margin-top:4px;font-size:.83rem;font-weight:500;overflow-wrap:anywhere}
    .evidence{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin-top:16px}
    .ev{min-width:0}
    .ev b{display:block;font-family:var(--font-mono);font-size:.61rem;text-transform:uppercase;letter-spacing:.09em;
      color:var(--ink-4);font-weight:500}
    .ev .ev-val{margin-top:5px;font-size:1.02rem;font-weight:600;letter-spacing:-.015em;font-variant-numeric:tabular-nums}
    .ev-track{margin-top:6px;height:4px;border-radius:2px;background:var(--surface-3);overflow:hidden}
    .ev-fill{height:100%;border-radius:2px}
    .ev .ev-note{display:block;margin-top:5px;font-size:.7rem;color:var(--ink-4);line-height:1.35}
    .order-ssot{margin-top:auto;padding-top:14px;border-top:1px solid var(--rule);font-size:.74rem;color:var(--ink-4);line-height:1.6}
    .order-ssot p{margin:0}
    .order-ssot strong{color:var(--ink-3);font-weight:500}
    .order-ssot .ssot-note{margin-top:5px;color:var(--ink-4)}
    .state-card h2,.order-card h2{margin:0;font-family:var(--font-mono);font-size:.65rem;text-transform:uppercase;
      letter-spacing:.11em;color:var(--ink-4);font-weight:500}
    .state-card p{margin:10px 0 0;color:var(--ink-2);font-size:.84rem;line-height:1.6}
    .state-list{margin-top:14px;display:grid;gap:1px;background:var(--rule);border:1px solid var(--rule);
      border-radius:var(--radius-md);overflow:hidden}
    .state-list div{background:var(--surface-2);padding:9px 11px;display:flex;justify-content:space-between;gap:10px;
      align-items:baseline;font-size:.78rem;color:var(--ink-3);min-width:0}
    .state-list b{font-family:var(--font-mono);font-weight:500;font-size:.61rem;text-transform:uppercase;letter-spacing:.09em;
      color:var(--ink-4)}
    .state-list span{color:var(--ink);font-weight:500;text-align:right;overflow-wrap:anywhere}
    .anomaly-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,220px),1fr));gap:12px;margin-top:14px}
    .anomaly{border:1px solid var(--rule);border-radius:var(--radius-md);background:var(--surface);padding:13px 14px;min-width:0}
    .anomaly .flag{display:flex;align-items:center;gap:7px;font-family:var(--font-mono);font-size:.61rem;text-transform:uppercase;
      letter-spacing:.09em;color:var(--ink-4)}
    .anomaly .dot{width:6px;height:6px;border-radius:50%;background:var(--mark-mute);flex:none}
    .anomaly.critical .dot{background:var(--critical)}
    .anomaly.warning .dot{background:var(--amber)}
    .anomaly.info .dot{background:var(--accent)}
    .anomaly .value{margin-top:8px;font-size:1.18rem;font-weight:600;letter-spacing:-.02em;font-variant-numeric:tabular-nums}
    .anomaly.critical .value{color:var(--critical)}
    .anomaly.warning .value{color:var(--amber)}
    .anomaly .text{margin-top:5px;font-size:.76rem;color:var(--ink-3);line-height:1.45}

    /* ---------- indicadores ---------- */
    .tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,224px),1fr));gap:12px;min-width:0}
    .tile{background:var(--surface);border:1px solid var(--rule);border-radius:var(--radius-md);padding:15px 16px;min-width:0;
      overflow:hidden;display:flex;flex-direction:column;transition:border-color .14s ease}
    .tile:hover{border-color:var(--rule-2)}
    .tile-head{display:flex;justify-content:space-between;gap:8px;align-items:baseline}
    .tile .k{font-size:.73rem;color:var(--ink-3);font-weight:500;min-width:0}
    .prov{font-family:var(--font-mono);font-size:.56rem;text-transform:uppercase;letter-spacing:.09em;color:var(--ink-4);
      border:1px solid var(--rule);border-radius:3px;padding:1px 4px;flex:none}
    .prov-official{color:var(--accent);border-color:var(--accent-rule);background:var(--accent-wash)}
    .tile .v{margin-top:11px;font-size:1.95rem;font-weight:600;line-height:1;letter-spacing:-.03em}
    .tile.risk .v{color:var(--critical)}
    .tile.capacity .v{color:var(--amber)}
    .tile.value.pos .v{color:var(--good)}
    .tile.value.neg .v{color:var(--critical)}
    .tile .s{margin-top:8px;font-size:.75rem;color:var(--ink-3);line-height:1.45}
    .tile .delta{display:inline-flex;align-items:center;gap:5px;margin-top:9px;font-size:.72rem;font-weight:600;
      font-variant-numeric:tabular-nums}
    .tile .delta.up{color:var(--good)}
    .tile .delta.down{color:var(--critical)}
    .tile .delta svg{width:9px;height:9px;fill:currentColor}
    .tile-foot{margin-top:auto;padding-top:14px}
    .tile-spark{height:46px;min-width:0}
    .tile-mini{height:22px;min-width:0}
    .t-track{height:4px;border-radius:2px;background:var(--surface-3);overflow:hidden}
    .t-fill{height:100%;border-radius:2px;background:var(--accent)}
    .t-note{display:block;margin-top:7px;font-size:.68rem;color:var(--ink-4);line-height:1.4}
    .ribbon{margin-top:12px;display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:1px;background:var(--rule);
      border:1px solid var(--rule);border-radius:var(--radius-md);overflow:hidden}
    @media (min-width:1360px){.ribbon{grid-template-columns:repeat(8,minmax(0,1fr))}}
    @media (max-width:720px){.ribbon{grid-template-columns:repeat(2,minmax(0,1fr))}}
    .ribbon .cell{background:var(--surface);padding:12px 13px;min-width:0}
    .ribbon .cell .k{font-size:.68rem;color:var(--ink-3);line-height:1.35}
    .ribbon .cell .v{margin-top:7px;font-size:1.16rem;font-weight:600;letter-spacing:-.02em;font-variant-numeric:tabular-nums}
    .ribbon .cell .s{margin-top:3px;font-size:.67rem;color:var(--ink-4)}

    /* ---------- gráficos ---------- */
    .grid2{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,420px),1fr));gap:14px;min-width:0}
    .chart{background:var(--surface);border:1px solid var(--rule);border-radius:var(--radius-md);padding:16px 17px 14px;
      min-width:0;overflow:hidden;display:flex;flex-direction:column}
    .chart h4{margin:0;font-size:.92rem;line-height:1.3;letter-spacing:-.012em;font-weight:600}
    .chart-note{margin-top:5px;font-size:.77rem;color:var(--ink-3);line-height:1.5;max-width:66ch}
    .legend{display:flex;gap:14px;flex-wrap:wrap;margin-top:11px}
    .legend span{display:inline-flex;align-items:center;gap:6px;font-size:.72rem;color:var(--ink-3)}
    .legend i{width:9px;height:9px;border-radius:2px;flex:none;font-style:normal}
    /* El lienzo crece con la tarjeta: en una fila de alturas desiguales el gráfico ocupa
       el espacio sobrante en lugar de dejar un hueco muerto bajo la última barra. */
    .plot{width:100%;height:clamp(232px,25vh,288px);flex:1 1 auto;margin-top:12px;padding-top:10px;
      border-top:1px solid var(--rule);min-width:0}
    .plot-tall{height:clamp(300px,34vh,364px)}
    .plot-short{height:180px;min-height:180px}
    .plot text{font-family:var(--font-mono);font-weight:500;font-size:10.5px;font-variant-numeric:tabular-nums}
    .plot rect,.plot circle,.plot path{transition:opacity .12s ease}
    .chart-tooltip{position:fixed;z-index:80;pointer-events:none;max-width:280px;background:var(--tip-bg);color:var(--tip-ink);
      border:1px solid var(--tip-rule);border-radius:var(--radius-sm);padding:8px 10px;font-size:.74rem;line-height:1.5;
      white-space:pre-line;box-shadow:var(--shadow)}
    .callout{margin-top:14px;padding:13px 15px;border-radius:var(--radius-md);background:var(--surface);border:1px solid var(--rule);
      border-left:2px solid var(--accent);font-size:.85rem;color:var(--ink-2);line-height:1.6}
    .callout b{color:var(--ink);font-weight:600;font-variant-numeric:tabular-nums}

    /* ---------- tabla ---------- */
    .toolbar{display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin:0 0 10px}
    .toolbar input{padding:9px 12px;border:1px solid var(--rule-2);border-radius:var(--radius-sm);min-width:280px;max-width:100%;
      background:var(--surface);color:var(--ink);font-size:.83rem}
    .toolbar input:focus{outline:none;border-color:var(--accent);box-shadow:0 0 0 3px var(--accent-wash)}
    .toolbar .count{font-family:var(--font-mono);font-size:.73rem;color:var(--ink-3);font-variant-numeric:tabular-nums}
    .pager{display:flex;gap:6px;align-items:center;flex-wrap:wrap;margin-left:auto}
    .pager .btn{padding:6px 10px}
    .pager .btn[disabled]{opacity:.4;cursor:not-allowed}
    .pager select{padding:6px 8px;border-radius:var(--radius-sm);border:1px solid var(--rule-2);background:var(--surface);color:var(--ink);
      font-size:.76rem}
    .pager .page-info{font-family:var(--font-mono);font-size:.73rem;color:var(--ink-3);font-variant-numeric:tabular-nums}
    .pager-label{font-size:.74rem;color:var(--ink-3);display:inline-flex;align-items:center;gap:6px}
    .table-wrap{border:1px solid var(--rule);border-radius:var(--radius-md);overflow:auto;max-height:640px;max-width:100%;
      background:var(--surface)}
    .empty-row{text-align:center;color:var(--ink-3);padding:28px}
    table{width:100%;border-collapse:collapse;min-width:1080px}
    th,td{padding:9px 12px;border-bottom:1px solid var(--rule);font-size:.79rem;text-align:left;white-space:nowrap}
    td{color:var(--ink-2)}
    td.num,th.num{font-family:var(--font-mono);font-variant-numeric:tabular-nums;text-align:right}
    th{position:sticky;top:0;background:var(--surface-2);color:var(--ink-3);cursor:pointer;z-index:2;font-weight:600;
      font-size:.66rem;text-transform:uppercase;letter-spacing:.07em;border-bottom:1px solid var(--rule-2);
      font-family:var(--font-mono)}
    th:hover{color:var(--ink)}
    th[data-sort]{color:var(--accent)}
    tbody tr:hover td{background:var(--surface-2)}
    .badge{padding:2px 8px;border-radius:3px;font-weight:500;font-size:.7rem;display:inline-block;border:1px solid transparent;
      font-family:var(--font-mono)}
    .badge-critico{background:var(--critical-wash);color:var(--critical);border-color:var(--critical-rule)}
    .badge-alto{background:var(--amber-wash);color:var(--amber);border-color:var(--amber-rule)}
    .badge-medio{background:var(--surface-3);color:var(--ink-2);border-color:var(--rule-2)}
    .badge-bajo{background:var(--good-wash);color:var(--good);border-color:var(--good-rule)}
    .footer-note{font-size:.75rem;color:var(--ink-4);margin-top:14px;line-height:1.6;max-width:92ch}
    .footer-note b{font-family:var(--font-mono);font-weight:500;color:var(--ink-3)}

    /* ---------- carga escalonada ---------- */
    @keyframes rise{from{opacity:0;transform:translateY(7px)}to{opacity:1;transform:translateY(0)}}
    body.first-paint .section,body.first-paint .header{animation:rise .42s cubic-bezier(.22,.61,.36,1) backwards}
    body.first-paint .header{animation-delay:0s}
    body.first-paint #sec_decision{animation-delay:.05s}
    body.first-paint #sec_estado{animation-delay:.1s}
    body.first-paint #sec_riesgo{animation-delay:.14s}

    @media (max-width:1400px){
      .grid2{grid-template-columns:repeat(auto-fit,minmax(min(100%,360px),1fr))}
      .evidence{grid-template-columns:repeat(2,minmax(0,1fr))}
    }
    @media (max-width:1200px){
      .layout{padding:14px;grid-template-columns:1fr}
      .sidebar{position:relative;top:auto;order:2}
      .content{order:1}
      .decision-grid{grid-template-columns:1fr}
      .btn-top{right:12px;bottom:12px}
    }
    @media (max-width:820px){
      .content{padding:0 0 20px}
      .header h1{font-size:1.32rem}
      .section{margin-top:28px}
      .section-head{grid-template-columns:auto minmax(0,1fr);row-gap:8px}
      .head-aside{grid-column:1/-1;text-align:left}
      .grid2{grid-template-columns:1fr}
      .tiles{grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}
      .tile .v{font-size:1.6rem}
      .order-route{grid-template-columns:1fr}
      .toolbar input{min-width:100%}
      .pager{width:100%;justify-content:flex-start;margin-left:0}
      .table-wrap{max-height:520px}
      table{min-width:820px}
    }
    @media print{
      body{background:#fff;color:#111}
      .sidebar,.top-nav,.btn-top,.header-actions,.filter-state,.toolbar{display:none !important}
      .layout{grid-template-columns:1fr;padding:0}
      .chart,.tile,.order-card,.state-card{break-inside:avoid}
      .section{break-inside:avoid;margin-top:22px}
      .chart-tooltip{display:none !important}
    }
    @media (prefers-reduced-motion:reduce){
      *,*::before,*::after{animation-duration:.01ms !important;animation-iteration-count:1 !important;
        transition-duration:.01ms !important;scroll-behavior:auto !important}
      html{scroll-behavior:auto}
    }
"""

_BODY = """
<a href="#mainContent" class="skip-link">Saltar al panel principal</a>
<div class="layout">
  <aside class="sidebar" id="globalFilters" aria-label="Filtros globales">
    <div class="brand">
      <span class="eyebrow">Centro de decisión</span>
      <b>Inteligencia de Mantenimiento Ferroviario</b>
      <span>Salud de activos, cola de taller y valor del mantenimiento basado en condición.</span>
    </div>
    <div class="filter-set">
      <span class="set-label">Cartera</span>
      <div class="filter-group"><label for="f_flota">Flota</label><select id="f_flota" name="flota" autocomplete="off"></select></div>
      <div class="filter-group"><label for="f_unidad">Unidad</label><select id="f_unidad" name="unidad" autocomplete="off"></select></div>
      <div class="filter-group"><label for="f_deposito">Depósito recomendado</label><select id="f_deposito" name="deposito" autocomplete="off"></select></div>
    </div>
    <div class="filter-set">
      <span class="set-label">Activo</span>
      <div class="filter-group"><label for="f_familia">Familia de componente</label><select id="f_familia" name="familia" autocomplete="off"></select></div>
      <div class="filter-group"><label for="f_sistema">Sistema principal</label><select id="f_sistema" name="sistema" autocomplete="off"></select></div>
    </div>
    <div class="filter-set">
      <span class="set-label">Decisión</span>
      <div class="filter-group"><label for="f_riesgo">Nivel de riesgo</label><select id="f_riesgo" name="riesgo" autocomplete="off"></select></div>
      <div class="filter-group"><label for="f_intervencion">Tipo de intervención</label><select id="f_intervencion" name="intervencion" autocomplete="off"></select></div>
      <div class="filter-group"><label for="f_ventana">Ventana temporal</label><select id="f_ventana" name="ventana" autocomplete="off"></select></div>
      <div class="filter-group"><label for="f_estrategia">Estrategia de mantenimiento</label><select id="f_estrategia" name="estrategia" autocomplete="off"></select></div>
    </div>
    <div class="side-actions">
      <button type="button" class="btn btn-reset" id="btnReset">Restablecer filtros</button>
    </div>
    <div class="sidebar-stats">
      <div><span>Componentes en vista</span><b id="s_count_rows">0</b></div>
      <div><span>Unidades en vista</span><b id="s_count_units">0</b></div>
      <div><span>Unidades prioridad 70+</span><b id="s_count_high">0</b></div>
      <div><span>Horas de taller</span><b id="s_hours">0</b></div>
    </div>
  </aside>

  <main class="content" id="mainContent">
    <section class="header">
      <div class="header-row">
        <div class="header-main">
          <span class="eyebrow">Mantenimiento basado en condición</span>
          <h1>Centro de control CBM ferroviario</h1>
          <p class="sub">Qué unidad entra primero a taller, qué lo sostiene, qué capacidad falta y qué vale la estrategia frente a operar hasta el fallo.</p>
        </div>
        <div class="header-actions">
          <button type="button" class="btn" id="btnFilters" aria-controls="globalFilters" aria-expanded="true">Ocultar filtros</button>
          <button type="button" class="btn btn-icon" id="btnTheme" aria-label="Cambiar tema" title="Cambiar tema claro u oscuro"></button>
          <button type="button" class="btn btn-icon" id="btnPrint" aria-label="Imprimir" title="Imprimir"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M6 9V3h12v6"/><path d="M6 18H4v-7h16v7h-2"/><path d="M8 15h8v6H8z"/></svg></button>
        </div>
      </div>
      <div class="rule-track"></div>
      <div class="meta">
        <span>Cobertura <b>__COVERAGE_START__ · __COVERAGE_END__</b></span>
        <span>Flotas <b>__N_FLOTAS__</b></span>
        <span>Unidades <b>__N_UNIDADES__</b></span>
        <span>Depósitos <b>__N_DEPOSITOS__</b></span>
        <span>Componentes <b>__N_COMPONENTES__</b></span>
        <span>Versión <b>__DASHBOARD_VERSION__</b></span>
      </div>
      <nav class="top-nav" aria-label="Navegación del panel de control">
        <a href="#sec_decision"><b>01</b>Decisión</a>
        <a href="#sec_estado"><b>02</b>Estado de flota</a>
        <a href="#sec_riesgo"><b>03</b>Riesgo técnico</a>
        <a href="#sec_cola"><b>04</b>Cola de intervención</a>
        <a href="#sec_taller"><b>05</b>Capacidad y pendientes</a>
        <a href="#sec_factores"><b>06</b>Factores</a>
        <a href="#sec_estrategia"><b>07</b>Caso estratégico</a>
        <a href="#sec_tabla"><b>08</b>Detalle</a>
      </nav>
      <div class="filter-state" id="filterState" aria-live="polite"></div>
    </section>

    <section class="section" id="sec_decision">
      <div class="section-head">
        <span class="sec-index">01</span>
        <div>
          <h3>Qué debe entrar primero a taller</h3>
          <p>La cola priorizada se resuelve en una sola entrada: el caso con mayor puntuación de intervención dentro de la vista activa y la evidencia que lo sostiene.</p>
        </div>
        <div class="head-aside" id="decisionAside"></div>
      </div>
      <div class="decision-grid">
        <article class="order-card">
          <div class="order-head">
            <span class="order-tag">Orden de trabajo · secuencia 1</span>
            <span class="order-rule" id="exec_rule">—</span>
          </div>
          <div class="order-id" id="exec_action" aria-live="polite">—</div>
          <p class="order-note" id="exec_action_note">—</p>
          <div class="order-route">
            <div class="route-item"><b>Depósito destino</b><span id="exec_step_unit">—</span></div>
            <div class="route-item"><b>Ventana sugerida</b><span id="exec_step_window">—</span></div>
            <div class="route-item"><b>Horas de taller</b><span id="exec_step_hours">—</span></div>
          </div>
          <div class="evidence" id="evidenceGrid"></div>
          <div class="order-ssot">
            <p><strong>Unidad que debe entrar primero:</strong> __TOP_UNIT__</p>
            <p><strong>Componente prioritario:</strong> __TOP_COMPONENT__</p>
            <p class="ssot-note">Registro oficial de métricas, sin filtros. La tarjeta responde a la vista activa.</p>
          </div>
        </article>
        <article class="state-card">
          <h2>Estado de ejecución</h2>
          <p id="exec_state">—</p>
          <div class="state-list">
            <div><b>Corte de pendientes</b><span id="exec_snapshot">—</span></div>
            <div><b>Bloqueo principal</b><span id="exec_bottleneck">—</span></div>
            <div><b>Casos no diferibles</b><span id="exec_step_deferral">—</span></div>
            <div><b>Coste de retraso en cola</b><span id="exec_queue_cost">—</span></div>
          </div>
        </article>
      </div>
      <div class="anomaly-grid" id="anomalyGrid"></div>
    </section>

    <section class="section" id="sec_estado">
      <div class="section-head">
        <span class="sec-index">02</span>
        <div>
          <h3>Estado de la flota</h3>
          <p>Los indicadores marcados como oficiales provienen del registro gobernado de métricas y no cambian con los filtros; los marcados como vista responden a la selección activa.</p>
        </div>
        <div class="head-aside" id="estadoAside"></div>
      </div>
      <div class="tiles">
        <div class="tile">
          <div class="tile-head"><span class="k">Disponibilidad de flota</span><span class="prov prov-official">oficial</span></div>
          <div class="v" id="k_avail">—</div>
          <div class="delta" id="k_avail_delta"></div>
          <div class="tile-foot">
            <div class="tile-spark" id="ch_spark" data-label="Disponibilidad semanal de flota"></div>
            <span class="t-note" id="k_avail_trend">—</span>
          </div>
        </div>
        <div class="tile risk">
          <div class="tile-head"><span class="k">Unidades con prioridad 70+</span><span class="prov">vista</span></div>
          <div class="v" id="k_uhr">—</div>
          <div class="s">Compiten por una entrada prioritaria a taller.</div>
          <div class="tile-foot">
            <div class="t-track"><div class="t-fill" id="k_uhr_fill"></div></div>
            <span class="t-note" id="k_uhr_note">—</span>
          </div>
        </div>
        <div class="tile capacity">
          <div class="tile-head"><span class="k">Pendientes críticos físicos</span><span class="prov prov-official">oficial</span></div>
          <div class="v" id="k_bcf">—</div>
          <div class="s">Cola física con criticidad estructural por edad o severidad.</div>
          <div class="tile-foot">
            <div class="t-track"><div class="t-fill" id="k_bcf_fill"></div></div>
            <span class="t-note" id="k_bcf_sub">—</span>
          </div>
        </div>
        <div class="tile value pos">
          <div class="tile-head"><span class="k">Diferencial CBM vs reactiva</span><span class="prov prov-official">oficial</span></div>
          <div class="v" id="k_ahorro">—</div>
          <div class="s" id="k_ahorro_sub">—</div>
          <div class="tile-foot">
            <div class="tile-mini" id="ch_range" data-label="Rango de sensibilidad del diferencial CBM"></div>
            <span class="t-note" id="k_ahorro_range">—</span>
          </div>
        </div>
      </div>
      <div class="ribbon" aria-label="Métricas operativas de detalle">
        <div class="cell"><div class="k">MTBF</div><div class="v" id="k_mtbf">—</div><div class="s">horas</div></div>
        <div class="cell"><div class="k">MTTR</div><div class="v" id="k_mttr">—</div><div class="s">horas</div></div>
        <div class="cell"><div class="k">Pendientes físicos</div><div class="v" id="k_bf">—</div><div class="s">órdenes abiertas</div></div>
        <div class="cell"><div class="k">Pendientes vencidos</div><div class="v" id="k_bv">—</div><div class="s">fuera de ventana</div></div>
        <div class="cell"><div class="k">Riesgo alto al diferir</div><div class="v" id="k_drh">—</div><div class="s">puntuación 70+</div></div>
        <div class="cell"><div class="k">Correctivas evitables</div><div class="v" id="k_ce">—</div><div class="s">migrables a plan</div></div>
        <div class="cell"><div class="k">Riesgo de fallo 65%+</div><div class="v" id="k_alertas">—</div><div class="s">ventana de 30 días</div></div>
        <div class="cell"><div class="k">Saturación media</div><div class="v" id="k_sat">—</div><div class="s" id="k_sat_sub">capacidad usada</div></div>
      </div>
    </section>

    <section class="section" id="sec_riesgo">
      <div class="section-head">
        <span class="sec-index">03</span>
        <div>
          <h3>Por qué esos activos y no otros</h3>
          <p>La prioridad nace del cruce entre deterioro observado, probabilidad de fallo a 30 días y la vida útil restante estimada.</p>
        </div>
        <div class="head-aside" id="riesgoAside"></div>
      </div>
      <div class="grid2">
        <div class="chart">
          <h4>Salud y riesgo por familia de componente</h4>
          <p class="chart-note">Familias ordenadas por riesgo. Son dos índices con unidades distintas, por eso ocupan paneles separados: se comparan familias dentro de cada panel, nunca longitudes entre paneles.</p>
          <div id="ch_family" class="plot"></div>
        </div>
        <div class="chart">
          <h4>Vida útil restante estimada</h4>
          <p class="chart-note">Concentración de componentes por ventana de RUL. La franja accionable son los primeros 30 días.</p>
          <div id="ch_rul" class="plot"></div>
        </div>
      </div>
    </section>

    <section class="section" id="sec_cola">
      <div class="section-head">
        <span class="sec-index">04</span>
        <div>
          <h3>Qué exige la cola y en qué orden</h3>
          <p>La secuencia combina urgencia técnica, daño evitado al servicio y ajuste al taller. Estas vistas confirman si la primera entrada es coherente.</p>
        </div>
        <div class="head-aside" id="colaAside"></div>
      </div>
      <div class="grid2">
        <div class="chart">
          <h4>Carga de taller por ventana de intervención</h4>
          <p class="chart-note">Horas de taller que exige cada ventana. La franja más urgente define el frente de trabajo inmediato.</p>
          <div id="ch_window" class="plot"></div>
        </div>
        <div class="chart">
          <h4>Unidades por prioridad de intervención</h4>
          <p class="chart-note">Media de la puntuación de prioridad por unidad. La unidad destacada es la primera de la cola.</p>
          <div id="ch_top_units" class="plot"></div>
        </div>
        <div class="chart">
          <h4>Prioridad frente a riesgo de diferimiento</h4>
          <p class="chart-note">Cada punto es un componente; el tamaño refleja el impacto en servicio. El cuadrante superior derecho no admite aplazamiento.</p>
          <div class="legend"><span><i style="background:var(--critical)"></i>Prioridad y diferimiento 70+</span><span><i style="background:var(--mark-mute)"></i>Resto de la cartera</span></div>
          <div id="ch_priority_deferral" class="plot"></div>
        </div>
        <div class="chart">
          <h4>Mezcla de decisiones operativas</h4>
          <p class="chart-note">Reparto de la cola entre intervención, inspección, observación y monitorización.</p>
          <div id="ch_decisions" class="plot"></div>
        </div>
      </div>
    </section>

    <section class="section" id="sec_taller">
      <div class="section-head">
        <span class="sec-index">05</span>
        <div>
          <h3>Qué puede ejecutarse realmente</h3>
          <p>Separar carga física, vencimiento y saturación evita confundir el riesgo analítico con la cola real de taller.</p>
        </div>
        <div class="head-aside" id="tallerAside"></div>
      </div>
      <div class="grid2">
        <div class="chart">
          <h4>Presión de taller por depósito</h4>
          <p class="chart-note">Carga equivalente dividida por la capacidad del depósito, en el último corte con pendientes reales. No es un porcentaje acotado: por encima de 1,0x la carga supera la capacidad y a partir de 1,05x el motor bloquea la entrada. El indicador oficial de saturación mide el corte de calendario más reciente, que llega sin pendientes cargados, y por eso es mucho menor.</p>
          <div class="legend"><span><i style="background:var(--accent)"></i>Holgada</span><span><i style="background:var(--amber)"></i>Al límite</span><span><i style="background:var(--critical)"></i>Bloqueo por capacidad</span></div>
          <div id="ch_depot" class="plot plot-tall"></div>
        </div>
        <div class="chart">
          <h4>Pendientes por depósito</h4>
          <p class="chart-note">Los pendientes vencidos y críticos son subconjuntos de los físicos abiertos, no categorías separadas.</p>
          <div class="legend"><span><i style="background:var(--ember-1)"></i>Físicos</span><span><i style="background:var(--ember-2)"></i>Vencidos</span><span><i style="background:var(--ember-4)"></i>Críticos</span></div>
          <div id="ch_backlog_depot" class="plot plot-tall"></div>
        </div>
      </div>
    </section>

    <section class="section" id="sec_factores">
      <div class="section-head">
        <span class="sec-index">06</span>
        <div>
          <h3>Qué señales empujan la situación</h3>
          <p>Los factores dominantes y la inspección automática separan anomalía, repetitividad y degradación como causa de la criticidad.</p>
        </div>
        <div class="head-aside" id="factoresAside"></div>
      </div>
      <div class="grid2">
        <div class="chart">
          <h4>Factor dominante del riesgo</h4>
          <p class="chart-note">Señal que más pesa en la puntuación de cada componente de la vista.</p>
          <div id="ch_drivers" class="plot"></div>
        </div>
        <div class="chart">
          <h4>Detección previa al fallo por familia</h4>
          <p class="chart-note">Proporción de fallos precedidos por una detección de inspección automática. Mide si la señal llega a tiempo.</p>
          <div id="ch_inspection" class="plot"></div>
        </div>
      </div>
    </section>

    <section class="section" id="sec_estrategia">
      <div class="section-head">
        <span class="sec-index">07</span>
        <div>
          <h3>Qué vale la estrategia y qué cuesta esperar</h3>
          <p>Escenarios comparativos con supuestos económicos explícitos. Las cifras son aproximaciones de escenario sobre datos sintéticos, no estimaciones contractuales.</p>
        </div>
        <div class="head-aside" id="estrategiaAside"></div>
      </div>
      <div class="callout" id="strategyInsight"></div>
      <div class="grid2">
        <div class="chart">
          <h4>Coste incremental por hora de servicio preservada</h4>
          <p class="chart-note">Umbral de decisión frente a la estrategia reactiva: lo que debería valer una hora de servicio para compensar el sobrecoste de cada estrategia. Es un umbral, no una disposición a pagar observada.</p>
          <div id="ch_strategy" class="plot plot-short"></div>
        </div>
        <div class="chart">
          <h4>Coste de aplazar la intervención</h4>
          <p class="chart-note">Coste e indisponibilidad acumulados al diferir, en escalas independientes para no forzar una correlación inexistente. La línea vertical marca los 14 días.</p>
          <div id="ch_deferral" class="plot"></div>
        </div>
        <div class="chart">
          <h4>Valor del coste de retraso capturado por la planificación</h4>
          <p class="chart-note">Cuánto del coste de retraso en cola convierte cada heurística en trabajo programado y cuánto queda expuesto. Bajo cada escenario, la proporción de riesgo residual que queda sin atender.</p>
          <div class="legend"><span><i style="background:var(--good)"></i>Capturado</span><span><i style="background:var(--critical)"></i>No capturado</span></div>
          <div id="ch_scheduling" class="plot plot-short"></div>
        </div>
      </div>
    </section>

    <section class="section" id="sec_tabla">
      <div class="section-head">
        <span class="sec-index">08</span>
        <div>
          <h3>Trazabilidad caso a caso</h3>
          <p>Cada fila es un componente puntuado con su regla de decisión, su secuencia de entrada y su coste de retraso. Ordena por cualquier columna.</p>
        </div>
        <div class="head-aside" id="tablaAside"></div>
      </div>
      <div class="toolbar">
        <input id="searchBox" name="busqueda" autocomplete="off" aria-label="Buscar unidad, componente o factor" placeholder="Buscar unidad, componente, factor…" />
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
      <p class="footer-note">Panel autocontenido, sin conexión ni dependencias externas. Versión <b>__DASHBOARD_VERSION__</b> · firma de payload <b>__PAYLOAD_SIGNATURE__</b>. Los indicadores oficiales consumen el registro gobernado de métricas; los filtros gobiernan gráficos, decisión y tabla.</p>
    </section>
  </main>
</div>
<button type="button" class="btn btn-top btn-icon" id="btnTop" aria-label="Volver arriba"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 19V5"/><path d="M5 12l7-7 7 7"/></svg></button>
<div id="chartTooltip" class="chart-tooltip" hidden></div>
"""

_SCRIPT = """
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
const THEME_KEY = "dashboard_theme_v1";
// Umbrales del motor de recomendación: por encima de 1,05x la entrada se bloquea por capacidad.
const CAPACITY_BLOCK = 1.05;
const CAPACITY_TIGHT = 0.85;

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
  "recommended_entry_sequence","unidad_id","componente_id","flota_id","deposito_recomendado","component_family",
  "sistema_principal","risk_level","decision_type","decision_rule_id","recommended_action_initial","time_window",
  "intervention_priority_score","deferral_risk_score","service_impact_score","workshop_fit_score","health_score",
  "prob_fallo_30d","component_rul_estimate","hours_required","coste_retraso_proxy","main_risk_driver",
  "confidence_flag","estrategia_mantenimiento_actual"
];
const tableLabels = {
  recommended_entry_sequence:"Sec.", unidad_id:"Unidad", componente_id:"Componente", flota_id:"Flota",
  deposito_recomendado:"Depósito recomendado", component_family:"Familia", sistema_principal:"Sistema",
  risk_level:"Nivel de riesgo", decision_type:"Decisión operacional", decision_rule_id:"Regla",
  recommended_action_initial:"Acción recomendada", time_window:"Ventana", intervention_priority_score:"Prioridad",
  deferral_risk_score:"Riesgo de diferimiento", service_impact_score:"Impacto en servicio",
  workshop_fit_score:"Ajuste al taller", health_score:"Salud", prob_fallo_30d:"Riesgo de fallo 30d",
  component_rul_estimate:"RUL (días)", hours_required:"Horas taller", coste_retraso_proxy:"Coste de retraso",
  main_risk_driver:"Factor principal", confidence_flag:"Confianza", estrategia_mantenimiento_actual:"Estrategia",
  deposito_id:"Depósito actual", unidad:"Unidad"
};
const numericColumns = new Set([
  "recommended_entry_sequence","intervention_priority_score","deferral_risk_score","service_impact_score",
  "workshop_fit_score","health_score","prob_fallo_30d","component_rul_estimate","hours_required","coste_retraso_proxy"
]);

/* ---------- formato ---------- */
function toNum(v){ const n = Number(v); return Number.isFinite(n) ? n : 0; }
function uniq(vals){ return ["Todos", ...Array.from(new Set(vals.filter(v => String(v).trim() !== ""))).sort()]; }
function fmt1(n){ return Number(n).toLocaleString("es-ES", {minimumFractionDigits:1, maximumFractionDigits:1}); }
function fmt2(n){ return Number(n).toLocaleString("es-ES", {minimumFractionDigits:2, maximumFractionDigits:2}); }
function fmt0(n){ return Math.round(Number(n)).toLocaleString("es-ES"); }
function fmtMoney(n){
  const value = Number(n);
  if(!Number.isFinite(value)) return "0 €";
  const sign = value < 0 ? "−" : "";
  const abs = Math.abs(value);
  if(abs >= 1e6) return `${sign}${fmt1(abs/1e6)} M€`;
  if(abs >= 1e3) return `${sign}${fmt0(abs/1e3)} k€`;
  return `${sign}${fmt0(abs)} €`;
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
function titleCase(v){ const s = String(v ?? ""); return s.charAt(0).toUpperCase() + s.slice(1); }
function setText(id, txt){ const el = document.getElementById(id); if(el) el.textContent = txt; }
function setHtml(id, html){ const el = document.getElementById(id); if(el) el.innerHTML = html; }

function setSelectOptions(sel, values){
  sel.replaceChildren();
  values.forEach(value => {
    const opt = document.createElement("option");
    opt.value = String(value);
    opt.textContent = String(value);
    sel.appendChild(opt);
  });
}

function badge(level){
  const cls = level==="Crítico"?"badge-critico":(level==="Alto"?"badge-alto":(level==="Medio"?"badge-medio":"badge-bajo"));
  return `<span class="badge ${cls}">${esc(level)}</span>`;
}

/* ---------- controles ---------- */
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
    // Persistencia no crítica.
  }
}

function syncFloatingControls(){
  const btnTop = document.getElementById("btnTop");
  if(btnTop) btnTop.classList.toggle("visible", window.scrollY > 380);
}

function activeFilterValue(id){
  const el = document.getElementById(id);
  return el ? el.value : "Todos";
}

function renderFilterState(){
  const el = document.getElementById("filterState");
  if(!el) return;
  const active = filters
    .map(([id, key]) => {
      const value = activeFilterValue(id);
      return value === "Todos" ? null : `${tableLabels[key] || key}: ${value}`;
    })
    .filter(Boolean);
  const search = document.getElementById("searchBox")?.value?.trim() || "";
  if(search) active.push(`Búsqueda: ${search}`);
  if(active.length){
    el.textContent = `Vista filtrada · ${active.join(" · ")}`;
    el.hidden = false;
  } else {
    el.textContent = "";
    el.hidden = true;
  }
}

function initFilters(){
  filters.forEach(([id,key]) => {
    const sel = document.getElementById(id);
    setSelectOptions(sel, uniq(baseRows.map(r => r[key])));
    sel.addEventListener("change", applyFilters);
  });
  document.getElementById("searchBox").addEventListener("input", applyFilters);
  document.getElementById("btnReset").addEventListener("click", resetFilters);
  document.getElementById("btnPrint").addEventListener("click", () => window.print());
  document.getElementById("btnTop").addEventListener("click", () => window.scrollTo({top:0, behavior:"smooth"}));
  document.getElementById("btnFilters").addEventListener("click", () => {
    setFiltersCollapsed(!document.body.classList.contains("filters-collapsed"));
    window.setTimeout(() => renderCharts(computeDerived()), 220);
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
    const narrow = window.matchMedia && window.matchMedia("(max-width: 820px)").matches;
    setFiltersCollapsed(narrow, false);
  }
  syncFloatingControls();
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

/* ---------- agregación ---------- */
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
  if(d <= 14) return "0-14";
  if(d <= 30) return "15-30";
  if(d <= 60) return "31-60";
  if(d <= 90) return "61-90";
  if(d <= 180) return "91-180";
  return "180+";
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

/* El estado de taller describe la red física, no la cartera filtrada: sólo el filtro
   explícito de depósito lo acota. Ligarlo a los depósitos recomendados ocultaría los
   depósitos que acumulan pendientes pero nunca reciben una entrada sugerida. */
function filterDepotByCurrentSelection(){
  const depSel = activeFilterValue("f_deposito");
  if(depSel !== "Todos") return depotLatest.filter(r => String(r.deposito_id) === String(depSel));
  return depotLatest.slice();
}

function availabilitySeries(){
  const byWeek = new Map();
  fleetWeek.forEach(r => {
    const k = String(r.week_start);
    const arr = byWeek.get(k) || [];
    arr.push(toNum(r.availability_rate));
    byWeek.set(k, arr);
  });
  return Array.from(byWeek.entries())
    .sort((a,b)=>a[0].localeCompare(b[0]))
    .map(([week, vals]) => ({label:week, value:mean(vals)*100}));
}

const WINDOW_ORDER = ["0-2d","3-7d","8-14d","15-21d"];
const WINDOW_TEXT = {"0-2d":"0 a 2 días","3-7d":"3 a 7 días","8-14d":"8 a 14 días","15-21d":"15 a 21 días"};
// Etiquetas de presentación para identificadores técnicos. Sin coincidencia se muestra
// el valor crudo, de modo que un nuevo factor nunca desaparece del gráfico.
const DRIVER_TEXT = {
  anomalias:"Anomalías de sensor", pendientes:"Pendientes acumulados",
  estres_operacion:"Estrés de operación", repetitividad:"Fallos repetitivos",
  degradacion:"Degradación", desgaste:"Desgaste"
};
function driverText(v){ return DRIVER_TEXT[String(v)] || titleCase(String(v).replaceAll("_", " ")); }

function computeDerived(){
  const rows = filteredRows;
  const depotSlice = filterDepotByCurrentSelection();

  const uniqueUnits = new Set(rows.map(r => String(r.unidad_id)));
  const highRiskUnits = new Set(rows.filter(r => toNum(r.intervention_priority_score)>=70).map(r => String(r.unidad_id)));

  const familyRisk = groupedMean(rows, "component_family", "prob_fallo_30d").sort((a,b)=>b.value-a.value);
  const familyHealth = new Map(groupedMean(rows, "component_family", "health_score").map(x => [x.label, x.value]));

  const rulBucket = groupedCount(
    rows.map(r => ({rul_bucket: rulBucketLabel(r.component_rul_estimate)})),
    "rul_bucket"
  );
  const decisionDist = groupedCount(rows, "decision_type").sort((a,b)=>b.value-a.value);
  const driverDist = groupedCount(rows, "main_risk_driver").sort((a,b)=>b.value-a.value).slice(0,8);
  const topPriority = topN(rows, "intervention_priority_score", 10);

  const windowLoad = WINDOW_ORDER.map(w => {
    const slice = rows.filter(r => String(r.time_window) === w);
    return {label:w, value:sum(slice.map(r => toNum(r.hours_required))), count:slice.length,
      cost:sum(slice.map(r => toNum(r.coste_retraso_proxy)))};
  }).filter(x => x.count > 0);

  const priorityDeferral = rows
    .slice()
    .sort((a,b)=>(toNum(b.intervention_priority_score)+toNum(b.deferral_risk_score))-(toNum(a.intervention_priority_score)+toNum(a.deferral_risk_score)))
    .slice(0,220)
    .map(r => ({
      x:toNum(r.intervention_priority_score),
      y:toNum(r.deferral_risk_score),
      size:Math.max(3.2, Math.min(9, 3.2 + toNum(r.service_impact_score)/16)),
      hot:toNum(r.deferral_risk_score)>=70 && toNum(r.intervention_priority_score)>=70,
      tip:`${r.unidad_id} · ${r.componente_id}\\nPrioridad ${fmt1(r.intervention_priority_score)} · Diferimiento ${fmt1(r.deferral_risk_score)}\\nImpacto en servicio ${fmt1(r.service_impact_score)}`
    }));

  const queueHours = sum(rows.map(r => toNum(r.hours_required)));
  const queueCost = sum(rows.map(r => toNum(r.coste_retraso_proxy)));
  const highDeferral = rows.filter(r => toNum(r.deferral_risk_score)>=70).length;
  const blockedDepots = depotSlice.filter(r => toNum(r.saturation_ratio) > CAPACITY_BLOCK);
  const worstDepot = depotSlice.slice().sort((a,b)=>toNum(b.saturation_ratio)-toNum(a.saturation_ratio))[0];

  return {
    rows, uniqueUnits, highRiskUnits, highDeferral, familyRisk, familyHealth, rulBucket, decisionDist,
    driverDist, topPriority, windowLoad, priorityDeferral, queueHours, queueCost, depotSlice,
    blockedDepots, worstDepot
  };
}

/* ---------- motor de gráficos ---------- */
function cssVar(name, fallback){
  const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return v || fallback;
}
function ink(){
  return {
    grid: cssVar("--grid", "#e7e4dc"),
    axis: cssVar("--axis", "#cfcbc1"),
    text: cssVar("--ink-2", "#3d434b"),
    faint: cssVar("--ink-4", "#9aa0a8"),
    surface: cssVar("--surface", "#fbfaf8"),
    accent: cssVar("--accent", "#2c5fa8"),
    good: cssVar("--good", "#0f7a52"),
    amber: cssVar("--amber", "#a66a00"),
    critical: cssVar("--critical", "#b3261e"),
    mute: cssVar("--mark-mute", "#8a8f98"),
    e1: cssVar("--ember-1", "#df9880"),
    e2: cssVar("--ember-2", "#cb7350"),
    e3: cssVar("--ember-3", "#ac4525"),
    e4: cssVar("--ember-4", "#7a2610")
  };
}

function makeSvg(containerId){
  const el = document.getElementById(containerId);
  if(!el) return null;
  const w = Math.max(160, el.clientWidth || 420);
  const h = Math.max(32, el.clientHeight || 240);
  const label = el.dataset.label || el.closest(".chart")?.querySelector("h4")?.textContent || containerId;
  el.innerHTML = `<svg width="100%" height="100%" viewBox="0 0 ${w} ${h}" role="img" aria-label="${esc(label)}"></svg>`;
  return el.querySelector("svg");
}

function emptyPlot(svg, message){
  const CK = ink();
  const w = svg.viewBox.baseVal.width, h = svg.viewBox.baseVal.height;
  svg.innerHTML = `<text x="${w/2}" y="${h/2}" text-anchor="middle" fill="${CK.faint}" font-size="11">${esc(message)}</text>`;
}

function bindSvgTooltip(svg){
  const tip = document.getElementById("chartTooltip");
  if(!svg || !tip) return;
  svg.querySelectorAll("[data-tip]").forEach(node => {
    node.style.cursor = "default";
    node.addEventListener("mousemove", (ev) => {
      const txt = node.getAttribute("data-tip");
      if(!txt) return;
      tip.hidden = false;
      tip.textContent = txt;
      const box = tip.getBoundingClientRect();
      const x = Math.min(ev.clientX + 14, window.innerWidth - box.width - 10);
      const y = Math.min(ev.clientY + 14, window.innerHeight - box.height - 10);
      tip.style.left = `${Math.max(8, x)}px`;
      tip.style.top = `${Math.max(8, y)}px`;
    });
    node.addEventListener("mouseleave", () => { tip.hidden = true; });
  });
}

/* Barra con extremo redondeado en el lado del dato y base recta. */
function barPath(x, y, w, h, dir){
  if(w <= 0 || h <= 0) return "";
  const r = Math.max(0, Math.min(4, dir === "right" ? Math.min(w, h/2) : Math.min(h, w/2)));
  if(dir === "right"){
    return `M ${x} ${y} H ${x+w-r} A ${r} ${r} 0 0 1 ${x+w} ${y+r} V ${y+h-r} A ${r} ${r} 0 0 1 ${x+w-r} ${y+h} H ${x} Z`;
  }
  return `M ${x} ${y+h} V ${y+r} A ${r} ${r} 0 0 1 ${x+r} ${y} H ${x+w-r} A ${r} ${r} 0 0 1 ${x+w} ${y+r} V ${y+h} Z`;
}

function ellipsis(text, maxChars){
  const s = String(text ?? "");
  return s.length > maxChars ? `${s.slice(0, maxChars-1)}…` : s;
}

/* Ancho aproximado del texto en la tipografía mono de los gráficos: sirve para decidir
   si una etiqueta cabe dentro de una marca antes de dibujarla. */
function textWidth(text, size=10.5){ return String(text ?? "").length * size * 0.6; }

/* Tinta legible sobre un relleno de color, elegida por luminancia y no por costumbre. */
function fillInk(hex){
  const m = /^#?([0-9a-f]{6})$/i.exec(String(hex).trim());
  if(!m) return "#ffffff";
  const v = parseInt(m[1], 16);
  const channels = [(v >> 16) & 255, (v >> 8) & 255, v & 255].map(c => {
    const s = c/255;
    return s <= 0.03928 ? s/12.92 : Math.pow((s+0.055)/1.055, 2.4);
  });
  const lum = 0.2126*channels[0] + 0.7152*channels[1] + 0.0722*channels[2];
  return (1.05/(lum+0.05)) >= ((lum+0.05)/0.05) ? "#ffffff" : "#101316";
}

/* Barras horizontales: etiqueta a la izquierda, valor al extremo del dato. */
function drawHBars(containerId, items, opts){
  const svg = makeSvg(containerId); if(!svg) return;
  const CK = ink();
  const o = Object.assign({gutter:88, valueFmt:(v)=>fmt1(v), color:CK.accent, colorOf:null, tipOf:null, labelChars:13, unit:"", maxSlot:46}, opts||{});
  const w = svg.viewBox.baseVal.width, h = svg.viewBox.baseVal.height;
  if(!items.length){ emptyPlot(svg, "Sin datos para los filtros activos"); return; }
  const valueRoom = 58;
  const m = {l:o.gutter, r:valueRoom, t:4, b:4};
  const iw = w-m.l-m.r, ih = h-m.t-m.b;
  const maxV = Math.max(...items.map(d=>toNum(d.value)), 0) || 1;
  // Con pocas categorías el carril se acota y el grupo se centra: las barras conservan
  // su densidad en lugar de estirarse hasta perder la lectura.
  const slot = Math.min(ih/items.length, o.maxSlot);
  const top = m.t + (ih - slot*items.length)/2;
  const thick = Math.max(6, Math.min(24, slot-8));
  let out = "";
  items.forEach((d,i)=>{
    const v = toNum(d.value);
    const bw = Math.max(0, iw*(v/maxV));
    const cy = top + i*slot + slot/2;
    const y = cy - thick/2;
    const color = o.colorOf ? o.colorOf(d, i) : o.color;
    const tip = o.tipOf ? o.tipOf(d) : `${d.label}: ${o.valueFmt(v)}${o.unit}`;
    out += `<rect x="0" y="${cy-slot/2}" width="${w}" height="${slot}" fill="transparent" data-tip="${esc(tip)}"></rect>`;
    out += `<text x="${m.l-9}" y="${cy+3.5}" text-anchor="end" fill="${CK.text}" font-size="10.5">${esc(ellipsis(d.label, o.labelChars))}</text>`;
    out += `<path d="${barPath(m.l, y, bw, thick, "right")}" fill="${color}"></path>`;
    out += `<text x="${m.l+bw+7}" y="${cy+3.5}" fill="${CK.text}" font-size="10.5">${o.valueFmt(v)}</text>`;
  });
  svg.innerHTML = out;
  bindSvgTooltip(svg);
}

/* Columnas verticales con eje categórico ordenado. */
function drawColumns(containerId, items, opts){
  const svg = makeSvg(containerId); if(!svg) return;
  const CK = ink();
  const o = Object.assign({color:CK.accent, colorOf:null, valueFmt:(v)=>fmt0(v), unit:"", axisTitle:""}, opts||{});
  const w = svg.viewBox.baseVal.width, h = svg.viewBox.baseVal.height;
  if(!items.length){ emptyPlot(svg, "Sin datos para los filtros activos"); return; }
  const m = {l:8, r:8, t:22, b:o.axisTitle?38:24};
  const iw = w-m.l-m.r, ih = h-m.t-m.b;
  const maxV = Math.max(...items.map(d=>toNum(d.value)), 1);
  const slot = iw/items.length;
  const thick = Math.max(6, Math.min(48, slot-10));
  let out = `<line x1="${m.l}" y1="${m.t+ih}" x2="${m.l+iw}" y2="${m.t+ih}" stroke="${CK.axis}" stroke-width="1"/>`;
  items.forEach((d,i)=>{
    const v = toNum(d.value);
    const bh = Math.max(0, ih*(v/maxV));
    const x = m.l + i*slot + (slot-thick)/2;
    const y = m.t + ih - bh;
    const color = o.colorOf ? o.colorOf(d, i) : o.color;
    out += `<rect x="${m.l+i*slot}" y="${m.t}" width="${slot}" height="${ih}" fill="transparent" data-tip="${esc(`${d.label}: ${o.valueFmt(v)}${o.unit}`)}"></rect>`;
    out += `<path d="${barPath(x, y, thick, bh, "up")}" fill="${color}"></path>`;
    out += `<text x="${x+thick/2}" y="${y-6}" text-anchor="middle" fill="${CK.text}" font-size="10.5">${o.valueFmt(v)}</text>`;
    out += `<text x="${x+thick/2}" y="${m.t+ih+14}" text-anchor="middle" fill="${CK.faint}" font-size="10">${esc(d.label)}</text>`;
  });
  if(o.axisTitle){
    out += `<text x="${m.l+iw/2}" y="${h-4}" text-anchor="middle" fill="${CK.faint}" font-size="10">${esc(o.axisTitle)}</text>`;
  }
  svg.innerHTML = out;
  bindSvgTooltip(svg);
}

/* Pequeños múltiplos con eje categórico compartido: cada medida conserva su propio
   panel y su propia unidad, de modo que nunca se comparan longitudes entre escalas. */
function drawPanelBars(containerId, items, panels){
  const svg = makeSvg(containerId); if(!svg) return;
  const CK = ink();
  const w = svg.viewBox.baseVal.width, h = svg.viewBox.baseVal.height;
  if(!items.length){ emptyPlot(svg, "Sin datos para los filtros activos"); return; }
  const m = {l:76, r:8, t:24, b:20};
  const gap = 30;
  const panelW = (w-m.l-m.r-gap*(panels.length-1))/panels.length;
  const ih = h-m.t-m.b;
  const slot = ih/items.length;
  const thick = Math.max(6, Math.min(18, slot-9));
  let out = "";
  items.forEach((d,i)=>{
    const cy = m.t + i*slot + slot/2;
    out += `<text x="${m.l-10}" y="${cy+3.5}" text-anchor="end" fill="${CK.text}" font-size="10.5">${esc(ellipsis(d.label, 11))}</text>`;
  });
  panels.forEach((p, pi) => {
    const x0 = m.l + pi*(panelW+gap);
    out += `<text x="${x0}" y="${m.t-11}" fill="${CK.faint}" font-size="10">${esc(p.title)}</text>`;
    out += `<line x1="${x0}" y1="${m.t}" x2="${x0}" y2="${m.t+ih}" stroke="${CK.axis}" stroke-width="1"/>`;
    items.forEach((d,i)=>{
      const v = toNum(d[p.key]);
      const bw = Math.max(0, (Math.max(0, Math.min(100, v))/100)*(panelW-34));
      const cy = m.t + i*slot + slot/2;
      out += `<rect x="${x0}" y="${cy-slot/2}" width="${panelW}" height="${slot}" fill="transparent" data-tip="${esc(`${d.label} · ${d.count} componentes\\n${p.title}: ${p.fmt(v)}`)}"></rect>`;
      out += `<path d="${barPath(x0, cy-thick/2, bw, thick, "right")}" fill="${p.color}"></path>`;
      out += `<text x="${x0+bw+6}" y="${cy+3.5}" fill="${CK.text}" font-size="10.5">${p.fmt(v)}</text>`;
    });
    out += `<text x="${x0}" y="${m.t+ih+13}" fill="${CK.faint}" font-size="9.5">0</text>`;
    out += `<text x="${x0+panelW-34}" y="${m.t+ih+13}" text-anchor="middle" fill="${CK.faint}" font-size="9.5">100</text>`;
  });
  svg.innerHTML = out;
  bindSvgTooltip(svg);
}

function drawScatter(containerId, points){
  const svg = makeSvg(containerId); if(!svg) return;
  const CK = ink();
  const w = svg.viewBox.baseVal.width, h = svg.viewBox.baseVal.height;
  if(!points.length){ emptyPlot(svg, "Sin datos para los filtros activos"); return; }
  const m = {l:38, r:12, t:10, b:34};
  const iw = w-m.l-m.r, ih = h-m.t-m.b;
  const pad = (lo, hi) => { const d = (hi-lo)||1; return [lo-d*0.06, hi+d*0.06]; };
  const [xMin, xMax] = pad(Math.min(...points.map(p=>p.x)), Math.max(...points.map(p=>p.x)));
  const [yMin, yMax] = pad(Math.min(...points.map(p=>p.y)), Math.max(...points.map(p=>p.y)));
  const sx = v => m.l + ((toNum(v)-xMin)/(xMax-xMin || 1))*iw;
  const sy = v => m.t + ih - ((toNum(v)-yMin)/(yMax-yMin || 1))*ih;
  const clampX = v => Math.max(m.l, Math.min(m.l+iw, sx(v)));
  const clampY = v => Math.max(m.t, Math.min(m.t+ih, sy(v)));
  const x70 = clampX(70), y70 = clampY(70);
  let out = `<rect x="${x70}" y="${m.t}" width="${m.l+iw-x70}" height="${y70-m.t}" fill="${CK.critical}" opacity=".07"/>`;
  out += `<line x1="${x70}" y1="${m.t}" x2="${x70}" y2="${m.t+ih}" stroke="${CK.grid}" stroke-width="1"/>`;
  out += `<line x1="${m.l}" y1="${y70}" x2="${m.l+iw}" y2="${y70}" stroke="${CK.grid}" stroke-width="1"/>`;
  out += `<line x1="${m.l}" y1="${m.t+ih}" x2="${m.l+iw}" y2="${m.t+ih}" stroke="${CK.axis}" stroke-width="1"/>`;
  out += `<line x1="${m.l}" y1="${m.t}" x2="${m.l}" y2="${m.t+ih}" stroke="${CK.axis}" stroke-width="1"/>`;
  points.filter(p=>!p.hot).forEach(p => {
    out += `<circle cx="${sx(p.x)}" cy="${sy(p.y)}" r="${p.size}" fill="${CK.mute}" fill-opacity=".55"/>`;
  });
  points.filter(p=>p.hot).forEach(p => {
    out += `<circle cx="${sx(p.x)}" cy="${sy(p.y)}" r="${p.size}" fill="${CK.critical}" fill-opacity=".85" stroke="${CK.surface}" stroke-width="2"/>`;
  });
  points.forEach(p => {
    out += `<circle cx="${sx(p.x)}" cy="${sy(p.y)}" r="${Math.max(11, p.size+4)}" fill="transparent" data-tip="${esc(p.tip)}"></circle>`;
  });
  out += `<text x="${x70+6}" y="${m.t+12}" fill="${CK.critical}" font-size="10">no diferible</text>`;
  out += `<text x="${m.l+iw}" y="${h-6}" text-anchor="end" fill="${CK.faint}" font-size="10">Prioridad de intervención</text>`;
  out += `<text x="10" y="${m.t+2}" fill="${CK.faint}" font-size="10" transform="rotate(-90 10,${m.t+2})" text-anchor="end">Riesgo de diferimiento</text>`;
  [Math.ceil(yMin/10)*10, Math.floor(yMax/10)*10].forEach(t => {
    out += `<text x="${m.l-6}" y="${sy(t)+3.5}" text-anchor="end" fill="${CK.faint}" font-size="10">${fmt0(t)}</text>`;
  });
  svg.innerHTML = out;
  bindSvgTooltip(svg);
}

/* Barras agrupadas para magnitudes anidadas (rampa ordinal de un solo tono). */
function drawNestedBars(containerId, rows){
  const svg = makeSvg(containerId); if(!svg) return;
  const CK = ink();
  const w = svg.viewBox.baseVal.width, h = svg.viewBox.baseVal.height;
  const items = rows.slice().sort((a,b)=>toNum(b.backlog_physical_items)-toNum(a.backlog_physical_items)).slice(0,10);
  if(!items.length){ emptyPlot(svg, "Sin pendientes en el corte válido"); return; }
  const m = {l:60, r:52, t:4, b:18};
  const iw = w-m.l-m.r, ih = h-m.t-m.b;
  const maxV = Math.max(...items.flatMap(r => [toNum(r.backlog_physical_items)]), 1);
  const slot = ih/items.length;
  const thick = Math.max(3, Math.min(8, (slot-10)/3));
  let out = "";
  items.forEach((r,i)=>{
    const top = m.t + i*slot;
    const cy = top + slot/2;
    const series = [
      ["Físicos", toNum(r.backlog_physical_items), CK.e1],
      ["Vencidos", toNum(r.backlog_overdue_items), CK.e2],
      ["Críticos", toNum(r.backlog_critical_items), CK.e4]
    ];
    const groupH = thick*3 + 4;
    out += `<rect x="0" y="${top}" width="${w}" height="${slot}" fill="transparent" data-tip="${esc(`${r.nombre_deposito || r.deposito_id} (${r.deposito_id})\\nFísicos ${fmt0(series[0][1])} · Vencidos ${fmt0(series[1][1])} · Críticos ${fmt0(series[2][1])}`)}"></rect>`;
    out += `<text x="${m.l-9}" y="${cy+3.5}" text-anchor="end" fill="${CK.text}" font-size="10.5">${esc(r.deposito_id)}</text>`;
    series.forEach(([name, val, color], j) => {
      const y = cy - groupH/2 + j*(thick+2);
      const bw = Math.max(0, iw*(val/maxV));
      out += `<path d="${barPath(m.l, y, bw, thick, "right")}" fill="${color}"></path>`;
      if(j === 0){
        out += `<text x="${m.l+bw+7}" y="${y+thick}" fill="${CK.text}" font-size="10">${fmt0(val)}</text>`;
      }
    });
  });
  svg.innerHTML = out;
  bindSvgTooltip(svg);
}

/* Barra apilada horizontal de dos estados con separación en color de superficie. */
function drawStacked(containerId, items){
  const svg = makeSvg(containerId); if(!svg) return;
  const CK = ink();
  const w = svg.viewBox.baseVal.width, h = svg.viewBox.baseVal.height;
  if(!items.length){ emptyPlot(svg, "Sin datos de planificación"); return; }
  const m = {l:104, r:14, t:12, b:12};
  const iw = w-m.l-m.r, ih = h-m.t-m.b;
  const maxV = Math.max(...items.map(d => d.captured + d.missed), 1);
  const slot = Math.min(ih/items.length, 78);
  const top = m.t + (ih - slot*items.length)/2;
  const thick = Math.max(10, Math.min(22, slot-30));
  let out = "";
  items.forEach((d,i)=>{
    const cy = top + i*slot + slot/2;
    const y = cy - thick/2;
    const wc = iw*(d.captured/maxV);
    const wm = iw*(d.missed/maxV);
    // La barra ocupa el ancho completo, así que la nota vive bajo la etiqueta y no tras el dato.
    out += `<text x="${m.l-10}" y="${cy}" text-anchor="end" fill="${CK.text}" font-size="10.5">${esc(ellipsis(d.label, 15))}</text>`;
    out += `<text x="${m.l-10}" y="${cy+13}" text-anchor="end" fill="${CK.faint}" font-size="9.5">${esc(d.note)}</text>`;
    out += `<path d="${barPath(m.l, y, Math.max(0, wc-2), thick, "right")}" fill="${CK.good}" data-tip="${esc(`${d.label} · capturado ${fmtMoney(d.captured)} de ${fmtMoney(d.captured+d.missed)}`)}"></path>`;
    out += `<path d="${barPath(m.l+wc, y, Math.max(0, wm), thick, "right")}" fill="${CK.critical}" data-tip="${esc(`${d.label} · no capturado ${fmtMoney(d.missed)} de ${fmtMoney(d.captured+d.missed)}`)}"></path>`;
    const capturedText = fmtMoney(d.captured);
    if(wc > textWidth(capturedText) + 14){
      out += `<text x="${m.l+7}" y="${cy+3.5}" fill="${fillInk(CK.good)}" font-size="10.5">${esc(capturedText)}</text>`;
    }
  });
  svg.innerHTML = out;
  bindSvgTooltip(svg);
}

/* Pequeños múltiplos: una escala por panel, eje x compartido. */
function drawSmallMultiples(containerId, xs, panels){
  const svg = makeSvg(containerId); if(!svg) return;
  const CK = ink();
  const w = svg.viewBox.baseVal.width, h = svg.viewBox.baseVal.height;
  if(!xs.length){ emptyPlot(svg, "Sin escenarios de diferimiento"); return; }
  const m = {l:14, r:46, t:18, b:38};
  const gap = 28;
  const panelH = (h - m.t - m.b - gap*(panels.length-1)) / panels.length;
  const iw = w-m.l-m.r;
  const sx = (i)=> m.l + (xs.length===1 ? iw/2 : i*(iw/(xs.length-1)));
  let out = "";
  panels.forEach((p, pi) => {
    const top = m.t + pi*(panelH+gap);
    const maxV = Math.max(...p.values, 0) || 1;
    const sy = (v)=> top + panelH - (toNum(v)/maxV)*panelH*0.82;
    const line = p.values.map((v,i)=>`${i===0?"M":"L"} ${sx(i)} ${sy(v)}`).join(" ");
    const area = `${line} L ${sx(xs.length-1)} ${top+panelH} L ${sx(0)} ${top+panelH} Z`;
    out += `<text x="${m.l}" y="${top-5}" fill="${CK.faint}" font-size="10">${esc(p.title)}</text>`;
    out += `<line x1="${m.l}" y1="${top+panelH}" x2="${m.l+iw}" y2="${top+panelH}" stroke="${CK.axis}" stroke-width="1"/>`;
    const i14 = xs.indexOf("14");
    if(i14 >= 0){
      out += `<line x1="${sx(i14)}" y1="${top}" x2="${sx(i14)}" y2="${top+panelH}" stroke="${CK.grid}" stroke-width="1"/>`;
    }
    out += `<path d="${area}" fill="${p.color}" fill-opacity=".1"/>`;
    out += `<path d="${line}" fill="none" stroke="${p.color}" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>`;
    p.values.forEach((v,i)=>{
      out += `<circle cx="${sx(i)}" cy="${sy(v)}" r="3.5" fill="${p.color}" stroke="${CK.surface}" stroke-width="2"/>`;
      out += `<circle cx="${sx(i)}" cy="${sy(v)}" r="12" fill="transparent" data-tip="${esc(`Diferir ${xs[i]} días · ${p.title}: ${p.fmt(v)}`)}"></circle>`;
    });
    // Etiquetado selectivo: sólo los extremos de la serie llevan valor.
    const last = p.values.length-1;
    out += `<text x="${sx(0)+7}" y="${sy(p.values[0])-8}" fill="${CK.faint}" font-size="10">${p.fmt(p.values[0])}</text>`;
    out += `<text x="${sx(last)+5}" y="${sy(p.values[last])+3.5}" fill="${CK.text}" font-size="10.5">${p.fmt(p.values[last])}</text>`;
  });
  xs.forEach((x,i)=>{
    out += `<text x="${sx(i)}" y="${h-19}" text-anchor="middle" fill="${CK.faint}" font-size="10">${esc(x)}</text>`;
  });
  out += `<text x="${m.l+iw/2}" y="${h-5}" text-anchor="middle" fill="${CK.faint}" font-size="10">días de diferimiento</text>`;
  svg.innerHTML = out;
  bindSvgTooltip(svg);
}

function drawSparkline(containerId, series){
  const svg = makeSvg(containerId); if(!svg) return;
  const CK = ink();
  const w = svg.viewBox.baseVal.width, h = svg.viewBox.baseVal.height;
  if(series.length < 2){ emptyPlot(svg, ""); return; }
  const m = {l:2, r:8, t:5, b:4};
  const iw = w-m.l-m.r, ih = h-m.t-m.b;
  const vals = series.map(p=>p.value);
  const lo = Math.min(...vals), hi = Math.max(...vals);
  const span = (hi-lo) || 1;
  const sx = (i)=> m.l + i*(iw/(series.length-1));
  const sy = (v)=> m.t + ih - ((v-lo)/span)*ih;
  const line = series.map((p,i)=>`${i===0?"M":"L"} ${sx(i)} ${sy(p.value)}`).join(" ");
  const last = series.length-1;
  let out = `<path d="${line}" fill="none" stroke="${CK.mute}" stroke-width="1.6" stroke-linejoin="round" stroke-linecap="round"/>`;
  out += `<circle cx="${sx(last)}" cy="${sy(series[last].value)}" r="3" fill="${CK.accent}" stroke="${CK.surface}" stroke-width="2"/>`;
  out += `<rect x="0" y="0" width="${w}" height="${h}" fill="transparent" data-tip="${esc(`Disponibilidad semanal · ${series[0].label} a ${series[last].label}\\nMínimo ${fmt1(lo)}% · máximo ${fmt1(hi)}%`)}"></rect>`;
  svg.innerHTML = out;
  bindSvgTooltip(svg);
}

/* Índice de presión frente a la capacidad del depósito. No es un porcentaje acotado:
   la carga equivalente puede superar la capacidad varias veces, así que la escala
   llega hasta el máximo observado y la línea de referencia marca 1,0x. */
function drawPressureBars(containerId, rows){
  const svg = makeSvg(containerId); if(!svg) return;
  const CK = ink();
  const w = svg.viewBox.baseVal.width, h = svg.viewBox.baseVal.height;
  const items = rows.slice().sort((a,b)=>toNum(b.saturation_ratio)-toNum(a.saturation_ratio)).slice(0,10);
  if(!items.length){ emptyPlot(svg, "Sin depósitos para los filtros activos"); return; }
  const m = {l:64, r:48, t:20, b:6};
  const iw = w-m.l-m.r, ih = h-m.t-m.b;
  const maxV = Math.max(...items.map(r=>toNum(r.saturation_ratio)), CAPACITY_BLOCK*1.2);
  const sx = (v)=> (Math.max(0, v)/maxV)*iw;
  const slot = Math.min(ih/items.length, 44);
  const top = m.t + (ih - slot*items.length)/2;
  const thick = Math.max(5, Math.min(18, slot-8));
  const refX = m.l + sx(1);
  let out = `<line x1="${refX}" y1="${m.t-6}" x2="${refX}" y2="${m.t+ih}" stroke="${CK.axis}" stroke-width="1"/>`;
  out += `<text x="${refX+5}" y="${m.t-9}" fill="${CK.faint}" font-size="10">capacidad 1,0x</text>`;
  items.forEach((r,i)=>{
    const v = toNum(r.saturation_ratio);
    const cy = top + i*slot + slot/2;
    const color = v > CAPACITY_BLOCK ? CK.critical : (v >= CAPACITY_TIGHT ? CK.amber : CK.accent);
    const name = r.nombre_deposito ? `${r.deposito_id} · ${r.nombre_deposito}` : String(r.deposito_id);
    const tip = `${name}\\nPresión ${fmt1(v)}x la capacidad\\nCapacidad ${fmt0(r.capacidad_taller)} h · ${fmt0(r.backlog_physical_items)} pendientes físicos`;
    out += `<rect x="0" y="${cy-slot/2}" width="${w}" height="${slot}" fill="transparent" data-tip="${esc(tip)}"></rect>`;
    out += `<text x="${m.l-9}" y="${cy+3.5}" text-anchor="end" fill="${CK.text}" font-size="10.5">${esc(r.deposito_id)}</text>`;
    out += `<path d="${barPath(m.l, cy-thick/2, sx(v), thick, "right")}" fill="${color}"></path>`;
    out += `<text x="${m.l+sx(v)+7}" y="${cy+3.5}" fill="${CK.text}" font-size="10.5">${fmt1(v)}x</text>`;
  });
  svg.innerHTML = out;
  bindSvgTooltip(svg);
}

function setBar(id, pct, color){
  const el = document.getElementById(id);
  if(!el) return;
  el.style.width = `${Math.max(1.5, Math.min(100, toNum(pct))).toFixed(1)}%`;
  if(color) el.style.background = color;
}

/* Banda de sensibilidad P10-P90 con la estimación puntual y el cero de referencia:
   el lector ve de un vistazo si todo el rango cae del mismo lado del cero. */
function drawRange(containerId, lo, hi, point){
  const svg = makeSvg(containerId); if(!svg) return;
  const CK = ink();
  const w = svg.viewBox.baseVal.width, h = svg.viewBox.baseVal.height;
  const values = [lo, hi, point, 0].map(toNum);
  const min = Math.min(...values), max = Math.max(...values);
  const span = (max-min) || 1;
  const pad = 10;
  const sx = (v)=> pad + ((toNum(v)-min)/span)*(w-pad*2);
  const cy = h/2;
  const negative = toNum(point) < 0;
  const color = negative ? CK.critical : CK.good;
  const x0 = Math.min(sx(lo), sx(hi)), x1 = Math.max(sx(lo), sx(hi));
  let out = `<line x1="${pad}" y1="${cy}" x2="${w-pad}" y2="${cy}" stroke="${CK.grid}" stroke-width="1"/>`;
  out += `<rect x="${x0}" y="${cy-4}" width="${Math.max(2, x1-x0)}" height="8" rx="4" fill="${color}" fill-opacity=".22"/>`;
  out += `<line x1="${sx(0)}" y1="${cy-7}" x2="${sx(0)}" y2="${cy+7}" stroke="${CK.axis}" stroke-width="1"/>`;
  out += `<circle cx="${sx(point)}" cy="${cy}" r="4" fill="${color}" stroke="${CK.surface}" stroke-width="2"/>`;
  out += `<rect x="0" y="0" width="${w}" height="${h}" fill="transparent" data-tip="${esc(`Escenario base ${fmtMoney(point)}\\nP10 ${fmtMoney(lo)} · P90 ${fmtMoney(hi)}\\nLa marca vertical es el cero`)}"></rect>`;
  svg.innerHTML = out;
  bindSvgTooltip(svg);
}

/* ---------- render ---------- */
function renderKPIs(d){
  const avail = toNum(metricSnapshot.fleet_availability_pct);
  const backlogPhysical = toNum(metricSnapshot.backlog_physical_items_count);
  const backlogCritical = toNum(metricSnapshot.backlog_critical_physical_count);
  const savings = toNum(metricSnapshot.cbm_operational_savings_eur);
  const topDepot = metricSnapshot.top_depot_by_saturation || "n/a";

  setText("k_avail", `${fmt1(avail)}%`);
  setText("k_mtbf", fmt0(metricSnapshot.mtbf_proxy_hours));
  setText("k_mttr", fmt1(metricSnapshot.mttr_proxy_hours));
  setText("k_uhr", fmt0(d.highRiskUnits.size));
  setText("k_bf", fmt0(backlogPhysical));
  setText("k_bv", fmt0(metricSnapshot.backlog_overdue_items_count));
  setText("k_bcf", fmt0(backlogCritical));
  setText("k_drh", fmt0(metricSnapshot.high_deferral_risk_cases_count));
  setText("k_ce", fmt0(metricSnapshot.avoidable_correctives_inspection));
  setText("k_ahorro", fmtMoney(savings));
  setText("k_alertas", fmt0(metricSnapshot.early_warnings_active_count));
  setText("k_sat", `${fmt0(metricSnapshot.mean_depot_saturation_pct)}%`);

  const availTarget = 90;
  const availDelta = avail - availTarget;
  const availEl = document.getElementById("k_avail_delta");
  if(availEl){
    const up = availDelta >= 0;
    availEl.className = `delta ${up ? "up" : "down"}`;
    const arrow = up ? "M5 0l5 9H0z" : "M5 9L0 0h10z";
    availEl.innerHTML = `<svg viewBox="0 0 10 9" aria-hidden="true"><path d="${arrow}"/></svg>${up ? "+" : "−"}${fmt1(Math.abs(availDelta))} pp frente al objetivo del 90%`;
  }

  const unitShare = d.uniqueUnits.size > 0 ? (d.highRiskUnits.size / d.uniqueUnits.size) * 100 : 0;
  setBar("k_uhr_fill", unitShare, "var(--critical)");
  setText("k_uhr_note", `${fmt0(unitShare)}% de las ${fmt0(d.uniqueUnits.size)} unidades de la vista`);

  const criticalShare = backlogPhysical > 0 ? (backlogCritical / backlogPhysical) * 100 : 0;
  setBar("k_bcf_fill", criticalShare, "var(--amber)");
  setText("k_bcf_sub", `${fmt0(criticalShare)}% de los ${fmt0(backlogPhysical)} pendientes físicos abiertos`);
  setText("k_sat_sub", topDepot !== "n/a"
    ? `máx. ${fmt0(metricSnapshot.top_depot_saturation_pct)}% en ${topDepot} · corte de calendario`
    : "capacidad usada");

  const valueTile = document.querySelector(".tile.value");
  if(valueTile){
    valueTile.classList.toggle("pos", savings >= 0);
    valueTile.classList.toggle("neg", savings < 0);
  }
  setText("k_ahorro_sub", savings >= 0
    ? "Ahorro operativo aproximado frente a operar hasta el fallo."
    : "Sobrecoste operativo aproximado frente a operar hasta el fallo.");

  const rangeLo = toNum(metricSnapshot.cbm_value_range_min_eur);
  const rangeHi = toNum(metricSnapshot.cbm_value_range_max_eur);
  drawRange("ch_range", rangeLo, rangeHi, savings);
  setText("k_ahorro_range", `Sensibilidad P10 ${fmtMoney(rangeLo)} a P90 ${fmtMoney(rangeHi)}`);

  setText("s_count_rows", fmt0(d.rows.length));
  setText("s_count_units", fmt0(d.uniqueUnits.size));
  setText("s_count_high", fmt0(d.highRiskUnits.size));
  setText("s_hours", fmt0(d.queueHours));
}

function evidenceItem(label, value, pct, color, note){
  const width = Math.max(1.5, Math.min(100, pct)).toFixed(1);
  return `<div class="ev">
    <b>${esc(label)}</b>
    <div class="ev-val">${esc(value)}</div>
    <div class="ev-track"><div class="ev-fill" style="width:${width}%;background:${color}"></div></div>
    <span class="ev-note">${esc(note)}</span>
  </div>`;
}

function renderDecision(d){
  const top = d.rows.slice().sort((a,b)=>toNum(b.intervention_priority_score)-toNum(a.intervention_priority_score))[0];
  const backlogCritical = toNum(metricSnapshot.backlog_critical_physical_count);
  const backlogOverdue = toNum(metricSnapshot.backlog_overdue_items_count);
  const backlogPhysical = toNum(metricSnapshot.backlog_physical_items_count);
  const pending = schedulingMetrics.find(r => String(r.scenario)==="heuristica_redisenada_35d");
  const pendingCapacityPct = pending ? toNum(pending.pendiente_capacidad_pct) : 0;
  const actionablePct = pending ? toNum(pending.actionable_pct) : 0;

  if(top){
    setText("exec_action", `${top.unidad_id} · ${top.componente_id}`);
    setText("exec_rule", `Regla ${top.decision_rule_id || "n/d"}`);
    setText("exec_action_note", `${titleCase(top.decision_type)} de ${top.component_family} en el sistema ${top.sistema_principal}. El factor dominante es ${top.main_risk_driver ? driverText(top.main_risk_driver).toLowerCase() : "no clasificado"} y la confianza del modelo es ${top.confidence_flag || "n/d"}.`);
    setText("exec_step_unit", String(top.deposito_recomendado || "n/d"));
    setText("exec_step_window", `${fmt0(top.suggested_window_days)} días`);
    setText("exec_step_hours", `${fmt1(top.hours_required)} h`);
    setHtml("evidenceGrid", [
      evidenceItem("Prioridad", fmt1(top.intervention_priority_score), toNum(top.intervention_priority_score), "var(--critical)", "sobre 100"),
      evidenceItem("Riesgo al diferir", fmt1(top.deferral_risk_score), toNum(top.deferral_risk_score), "var(--critical)", "sobre 100"),
      evidenceItem("Impacto en servicio", fmt1(top.service_impact_score), toNum(top.service_impact_score), "var(--amber)", "sobre 100"),
      evidenceItem("Salud", fmt1(top.health_score), toNum(top.health_score), "var(--good)", `RUL ${fmt0(top.component_rul_estimate)} días`)
    ].join(""));
  } else {
    setText("exec_action", "Sin decisión activa");
    setText("exec_rule", "—");
    setText("exec_action_note", "Ningún componente cumple los filtros aplicados. Restablece los filtros para recuperar la recomendación.");
    setText("exec_step_unit", "—");
    setText("exec_step_window", "—");
    setText("exec_step_hours", "—");
    setHtml("evidenceGrid", "");
  }

  setText("exec_step_deferral", `${fmt0(d.highDeferral)} de ${fmt0(d.rows.length)} en vista`);
  setText("exec_queue_cost", fmtMoney(d.queueCost));
  setText("exec_state", `${fmt1(backlogPhysical ? backlogOverdue/backlogPhysical*100 : 0)}% de los ${fmt0(backlogPhysical)} pendientes físicos están vencidos y ${fmt0(backlogCritical)} son críticos. La heurística rediseñada convierte ${fmt1(actionablePct)}% de los casos en acción y deja ${fmt1(pendingCapacityPct)}% sin ventana por capacidad.`);
  setText("exec_snapshot", meta.latest_depot_valid_date
    ? `${meta.latest_depot_valid_date}${meta.latest_depot_calendar_zero_backlog ? " · calendario reciente sin carga" : ""}`
    : "n/a");
  setText("exec_bottleneck", pendingCapacityPct >= 40 ? "Capacidad de taller" : "Repuestos y secuenciación");
  setText("decisionAside", `${fmt0(d.rows.length)} componentes · ${fmt0(d.queueHours)} h de taller`);
}

function renderAnomalies(){
  setHtml("anomalyGrid", anomaliesData.map(a => `
    <div class="anomaly ${esc(a.severity)}">
      <div class="flag"><span class="dot"></span>${esc(a.title)}</div>
      <div class="value">${esc(a.value)}</div>
      <div class="text">${esc(a.description)}</div>
    </div>
  `).join(""));
}

function strategyEconomics(){
  const label = {basada_en_condicion:"CBM", preventiva_rigida:"Preventiva rígida", reactiva:"Reactiva"};
  return strategyData
    .filter(r => toNum(r.horas_servicio_preservadas_vs_reactiva) > 0)
    .map(r => ({
      key: String(r.estrategia),
      label: label[String(r.estrategia)] || String(r.estrategia),
      value: Math.abs(toNum(r.ahorro_neto_vs_reactiva)) / toNum(r.horas_servicio_preservadas_vs_reactiva),
      hours: toNum(r.horas_servicio_preservadas_vs_reactiva),
      delta: toNum(r.ahorro_neto_vs_reactiva),
      availability: toNum(r.fleet_availability),
      prob: toNum(r.prob_ahorro_positivo)*100
    }))
    .sort((a,b)=>b.value-a.value);
}

function renderInsights(d){
  const economics = strategyEconomics();
  const cbm = economics.find(e => e.key === "basada_en_condicion");
  const alt = economics.find(e => e.key === "preventiva_rigida");
  const breakeven = toNum(metricSnapshot.cbm_breakeven_value_per_service_hour_eur);
  const availPp = toNum(metricSnapshot.cbm_vs_reactiva_availability_pp);
  const prob = toNum(metricSnapshot.cbm_prob_positive_savings)*100;
  const rmin = toNum(metricSnapshot.cbm_value_range_min_eur)/1e6;
  const rmax = toNum(metricSnapshot.cbm_value_range_max_eur)/1e6;
  const insight = document.getElementById("strategyInsight");
  if(insight && cbm){
    const compare = alt
      ? ` La preventiva rígida preserva el <b>${fmt0(alt.hours/cbm.hours*100)}%</b> de esas horas a <b>${fmt0(alt.value)} €</b> por hora, así que el caso base no sostiene CBM por coste: debe justificarse por riesgo, seguridad o por supuestos de coste recalibrados.`
      : "";
    insight.innerHTML = `En el escenario base CBM preserva <b>${fmt0(cbm.hours)} horas</b> de servicio frente a la estrategia reactiva (<b>+${fmt2(availPp)} pp</b> de disponibilidad) con un diferencial de <b>${fmtMoney(cbm.delta)}</b>, es decir un umbral de <b>${fmt0(breakeven)} €</b> por hora preservada. La probabilidad de ahorro positivo es del <b>${fmt1(prob)}%</b> y el rango de sensibilidad va de <b>${fmt2(rmin)} M€</b> a <b>${fmt2(rmax)} M€</b>.${compare}`;
  }

  const topUnit = metricSnapshot.top_unit_by_priority || "n/a";
  const worstFamily = d.familyRisk.length ? d.familyRisk[0] : null;
  setText("estadoAside", `Objetivo de disponibilidad 90% · corte ${meta.latest_depot_valid_date || "n/d"}`);
  setText("riesgoAside", worstFamily ? `Familia con mayor presión: ${worstFamily.label}` : "");
  setText("colaAside", `${fmt0(d.highRiskUnits.size)} unidades con prioridad 70+ · prioridad oficial ${topUnit}`);
  setText("tallerAside", d.worstDepot
    ? `${fmt0(d.blockedDepots.length)} de ${fmt0(d.depotSlice.length)} depósitos por encima de capacidad · máximo ${fmt1(d.worstDepot.saturation_ratio)}x en ${d.worstDepot.deposito_id}`
    : "");
  setText("factoresAside", `${fmt0(metricSnapshot.avoidable_downtime_hours_inspection)} h de indisponibilidad evitables por inspección`);
  setText("estrategiaAside", `Coste de diferir 14 días: ${fmtMoney(metricSnapshot.deferral_cost_delta_14d_eur)}`);
  setText("tablaAside", `${fmt0(d.rows.length)} filas · ${fmtMoney(d.queueCost)} de coste de retraso`);
}

function renderCharts(d){
  const CK = ink();
  const topUnitOfficial = String(metricSnapshot.top_unit_by_priority || "");

  const series = availabilitySeries();
  drawSparkline("ch_spark", series);
  if(series.length > 1){
    setText("k_avail_trend", `Serie semanal ${series[0].label} a ${series[series.length-1].label} · última semana ${fmt1(series[series.length-1].value)}%`);
  }

  drawPanelBars("ch_family", d.familyRisk.slice(0,6).map(f => ({
    label: f.label,
    risk: f.value*100,
    health: d.familyHealth.get(f.label) || 0,
    count: d.rows.filter(r => String(r.component_family) === f.label).length
  })), [
    {title:"Riesgo de fallo a 30 días (%)", key:"risk", color:CK.critical, fmt:(v)=>`${fmt0(v)}%`},
    {title:"Salud media (índice 0-100)", key:"health", color:CK.good, fmt:(v)=>fmt0(v)}
  ]);

  const rulOrder = {"0-14":0,"15-30":1,"31-60":2,"61-90":3,"91-180":4,"180+":5};
  drawColumns("ch_rul", d.rulBucket.slice().sort((a,b)=>(rulOrder[a.label] ?? 99)-(rulOrder[b.label] ?? 99)), {
    colorOf: (item) => (item.label === "0-14" || item.label === "15-30") ? CK.critical : CK.mute,
    valueFmt: (v)=>fmt0(v),
    unit: " componentes",
    axisTitle: "días de vida útil restante"
  });

  drawHBars("ch_window", d.windowLoad.map(x => ({...x, label: WINDOW_TEXT[x.label] || x.label})), {
    gutter: 84,
    labelChars: 12,
    colorOf: (_item, i) => [CK.e4, CK.e3, CK.e2, CK.e1][i] || CK.e1,
    valueFmt: (v)=>`${fmt0(v)} h`,
    tipOf: (x)=>`${x.label}\\n${fmt0(x.count)} componentes · ${fmt0(x.value)} h de taller\\nCoste de retraso ${fmtMoney(x.cost)}`
  });

  drawHBars("ch_top_units", d.topPriority, {
    gutter: 72,
    colorOf: (item) => item.label === topUnitOfficial ? CK.critical : CK.mute,
    valueFmt: (v)=>fmt1(v),
    tipOf: (x)=>`${x.label}${x.label === topUnitOfficial ? " · prioridad oficial nº1" : ""}\\nPrioridad media ${fmt1(x.value)}`
  });

  drawScatter("ch_priority_deferral", d.priorityDeferral);

  drawHBars("ch_decisions", d.decisionDist, {
    gutter: 152,
    labelChars: 24,
    color: CK.accent,
    valueFmt: (v)=>fmt0(v),
    unit: " componentes"
  });

  drawPressureBars("ch_depot", d.depotSlice);
  drawNestedBars("ch_backlog_depot", d.depotSlice);

  drawHBars("ch_drivers", d.driverDist.map(x => ({...x, label: driverText(x.label)})), {
    gutter: 138,
    labelChars: 21,
    color: CK.accent,
    valueFmt: (v)=>fmt0(v),
    unit: " componentes"
  });

  const inspection = inspectionData
    .map(r => ({label:String(r.family || "familia"), value:toNum(r.pre_failure_detection_rate)*100,
      lead:toNum(r.lead_time_medio_dias)}))
    .sort((a,b)=>b.value-a.value);
  drawHBars("ch_inspection", inspection, {
    gutter: 82,
    color: CK.accent,
    valueFmt: (v)=>`${fmt1(v)}%`,
    tipOf: (x)=>`${x.label}\\nDetección previa al fallo ${fmt1(x.value)}%\\nAntelación media ${fmt1(x.lead)} días`
  });

  const economics = strategyEconomics();
  drawHBars("ch_strategy", economics, {
    gutter: 124,
    labelChars: 18,
    colorOf: (item) => item.key === "basada_en_condicion" ? CK.critical : CK.mute,
    valueFmt: (v)=>`${fmt0(v)} €/h`,
    tipOf: (x)=>`${x.label} frente a reactiva\\nUmbral ${fmt0(x.value)} € por hora preservada\\n${fmt0(x.hours)} horas preservadas · diferencial ${fmtMoney(x.delta)}\\nDisponibilidad ${fmt2(x.availability)}% · probabilidad de ahorro ${fmt1(x.prob)}%`
  });

  drawSmallMultiples("ch_deferral", deferralData.map(r=>String(r.defer_dias)), [
    {title:"Coste total", color:CK.critical, values:deferralData.map(r=>toNum(r.costo_total_eur)), fmt:(v)=>fmtMoney(v)},
    {title:"Indisponibilidad", color:CK.accent, values:deferralData.map(r=>toNum(r.downtime_total_h)), fmt:(v)=>`${fmt0(v)} h`}
  ]);

  const scenarioLabel = {heuristica_redisenada_35d:"Rediseñada", base_inicial_voraz_21d:"Base inicial"};
  drawStacked("ch_scheduling", schedulingMetrics.map(r => ({
    label: scenarioLabel[String(r.scenario)] || String(r.scenario),
    captured: toNum(r.valor_capturado_proxy),
    missed: toNum(r.valor_no_capturado_proxy),
    note: `residual ${fmt0(toNum(r.riesgo_residual_no_atendido_pct))}%`
  })));
}

function renderTable(d){
  const head = document.getElementById("tableHead");
  const body = document.getElementById("tableBody");
  if(!head.dataset.ready){
    head.innerHTML = tableColumns
      .map(c => `<th scope="col" data-col="${c}"${numericColumns.has(c) ? ' class="num"' : ""}>${tableLabels[c] || c}</th>`)
      .join("");
    head.querySelectorAll("th").forEach(th => {
      th.addEventListener("click", () => {
        const c = th.dataset.col;
        if(sortKey===c){ sortAsc=!sortAsc; } else { sortKey=c; sortAsc=true; }
        filteredRows.sort((a,b)=>{
          const va=a[c], vb=b[c];
          const na=toNum(va), nb=toNum(vb);
          const bothNum = numericColumns.has(c) || !(Number.isNaN(na)||Number.isNaN(nb));
          if(bothNum) return sortAsc ? na-nb : nb-na;
          return sortAsc ? String(va).localeCompare(String(vb)) : String(vb).localeCompare(String(va));
        });
        head.querySelectorAll("th").forEach(other => other.removeAttribute("data-sort"));
        th.setAttribute("data-sort", sortAsc ? "asc" : "desc");
        renderAll();
      });
    });
    head.dataset.ready = "1";
  }
  if(!d.rows.length){
    body.innerHTML = `<tr><td colspan="${tableColumns.length}" class="empty-row">Sin resultados para los filtros actuales</td></tr>`;
    setText("pageInfo", "Página 0/0 · 0 filas");
    const prevEmpty = document.getElementById("btnPrevPage");
    const nextEmpty = document.getElementById("btnNextPage");
    if(prevEmpty) prevEmpty.disabled = true;
    if(nextEmpty) nextEmpty.disabled = true;
    return;
  }
  const totalPages = Math.max(1, Math.ceil(d.rows.length / pageSize));
  currentPage = Math.min(currentPage, totalPages);
  const start = (currentPage - 1) * pageSize;
  const pageRows = d.rows.slice(start, start + pageSize);

  body.innerHTML = pageRows.map(r => `
    <tr>${tableColumns.map(c => {
      if(c === "risk_level") return `<td>${badge(r[c])}</td>`;
      if(c === "coste_retraso_proxy") return `<td class="num">${fmtMoney(r[c])}</td>`;
      return `<td${numericColumns.has(c) ? ' class="num"' : ""}>${esc(r[c])}</td>`;
    }).join("")}</tr>
  `).join("");

  setText("pageInfo", `Página ${currentPage}/${totalPages} · ${fmt0(d.rows.length)} filas`);
  const prev = document.getElementById("btnPrevPage");
  const next = document.getElementById("btnNextPage");
  if(prev) prev.disabled = currentPage <= 1;
  if(next) next.disabled = currentPage >= totalPages;
}

function renderAll(){
  const d = computeDerived();
  renderKPIs(d);
  renderDecision(d);
  renderAnomalies();
  renderInsights(d);
  renderCharts(d);
  renderTable(d);
}

/* ---------- tema ---------- */
const ICON_SUN = '<svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="4.2"/><path d="M12 2v2.6M12 19.4V22M4.2 4.2l1.9 1.9M17.9 17.9l1.9 1.9M2 12h2.6M19.4 12H22M4.2 19.8l1.9-1.9M17.9 6.1l1.9-1.9"/></svg>';
const ICON_MOON = '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M20 14.2A8.2 8.2 0 019.8 4a8.4 8.4 0 106.9 12.5A8.3 8.3 0 0120 14.2z"/></svg>';

function applyTheme(theme){
  document.documentElement.setAttribute("data-theme", theme);
  const btn = document.getElementById("btnTheme");
  if(btn){
    const dark = theme === "dark";
    btn.innerHTML = dark ? ICON_SUN : ICON_MOON;
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
document.body.classList.add("first-paint");
window.setTimeout(() => document.body.classList.remove("first-paint"), 900);

let resizeTimer = null;
window.addEventListener("resize", () => {
  clearTimeout(resizeTimer);
  resizeTimer = setTimeout(() => renderAll(), 180);
});
window.addEventListener("scroll", syncFloatingControls, { passive:true });
"""

_PAGE = """<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Centro de control CBM ferroviario</title>
  <meta name="description" content="Panel de decisión para mantenimiento ferroviario basado en condición: cola de taller, riesgo de diferimiento y valor de la estrategia." />
  <meta name="dashboard-version" content="__DASHBOARD_VERSION__" />
  <meta name="dashboard-signature" content="__PAYLOAD_SIGNATURE__" />
  <meta name="theme-color" content="#f1efea" media="(prefers-color-scheme: light)" />
  <meta name="theme-color" content="#0c0e10" media="(prefers-color-scheme: dark)" />
  <style>
__CSS__
  </style>
</head>
<body>
__BODY__
<script>
__SCRIPT__
</script>
</body>
</html>
"""


def _render_page(payload: dict, context: dict) -> str:
    css = _CSS.replace("__FONT_FACES__", _embedded_font_faces())
    html = (
        _PAGE.replace("__CSS__", css.strip("\n"))
        .replace("__BODY__", _BODY.strip("\n"))
        .replace("__SCRIPT__", _SCRIPT.strip("\n"))
    )
    for token, value in context.items():
        html = html.replace(token, value)
    return html.replace("__PAYLOAD__", _json_for_script(payload))


def _redirect_html(target_url: str) -> str:
    return "\n".join(
        [
            "<!DOCTYPE html>",
            '<html lang="es">',
            "<head>",
            '  <meta charset="UTF-8" />',
            '  <meta name="viewport" content="width=device-width, initial-scale=1" />',
            f'  <meta http-equiv="refresh" content="0; url={target_url}" />',
            "  <title>Centro de control CBM ferroviario</title>",
            "  <style>",
            "    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }",
            '    html, body { height: 100%; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif; background: #f1efea; color: #15181c; }',
            "    body { display: flex; align-items: center; justify-content: center; padding: 24px; }",
            "    main { text-align: center; max-width: 420px; }",
            "    .label { font-size: .7rem; font-weight: 600; letter-spacing: .12em; text-transform: uppercase; color: #2c5fa8; margin-bottom: 1rem; }",
            "    h1 { font-size: 1.5rem; font-weight: 600; letter-spacing: -.02em; margin-bottom: .5rem; }",
            "    p { font-size: .9rem; color: #6b7280; margin-bottom: 1.5rem; line-height: 1.5; }",
            "    a { color: #2c5fa8; font-weight: 600; text-decoration: none; }",
            "    a:hover { text-decoration: underline; }",
            "    .spinner { width: 22px; height: 22px; border: 2px solid #e2dfd7; border-top-color: #2c5fa8; border-radius: 50%; animation: spin .7s linear infinite; margin: 0 auto 1rem; }",
            "    @keyframes spin { to { transform: rotate(360deg); } }",
            "    @media (prefers-reduced-motion: reduce) { .spinner { animation: none; } }",
            "    @media (prefers-color-scheme: dark) { html, body { background: #0c0e10; color: #f2f3f4; } p { color: #9198a1; } a, .label { color: #5e90de; } .spinner { border-color: #282d33; border-top-color: #5e90de; } }",
            "  </style>",
            "</head>",
            "<body>",
            "  <main>",
            '    <div class="label">Mantenimiento basado en condición</div>',
            "    <h1>Centro de control CBM ferroviario</h1>",
            "    <p>Redirigiendo al panel operativo.</p>",
            '    <div class="spinner"></div>',
            f'    <p><a href="{target_url}">Abrir panel de control</a></p>',
            "  </main>",
            "</body>",
            "</html>",
        ]
    )


def build_dashboard() -> str:
    """Construye el panel y devuelve la ruta del HTML canónico."""
    OUTPUTS_DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
    payload, context = _build_payload()
    html = _render_page(payload, context)

    branded_path = OUTPUTS_DASHBOARD_DIR / DASHBOARD_SLUG
    branded_path.write_text(html, encoding="utf-8")

    root_index = ROOT_DIR / "index.html"
    root_index.write_text(_redirect_html(f"outputs/dashboard/{DASHBOARD_SLUG}") + "\n", encoding="utf-8")
    (ROOT_DIR / ".nojekyll").write_text("", encoding="utf-8")
    return str(branded_path)


if __name__ == "__main__":
    build_dashboard()
