from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

from src.config import DATA_RAW_DIR, OUTPUTS_REPORTS_DIR


REPORT_DIR = OUTPUTS_REPORTS_DIR / "explore_data"


@dataclass
class TableMeta:
    grain: str
    candidate_key: List[str]
    expected_fks: Dict[str, str]


TABLE_META: Dict[str, TableMeta] = {
    "flotas": TableMeta(
        grain="1 fila por flota",
        candidate_key=["flota_id"],
        expected_fks={},
    ),
    "unidades": TableMeta(
        grain="1 fila por unidad",
        candidate_key=["unidad_id"],
        expected_fks={"flota_id": "flotas.flota_id", "deposito_id": "depositos.deposito_id"},
    ),
    "depositos": TableMeta(
        grain="1 fila por depósito",
        candidate_key=["deposito_id"],
        expected_fks={},
    ),
    "componentes_criticos": TableMeta(
        grain="1 fila por componente crítico instalado",
        candidate_key=["componente_id"],
        expected_fks={"unidad_id": "unidades.unidad_id"},
    ),
    "sensores_componentes": TableMeta(
        grain="1 fila por timestamp-unidad-componente-sensor",
        candidate_key=["timestamp", "unidad_id", "componente_id", "sensor_tipo"],
        expected_fks={"unidad_id": "unidades.unidad_id", "componente_id": "componentes_criticos.componente_id"},
    ),
    "inspecciones_automaticas": TableMeta(
        grain="1 fila por evento de inspección automática",
        candidate_key=["inspeccion_id"],
        expected_fks={"unidad_id": "unidades.unidad_id", "componente_id": "componentes_criticos.componente_id"},
    ),
    "eventos_mantenimiento": TableMeta(
        grain="1 fila por evento de mantenimiento",
        candidate_key=["mantenimiento_id"],
        expected_fks={
            "unidad_id": "unidades.unidad_id",
            "componente_id": "componentes_criticos.componente_id",
            "deposito_id": "depositos.deposito_id",
        },
    ),
    "fallas_historicas": TableMeta(
        grain="1 fila por falla registrada",
        candidate_key=["falla_id"],
        expected_fks={"unidad_id": "unidades.unidad_id", "componente_id": "componentes_criticos.componente_id"},
    ),
    "alertas_operativas": TableMeta(
        grain="1 fila por alerta operacional",
        candidate_key=["alerta_id"],
        expected_fks={"unidad_id": "unidades.unidad_id", "componente_id": "componentes_criticos.componente_id"},
    ),
    "intervenciones_programadas": TableMeta(
        grain="1 fila por intervención programada",
        candidate_key=["intervencion_id"],
        expected_fks={
            "unidad_id": "unidades.unidad_id",
            "componente_id": "componentes_criticos.componente_id",
            "deposito_id": "depositos.deposito_id",
        },
    ),
    "disponibilidad_servicio": TableMeta(
        grain="1 fila por unidad y fecha",
        candidate_key=["fecha", "unidad_id"],
        expected_fks={"unidad_id": "unidades.unidad_id", "flota_id": "flotas.flota_id"},
    ),
    "asignacion_servicio": TableMeta(
        grain="1 fila por unidad y fecha",
        candidate_key=["fecha", "unidad_id"],
        expected_fks={"unidad_id": "unidades.unidad_id"},
    ),
    "backlog_mantenimiento": TableMeta(
        grain="1 fila por snapshot fecha-depósito-unidad-componente",
        candidate_key=["fecha", "deposito_id", "unidad_id", "componente_id"],
        expected_fks={
            "unidad_id": "unidades.unidad_id",
            "component_id": "componentes_criticos.componente_id",
            "deposito_id": "depositos.deposito_id",
        },
    ),
    "parametros_operativos_contexto": TableMeta(
        grain="1 fila por fecha y línea",
        candidate_key=["fecha", "linea_servicio"],
        expected_fks={},
    ),
    "escenarios_mantenimiento": TableMeta(
        grain="1 fila por fecha y escenario",
        candidate_key=["fecha", "escenario"],
        expected_fks={},
    ),
}


def _classify_column(col: str, dtype: str) -> str:
    c = col.lower()
    if c.endswith("_id") or c in {"falla_id", "alerta_id", "inspeccion_id", "mantenimiento_id", "intervencion_id"}:
        return "identificadores"
    if "fecha" in c or "timestamp" in c or c.startswith("ano"):
        return "temporales"
    if c.endswith("_flag") or c.startswith("es_") or c.startswith("is_"):
        return "booleanas"
    if any(k in c for k in ["tipo", "modo", "familia", "estrategia", "resultado", "motivo", "region", "linea", "operador", "severidad"]):
        return "dimensiones"
    if any(k in c for k in ["ratio", "score", "riesgo", "coste", "horas", "km", "temperatura", "vibracion", "presion", "desgaste", "corriente", "ruido", "intensidad", "puntualidad", "availability", "disponibilidad"]):
        return "metricas"
    if dtype in {"int64", "float64"}:
        return "metricas"
    if dtype.startswith("datetime"):
        return "temporales"
    return "estructurales"


def _find_temporal_col(df: pd.DataFrame) -> str | None:
    for c in ["timestamp", "fecha", "fecha_falla", "fecha_inicio", "fecha_programada", "fecha_entrada_servicio"]:
        if c in df.columns:
            return c
    return None


def _numeric_profile(df: pd.DataFrame, table: str) -> pd.DataFrame:
    rows = []
    for col in df.select_dtypes(include=[np.number]).columns:
        s = df[col].dropna()
        if len(s) == 0:
            continue
        rows.append(
            {
                "tabla": table,
                "columna": col,
                "min": float(s.min()),
                "p01": float(np.percentile(s, 1)),
                "p50": float(np.percentile(s, 50)),
                "p99": float(np.percentile(s, 99)),
                "max": float(s.max()),
                "mean": float(s.mean()),
                "std": float(s.std(ddof=0)),
            }
        )
    return pd.DataFrame(rows)


def _table_specific_checks(tables: Dict[str, pd.DataFrame]) -> List[dict]:
    issues: List[dict] = []

    comp = tables["componentes_criticos"]
    mask_life = (comp["edad_componente_dias"] <= 0) | (comp["vida_util_teorica_dias"] <= 0)
    if int(mask_life.sum()) > 0:
        issues.append({
            "severidad": "alta",
            "issue": "componentes_vida_util_imposible",
            "tabla": "componentes_criticos",
            "detalle": f"{int(mask_life.sum())} filas con edad/vida util no positiva",
            "impacto": "Distorsiona RUL y health scoring",
        })

    sens = tables["sensores_componentes"]
    out_temp = int(((sens["temperatura_operacion"] < -30) | (sens["temperatura_operacion"] > 180)).sum())
    out_vibr = int(((sens["vibracion_proxy"] < 0) | (sens["vibracion_proxy"] > 30)).sum())
    out_wear = int(((sens["desgaste_proxy"] < 0) | (sens["desgaste_proxy"] > 200)).sum())
    if out_temp + out_vibr + out_wear > 0:
        issues.append({
            "severidad": "alta",
            "issue": "sensores_fuera_rango",
            "tabla": "sensores_componentes",
            "detalle": f"temp={out_temp}, vibr={out_vibr}, desgaste={out_wear}",
            "impacto": "Afecta degradación, alertas, riesgo y dashboard",
        })

    fail = tables["fallas_historicas"]
    no_impact = int(((fail["impacto_en_servicio"].isna()) | (fail["tiempo_fuera_servicio_horas"] <= 0)).sum())
    if no_impact > 0:
        issues.append({
            "severidad": "alta",
            "issue": "fallas_sin_impacto_asociado",
            "tabla": "fallas_historicas",
            "detalle": f"{no_impact} fallas sin impacto o downtime válido",
            "impacto": "Debilita MTTR, indisponibilidad y evaluación estratégica",
        })

    maint = tables["eventos_mantenimiento"]
    start = pd.to_datetime(maint["fecha_inicio"], errors="coerce")
    end = pd.to_datetime(maint["fecha_fin"], errors="coerce")
    bad_time = int((start > end).sum())
    missing_result = int(maint["resultado_intervencion"].isna().sum())
    if bad_time > 0:
        issues.append({
            "severidad": "alta",
            "issue": "mantenimientos_con_tiempo_incoherente",
            "tabla": "eventos_mantenimiento",
            "detalle": f"{bad_time} eventos con fecha_inicio > fecha_fin",
            "impacto": "Afecta carga de taller, MTTR y scheduling",
        })
    if missing_result > 0:
        issues.append({
            "severidad": "media",
            "issue": "mantenimientos_sin_resultado",
            "tabla": "eventos_mantenimiento",
            "detalle": f"{missing_result} eventos sin resultado_intervencion",
            "impacto": "Complica evaluación de efectividad del mantenimiento",
        })

    backlog = tables["backlog_mantenimiento"]
    bad_backlog = int(((backlog["antiguedad_backlog_dias"] < 0) | (backlog["riesgo_acumulado"] < 0)).sum())
    if bad_backlog > 0:
        issues.append({
            "severidad": "alta",
            "issue": "backlog_inconsistente",
            "tabla": "backlog_mantenimiento",
            "detalle": f"{bad_backlog} filas con antigüedad o riesgo negativos",
            "impacto": "Distorsiona presión de depósito y priorización",
        })

    disp = tables["disponibilidad_servicio"]
    recon_error = float((disp["horas_planificadas"] - (disp["horas_disponibles"] + disp["horas_no_disponibles"])).abs().mean())
    if recon_error > 0.02:
        issues.append({
            "severidad": "media",
            "issue": "coherencia_horas_disponibilidad",
            "tabla": "disponibilidad_servicio",
            "detalle": f"error medio de reconciliación={recon_error:.4f}",
            "impacto": "Afecta KPI de disponibilidad e impacto operativo",
        })

    return issues


def run_explore_data_audit() -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    tables = {name: pd.read_csv(DATA_RAW_DIR / f"{name}.csv") for name in TABLE_META}

    profile_rows: List[dict] = []
    column_rows: List[dict] = []
    numeric_profiles: List[pd.DataFrame] = []
    issues: List[dict] = []

    for table_name, df in tables.items():
        meta = TABLE_META[table_name]
        rows = len(df)
        cols = df.shape[1]

        temporal_col = _find_temporal_col(df)
        if temporal_col is not None:
            dt = pd.to_datetime(df[temporal_col], errors="coerce")
            tmin = dt.min()
            tmax = dt.max()
            temporal_cov = f"{'' if pd.isna(tmin) else tmin.date()} -> {'' if pd.isna(tmax) else tmax.date()}"
        else:
            temporal_cov = "n/a"

        key_cols = [k for k in meta.candidate_key if k in df.columns]
        duplicate_key_rows = int(df.duplicated(subset=key_cols).sum()) if key_cols else 0
        null_key_rows = int(df[key_cols].isna().any(axis=1).sum()) if key_cols else 0

        if duplicate_key_rows > 0:
            sev = "alta" if duplicate_key_rows / max(rows, 1) > 0.001 else "media"
            issues.append(
                {
                    "severidad": sev,
                    "issue": "duplicados_en_candidate_key",
                    "tabla": table_name,
                    "detalle": f"{duplicate_key_rows} duplicados sobre key={key_cols}",
                    "impacto": "Puede romper joins y agregaciones de marts",
                }
            )

        if null_key_rows > 0:
            issues.append(
                {
                    "severidad": "alta",
                    "issue": "nulls_en_candidate_key",
                    "tabla": table_name,
                    "detalle": f"{null_key_rows} filas con null en key={key_cols}",
                    "impacto": "Riesgo alto de orfandad y pérdida de trazabilidad",
                }
            )

        profile_rows.append(
            {
                "tabla": table_name,
                "grain": meta.grain,
                "candidate_key": ", ".join(key_cols),
                "foreign_keys_esperadas": ", ".join([f"{k}->{v}" for k, v in meta.expected_fks.items()]),
                "n_filas": rows,
                "n_columnas": cols,
                "cobertura_temporal": temporal_cov,
                "null_rate_promedio_pct": round(float(df.isna().mean().mean() * 100), 4),
                "duplicados_candidate_key": duplicate_key_rows,
            }
        )

        for col in df.columns:
            series = df[col]
            column_rows.append(
                {
                    "tabla": table_name,
                    "columna": col,
                    "dtype": str(series.dtype),
                    "clasificacion": _classify_column(col, str(series.dtype)),
                    "null_pct": round(float(series.isna().mean() * 100), 4),
                    "n_unique": int(series.nunique(dropna=True)),
                    "cardinality_pct": round(float(series.nunique(dropna=True) / max(rows, 1) * 100), 4),
                    "sample_values": " | ".join(series.dropna().astype(str).head(3).tolist()),
                }
            )

            null_pct = float(series.isna().mean() * 100)
            if null_pct > 40 and _classify_column(col, str(series.dtype)) in {"metricas", "temporales", "identificadores"}:
                issues.append(
                    {
                        "severidad": "media",
                        "issue": "null_rate_critico",
                        "tabla": table_name,
                        "detalle": f"{col} con null_pct={null_pct:.2f}",
                        "impacto": "Puede sesgar features, scoring y visualización",
                    }
                )

        numeric_profiles.append(_numeric_profile(df, table_name))

    # FK validations
    fk_issues = _validate_foreign_keys(tables)
    issues.extend(fk_issues)

    # Domain-specific validations
    issues.extend(_table_specific_checks(tables))

    summary_df = pd.DataFrame(profile_rows).sort_values("tabla")
    column_df = pd.DataFrame(column_rows).sort_values(["tabla", "columna"])
    numeric_df = pd.concat(numeric_profiles, ignore_index=True) if numeric_profiles else pd.DataFrame()
    issues_df = pd.DataFrame(issues).drop_duplicates().sort_values(["severidad", "tabla", "issue"]) if issues else pd.DataFrame(
        columns=["severidad", "issue", "tabla", "detalle", "impacto"]
    )

    joins_df = _build_official_joins()
    marts_df = _build_candidate_marts()

    summary_df.to_csv(REPORT_DIR / "table_profile_summary.csv", index=False)
    column_df.to_csv(REPORT_DIR / "column_profile_classification.csv", index=False)
    numeric_df.to_csv(REPORT_DIR / "numeric_distribution_profile.csv", index=False)
    issues_df.to_csv(REPORT_DIR / "issues_prioritized.csv", index=False)
    joins_df.to_csv(REPORT_DIR / "official_joins_proposal.csv", index=False)
    marts_df.to_csv(REPORT_DIR / "candidate_marts_proposal.csv", index=False)

    report_md = _build_markdown_report(summary_df, issues_df, joins_df, marts_df)
    (REPORT_DIR / "explore_data_report.md").write_text(report_md, encoding="utf-8")
    (REPORT_DIR / "explore_data_report.html").write_text(_markdown_to_html(report_md), encoding="utf-8")


def _validate_foreign_keys(tables: Dict[str, pd.DataFrame]) -> List[dict]:
    issues: List[dict] = []
    key_values = {
        "flotas.flota_id": set(tables["flotas"]["flota_id"].astype(str)),
        "unidades.unidad_id": set(tables["unidades"]["unidad_id"].astype(str)),
        "depositos.deposito_id": set(tables["depositos"]["deposito_id"].astype(str)),
        "componentes_criticos.componente_id": set(tables["componentes_criticos"]["componente_id"].astype(str)),
    }

    for table_name, meta in TABLE_META.items():
        df = tables[table_name]
        for fk_col, ref in meta.expected_fks.items():
            if fk_col not in df.columns:
                continue
            ref_values = key_values.get(ref)
            if ref_values is None:
                continue
            orphan_count = int((~df[fk_col].astype(str).isin(ref_values)).sum())
            if orphan_count > 0:
                issues.append(
                    {
                        "severidad": "alta",
                        "issue": "foreign_key_orphan",
                        "tabla": table_name,
                        "detalle": f"{fk_col} -> {ref}: orphans={orphan_count}",
                        "impacto": "Joins incompletos y sesgo en aggregation/scoring",
                    }
                )

    return issues


def _build_official_joins() -> pd.DataFrame:
    rows = [
        ("componentes_criticos", "unidades", "componentes_criticos.unidad_id = unidades.unidad_id", "jerarquía activo"),
        ("unidades", "flotas", "unidades.flota_id = flotas.flota_id", "nivel flota"),
        ("unidades", "depositos", "unidades.deposito_id = depositos.deposito_id", "capacidad y especialización"),
        ("sensores_componentes", "componentes_criticos", "sensores_componentes.componente_id = componentes_criticos.componente_id", "condición por activo"),
        ("inspecciones_automaticas", "componentes_criticos", "inspecciones_automaticas.componente_id = componentes_criticos.componente_id", "defectos detectados"),
        ("fallas_historicas", "componentes_criticos", "fallas_historicas.componente_id = componentes_criticos.componente_id", "historial de fallo"),
        ("eventos_mantenimiento", "componentes_criticos", "eventos_mantenimiento.componente_id = componentes_criticos.componente_id", "historial de intervención"),
        ("alertas_operativas", "componentes_criticos", "alertas_operativas.componente_id = componentes_criticos.componente_id", "early warning"),
        ("disponibilidad_servicio", "asignacion_servicio", "disponibilidad_servicio.fecha = asignacion_servicio.fecha AND disponibilidad_servicio.unidad_id = asignacion_servicio.unidad_id", "impacto servicio"),
        ("backlog_mantenimiento", "intervenciones_programadas", "backlog_mantenimiento.componente_id = intervenciones_programadas.componente_id AND backlog_mantenimiento.unidad_id = intervenciones_programadas.unidad_id", "presión de taller"),
    ]
    return pd.DataFrame(rows, columns=["left_table", "right_table", "join_condition", "purpose"])


def _build_candidate_marts() -> pd.DataFrame:
    rows = [
        ("mart_component_day", "componente-dia", "salud, degradación, alertas, fallas, mantenimiento", "scoring y RUL"),
        ("mart_unit_day", "unidad-dia", "riesgo agregado, indisponibilidad, backlog, impacto servicio", "priorización operativa"),
        ("mart_depot_day", "deposito-dia", "saturación, carga correctiva/programada, riesgo pendiente", "planificación de taller"),
        ("mart_fleet_week", "flota-semana", "availability, MTBF/MTTR proxy, tendencia estratégica", "dirección de mantenimiento"),
        ("mart_condition_value", "global-periodo", "alertas tempranas, correctivas evitables, valor CBM", "business case"),
    ]
    return pd.DataFrame(rows, columns=["mart_name", "grain", "core_content", "main_use"])


def _build_markdown_report(
    summary_df: pd.DataFrame,
    issues_df: pd.DataFrame,
    joins_df: pd.DataFrame,
    marts_df: pd.DataFrame,
) -> str:
    lines: List[str] = []
    lines.append("# Explore-Data Audit | Railway CBM")
    lines.append("")
    lines.append("## Objetivo")
    lines.append("Auditoría formal de calidad y readiness de datos previa a modelado, scoring, RUL, priorización y dashboard.")
    lines.append("")
    lines.append("## Resumen por dataset")
    lines.append(summary_df.to_markdown(index=False))
    lines.append("")

    total_issues = len(issues_df)
    high = int((issues_df["severidad"] == "alta").sum()) if total_issues > 0 else 0
    med = int((issues_df["severidad"] == "media").sum()) if total_issues > 0 else 0
    low = int((issues_df["severidad"] == "baja").sum()) if total_issues > 0 else 0

    lines.append("## Issues priorizados")
    lines.append(f"- Total issues: {total_issues}")
    lines.append(f"- Alta severidad: {high}")
    lines.append(f"- Media severidad: {med}")
    lines.append(f"- Baja severidad: {low}")
    lines.append("")

    if total_issues > 0:
        lines.append(issues_df.to_markdown(index=False))
    else:
        lines.append("Sin issues críticos detectados en esta corrida.")
    lines.append("")

    lines.append("## Recomendaciones de transformación analítica")
    lines.append("- Consolidar un `component_day` con ventanas rolling y señales de estrés para soporte directo a scoring y RUL.")
    lines.append("- Normalizar severidades categóricas a ordinales para modelos interpretables y comparables entre dominios.")
    lines.append("- Mantener snapshots de backlog con frecuencia fija para análisis de presión de taller robusto.")
    lines.append("- Controlar drift de sensores por familia de componente antes de ajustar thresholds de alerta.")
    lines.append("- Versionar reglas de early warning y su precisión operacional por familia (wheel/brake/bogie/pantograph).")
    lines.append("")

    lines.append("## Propuesta de joins oficiales")
    lines.append(joins_df.to_markdown(index=False))
    lines.append("")

    lines.append("## Propuesta de marts analíticos")
    lines.append(marts_df.to_markdown(index=False))
    lines.append("")

    lines.append("## Impacto en scoring, RUL y dashboard")
    lines.append("- Nulls o duplicados en llaves de componente/unidad afectan directamente calidad de features y estabilidad del ranking de riesgo.")
    lines.append("- Incoherencias temporales en mantenimiento alteran `days_since_last_maintenance` y degradan priorización de taller.")
    lines.append("- Señales fuera de rango generan falsos positivos de alerta y sesgo en estimaciones de health/failure risk.")
    lines.append("- Fallas sin impacto asociado subestiman MTTR, indisponibilidad y valor de estrategias CBM.")

    return "\n".join(lines)


def _markdown_to_html(md_text: str) -> str:
    escaped = md_text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return f"""<!DOCTYPE html>
<html lang='es'>
<head>
  <meta charset='utf-8'>
  <title>Explore Data Report</title>
  <style>
    body {{ font-family: 'Avenir Next', 'Segoe UI', sans-serif; margin: 24px auto; width: min(1200px, 94vw); background: #f7fafc; color: #0f1720; }}
    pre {{ white-space: pre-wrap; background: #ffffff; border: 1px solid #e2e8f0; border-radius: 8px; padding: 16px; }}
    h1 {{ color: #0b4f6c; }}
  </style>
</head>
<body>
  <pre>{escaped}</pre>
</body>
</html>
"""


if __name__ == "__main__":
    run_explore_data_audit()
