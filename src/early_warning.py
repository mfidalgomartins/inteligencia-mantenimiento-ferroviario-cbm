from __future__ import annotations

import numpy as np
import pandas as pd

from src.config import DATA_PROCESSED_DIR, OUTPUTS_REPORTS_DIR


def run_early_warning_rules() -> pd.DataFrame:
    scoring = pd.read_csv(DATA_PROCESSED_DIR / "scoring_componentes.csv")
    rul = pd.read_csv(DATA_PROCESSED_DIR / "component_rul_estimate.csv")

    df = scoring.merge(
        rul[["unidad_id", "componente_id", "component_rul_estimate", "confidence_flag"]],
        on=["unidad_id", "componente_id"],
        how="left",
    )
    if "confidence_flag" not in df.columns:
        if "confidence_flag_x" in df.columns and "confidence_flag_y" in df.columns:
            df["confidence_flag"] = df["confidence_flag_x"].fillna(df["confidence_flag_y"])
        elif "confidence_flag_x" in df.columns:
            df["confidence_flag"] = df["confidence_flag_x"]
        elif "confidence_flag_y" in df.columns:
            df["confidence_flag"] = df["confidence_flag_y"]
        else:
            df["confidence_flag"] = "media"

    # Umbrales adaptativos por familia para reducir colapso de clases altas.
    risk_q75 = df.groupby("component_family")["prob_fallo_30d"].transform(lambda s: s.quantile(0.75))
    risk_q90 = df.groupby("component_family")["prob_fallo_30d"].transform(lambda s: s.quantile(0.90))
    health_q30 = df.groupby("component_family")["health_score"].transform(lambda s: s.quantile(0.30))
    health_q15 = df.groupby("component_family")["health_score"].transform(lambda s: s.quantile(0.15))
    rul_q25 = df.groupby("component_family")["component_rul_estimate"].transform(lambda s: s.quantile(0.25))
    rul_q10 = df.groupby("component_family")["component_rul_estimate"].transform(lambda s: s.quantile(0.10))

    df["risk_thr_high"] = risk_q75.clip(0.45, 0.72)
    df["risk_thr_critical"] = risk_q90.clip(0.62, 0.86)
    df["health_thr_low"] = health_q30.clip(32, 52)
    df["health_thr_critical"] = health_q15.clip(20, 40)
    df["rul_thr_short"] = rul_q25.clip(20, 70)
    df["rul_thr_critical"] = rul_q10.clip(8, 45)

    df["regla_riesgo_alto"] = (df["prob_fallo_30d"] >= df["risk_thr_high"]).astype(int)
    df["regla_salud_baja"] = (df["health_score"] <= df["health_thr_low"]).astype(int)
    df["regla_rul_corto"] = (df["component_rul_estimate"].fillna(365) <= df["rul_thr_short"]).astype(int)
    df["regla_driver_critico"] = (
        df["main_risk_driver"].isin(["backlog", "repetitividad", "anomalias"])
        & (df["prob_fallo_30d"] >= df["risk_thr_high"])
    ).astype(int)

    df["n_reglas_activas"] = df[["regla_riesgo_alto", "regla_salud_baja", "regla_rul_corto", "regla_driver_critico"]].sum(axis=1)

    df["nivel_alerta"] = "sin_alerta"
    df.loc[df["n_reglas_activas"] >= 1, "nivel_alerta"] = "preventiva"
    df.loc[(df["n_reglas_activas"] >= 2) | (df["prob_fallo_30d"] >= df["risk_thr_critical"]), "nivel_alerta"] = "alta"
    df.loc[
        (df["n_reglas_activas"] >= 3)
        | (
            (df["prob_fallo_30d"] >= df["risk_thr_critical"])
            & (df["component_rul_estimate"].fillna(365) <= df["rul_thr_critical"])
        )
        | (df["health_score"] <= df["health_thr_critical"]),
        "nivel_alerta",
    ] = "critica"

    action_map = {
        "sin_alerta": "monitorizar_intensivamente",
        "preventiva": "inspeccion_manual_72h",
        "alta": "programar_ventana_proxima",
        "critica": "intervenir_ahora",
    }
    df["accion_recomendada"] = df["nivel_alerta"].map(action_map)

    out = df[
        [
            "fecha",
            "unidad_id",
            "componente_id",
            "component_family",
            "health_score",
            "prob_fallo_30d",
            "riesgo_ajustado_negocio",
            "component_rul_estimate",
            "confidence_flag",
            "main_risk_driver",
            "n_reglas_activas",
            "nivel_alerta",
            "accion_recomendada",
        ]
    ].sort_values(["nivel_alerta", "riesgo_ajustado_negocio"], ascending=[True, False])

    out.to_csv(DATA_PROCESSED_DIR / "alertas_tempranas.csv", index=False)

    # resumen para reporte
    summary = (
        out.groupby("nivel_alerta", as_index=False)
        .agg(
            n_componentes=("componente_id", "nunique"),
            riesgo_medio=("prob_fallo_30d", "mean"),
            rul_medio=("component_rul_estimate", "mean"),
        )
        .sort_values("n_componentes", ascending=False)
    )
    OUTPUTS_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    summary.to_csv(OUTPUTS_REPORTS_DIR / "early_warning_summary.csv", index=False)

    return out


if __name__ == "__main__":
    run_early_warning_rules()
