from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"


def _latest_component_semantics() -> pd.DataFrame:
    comp = pd.read_csv(PROCESSED / "component_day_features.csv")
    comp["fecha"] = pd.to_datetime(comp["fecha"], errors="coerce")
    latest = comp["fecha"].max()
    return comp[comp["fecha"] == latest].copy()


def _merge_semantic_columns(score: pd.DataFrame) -> pd.DataFrame:
    comp_latest = _latest_component_semantics()[["unidad_id", "componente_id", "deterioration_index", "maintenance_restoration_index"]]
    df = score.merge(
        comp_latest.rename(
            columns={
                "deterioration_index": "deterioration_index_aux",
                "maintenance_restoration_index": "maintenance_restoration_index_aux",
            }
        ),
        on=["unidad_id", "componente_id"],
        how="left",
    )
    if "deterioration_index" not in df.columns:
        df["deterioration_index"] = df["deterioration_index_aux"]
    else:
        df["deterioration_index"] = df["deterioration_index"].fillna(df["deterioration_index_aux"])
    if "maintenance_restoration_index" not in df.columns:
        df["maintenance_restoration_index"] = df["maintenance_restoration_index_aux"]
    else:
        df["maintenance_restoration_index"] = df["maintenance_restoration_index"].fillna(df["maintenance_restoration_index_aux"])
    return df


def test_semantic_ranges_component_layer():
    comp = pd.read_csv(PROCESSED / "component_day_features.csv")
    assert comp["estimated_health_index"].between(0, 100).all()
    assert comp["deterioration_input_index"].between(0, 100).all()
    assert comp["deterioration_index"].between(0, 100).all()
    assert comp["maintenance_restoration_index"].between(0, 100).all()
    assert comp["degradation_velocity"].between(0, 10).all()


def test_sql_health_deterioration_balance():
    val = pd.read_csv(PROCESSED / "val_semantic_health_deterioration.csv")
    assert float(val["health_deterioration_balance_mae"].iloc[0]) <= 1e-6
    assert int(val["health_out_of_range"].iloc[0]) == 0
    assert int(val["deterioration_out_of_range"].iloc[0]) == 0


def test_sign_consistency_health_deterioration_risk():
    score = pd.read_csv(PROCESSED / "scoring_componentes.csv")
    df = _merge_semantic_columns(score)

    rho_health_risk = float(df[["health_score", "prob_fallo_30d"]].corr(method="spearman").iloc[0, 1])
    rho_det_risk = float(df[["deterioration_index", "prob_fallo_30d"]].corr(method="spearman").iloc[0, 1])
    rho_rest_risk = float(df[["maintenance_restoration_index", "prob_fallo_30d"]].corr(method="spearman").iloc[0, 1])

    assert rho_health_risk <= -0.20
    assert rho_det_risk >= 0.20
    assert rho_rest_risk <= -0.04


def test_monotonicity_risk_by_deterioration_terciles():
    score = pd.read_csv(PROCESSED / "scoring_componentes.csv")
    df = _merge_semantic_columns(score)
    tiers = pd.qcut(df["deterioration_index"].fillna(df["deterioration_index"].median()), q=3, labels=["bajo", "medio", "alto"])
    risk_means = df.assign(det_tier=tiers).groupby("det_tier", observed=False)["prob_fallo_30d"].mean()
    assert float(risk_means.loc["alto"]) >= float(risk_means.loc["medio"]) >= float(risk_means.loc["bajo"])


def test_main_driver_not_collapsed():
    score = pd.read_csv(PROCESSED / "scoring_componentes.csv")
    dominant_share = float(score["main_risk_driver"].value_counts(normalize=True).max())
    assert dominant_share <= 0.90


def test_inspection_coverage_in_range():
    perf = pd.read_csv(PROCESSED / "inspection_module_family_performance.csv")
    assert perf["coverage_pre_falla"].between(0, 1).all()


def test_failure_risk_not_saturated():
    score = pd.read_csv(PROCESSED / "scoring_componentes.csv")
    share_high = float((score["prob_fallo_30d"] >= 0.90).mean())
    assert share_high <= 0.40


def test_alert_classes_not_collapsed():
    alertas = pd.read_csv(PROCESSED / "alertas_tempranas.csv")
    dist = alertas["nivel_alerta"].value_counts(normalize=True)
    share_high_critical = float(dist.get("alta", 0.0) + dist.get("critica", 0.0))
    assert share_high_critical <= 0.75


def test_recommended_action_not_collapsed():
    score = pd.read_csv(PROCESSED / "scoring_componentes.csv")
    dominant_share = float(score["recommended_action_initial"].value_counts(normalize=True).max())
    n_classes = int(score["recommended_action_initial"].nunique())
    assert dominant_share <= 0.60
    assert n_classes >= 5
