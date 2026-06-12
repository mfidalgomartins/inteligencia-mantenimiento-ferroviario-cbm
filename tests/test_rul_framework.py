from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"
DOCS = ROOT / "docs"


def test_rul_framework_artifacts_exist():
    expected = [
        PROCESSED / "component_rul_estimate.csv",
        PROCESSED / "rul_before_after_comparison.csv",
        PROCESSED / "rul_distribution_before_after.csv",
        PROCESSED / "rul_family_discrimination_before_after.csv",
        PROCESSED / "rul_backtest_failure_linkage.csv",
        PROCESSED / "rul_validation_checks.csv",
        PROCESSED / "rul_window_utility_before_after.csv",
        DOCS / "rul_framework.md",
    ]
    for path in expected:
        assert path.exists(), f"Falta artefacto RUL: {path}"


def test_rul_not_saturated_and_with_spread():
    rul = pd.read_csv(PROCESSED / "component_rul_estimate.csv")
    cap = float(rul["component_rul_estimate"].max())
    share_cap = float((rul["component_rul_estimate"] >= cap).mean())
    p10 = float(rul["component_rul_estimate"].quantile(0.10))
    p90 = float(rul["component_rul_estimate"].quantile(0.90))
    assert share_cap <= 0.60
    assert (p90 - p10) >= 55


def test_rul_family_discrimination_and_confidence_diversity():
    rul = pd.read_csv(PROCESSED / "component_rul_estimate.csv")
    fam = rul.groupby("component_family", as_index=False)["component_rul_estimate"].median()
    assert fam["component_family"].nunique() >= 4
    assert float(fam["component_rul_estimate"].max() - fam["component_rul_estimate"].min()) >= 12

    conf = rul["confidence_flag"].value_counts(normalize=True)
    assert conf.max() <= 0.95
    assert conf.shape[0] >= 2


def test_rul_failure_linkage_direction():
    backtest = pd.read_csv(PROCESSED / "rul_backtest_component_cutoff.csv")
    q25 = float(backtest["new_rul_days"].quantile(0.25))
    q75 = float(backtest["new_rul_days"].quantile(0.75))
    low = backtest.loc[backtest["new_rul_days"] <= q25, "failure_in_30d"]
    high = backtest.loc[backtest["new_rul_days"] >= q75, "failure_in_30d"]
    assert len(low) >= 500 and len(high) >= 500
    assert float(low.mean()) >= float(high.mean()) + 0.02


def test_rul_high_severity_validation_checks_pass():
    checks = pd.read_csv(PROCESSED / "rul_validation_checks.csv")
    failed = checks[(checks["severity"] == "alta") & (~checks["passed"].astype(bool))]
    assert failed.empty, failed[["check_id", "metric_value", "threshold"]].to_dict(orient="records")


def test_rul_integrated_with_workshop_priority():
    rul = pd.read_csv(PROCESSED / "component_rul_estimate.csv")
    prio = pd.read_csv(PROCESSED / "workshop_priority_table.csv")
    merged = prio.merge(
        rul[["unidad_id", "componente_id", "component_rul_estimate"]],
        on=["unidad_id", "componente_id"],
        how="inner",
        suffixes=("_prio", "_rul"),
    )
    assert len(merged) > 0
    diff = (merged["component_rul_estimate_prio"] - merged["component_rul_estimate_rul"]).abs().max()
    assert diff <= 1e-6
