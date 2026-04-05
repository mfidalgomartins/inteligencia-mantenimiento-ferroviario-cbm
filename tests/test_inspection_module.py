from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"
TARGET_FAMILIES = {"wheel", "brake", "bogie", "pantograph"}


def _load_family_perf() -> pd.DataFrame:
    return pd.read_csv(PROCESSED / "inspection_module_family_performance.csv")


def _load_checks() -> pd.DataFrame:
    return pd.read_csv(PROCESSED / "inspection_module_consistency_checks.csv")


def _load_linkage() -> pd.DataFrame:
    return pd.read_csv(PROCESSED / "inspection_module_failure_linkage.csv")


def test_inspection_families_are_official_taxonomy():
    perf = _load_family_perf()
    assert set(perf["family"].unique()) == TARGET_FAMILIES


def test_inspection_official_rates_are_bounded():
    perf = _load_family_perf()
    rate_cols = [
        "inspection_coverage",
        "defect_detection_rate",
        "pre_failure_detection_rate",
        "false_alert_proxy",
        "confidence_adjusted_detection_value",
        "family_mapping_consistency_rate",
        "alert_chain_rate",
        "maintenance_followthrough_rate",
    ]
    for col in rate_cols:
        assert perf[col].between(0, 1).all(), f"{col} fuera de [0,1]"


def test_inspection_numerators_denominators_are_coherent():
    perf = _load_family_perf()
    assert (perf["inspected_components"] <= perf["monitored_components"]).all()
    assert (perf["detections"] <= perf["total_inspections"]).all()
    assert (perf["failures_with_pre_detection"] <= perf["total_failures"]).all()
    assert (perf["detections_with_future_failure"] <= perf["total_detections"]).all()


def test_inspection_temporal_linkage_is_non_negative():
    linkage = _load_linkage()
    if linkage.empty:
        return
    linkage["inspection_ts"] = pd.to_datetime(linkage["inspection_ts"], errors="coerce")
    linkage["fecha_falla"] = pd.to_datetime(linkage["fecha_falla"], errors="coerce")
    days = (linkage["fecha_falla"] - linkage["inspection_ts"]).dt.total_seconds() / 86400.0
    assert days.ge(0).all()
    assert days.le(30).all()


def test_inspection_consistency_checks_pass():
    checks = _load_checks()
    assert checks["result"].all(), f"checks fallidos: {checks.loc[~checks['result'], 'check'].tolist()}"
