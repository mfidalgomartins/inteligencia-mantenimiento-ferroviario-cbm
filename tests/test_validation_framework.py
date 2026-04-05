from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "outputs" / "reports"
DOCS = ROOT / "docs"


def test_validation_detailed_outputs_exist():
    expected = [
        REPORTS / "validation_checks_detailed.csv",
        REPORTS / "validation_control_matrix.csv",
        REPORTS / "publish_blockers.csv",
        REPORTS / "release_readiness.csv",
        REPORTS / "validation_report.md",
        DOCS / "validation_framework.md",
    ]
    for path in expected:
        assert path.exists(), f"Falta artefacto de QA: {path}"


def test_validation_detailed_schema_and_layers():
    df = pd.read_csv(REPORTS / "validation_checks_detailed.csv")
    required_cols = {
        "check_id",
        "layer",
        "severity",
        "passed",
        "blocker_if_fail",
        "publish_blocker",
        "what_checked",
        "detail",
        "impact_potential",
        "technical_recommendation",
    }
    assert required_cols.issubset(df.columns)
    expected_layers = {
        "raw_data",
        "staging",
        "marts",
        "features",
        "scores",
        "recommendations",
        "dashboard_datasets",
        "reports_docs_consistency",
    }
    assert expected_layers.issubset(set(df["layer"].unique()))
    assert set(df["severity"].unique()).issubset({"critica", "alta", "media", "informativa"})


def test_hardened_check_catalog_present():
    df = pd.read_csv(REPORTS / "validation_checks_detailed.csv")
    required_checks = {
        "scores_variability_minimum",
        "recommendation_high_deferral_weak_action",
        "framework_non_cosmetic_balance",
        "reports_governance_contract_compliance",
    }
    assert required_checks.issubset(set(df["check_id"]))

    blockers = df.set_index("check_id")["blocker_if_fail"].to_dict()
    assert blockers.get("reports_governance_contract_compliance") is True
    assert blockers.get("framework_non_cosmetic_balance") is True


def test_publish_blockers_subset_consistency():
    detailed = pd.read_csv(REPORTS / "validation_checks_detailed.csv")
    blockers = pd.read_csv(REPORTS / "publish_blockers.csv")
    expected = detailed[(~detailed["passed"]) & (detailed["publish_blocker"])]
    assert len(blockers) == len(expected)


def test_validation_report_has_blocker_section():
    report = (REPORTS / "validation_report.md").read_text(encoding="utf-8")
    assert "Qué Se Comprobó" in report
    assert "Qué Falló" in report
    assert "Lista de Checks que Bloquean Publicación" in report
    assert "Release Readiness" in report


def test_release_readiness_schema_and_values():
    df = pd.read_csv(REPORTS / "release_readiness.csv")
    assert {"dimension", "value", "rule"}.issubset(df.columns)
    expected = {
        "technically_valid",
        "analytically_acceptable",
        "decision-support only",
        "screening-grade only",
        "not committee-grade",
        "publish-blocked",
        "primary_release_state",
    }
    assert expected.issubset(set(df["dimension"]))
    state = str(df.loc[df["dimension"] == "primary_release_state", "value"].iloc[0])
    allowed = {
        "publish-blocked",
        "committee-grade",
        "decision-support only",
        "screening-grade only",
        "analytically acceptable",
        "technically valid",
        "not committee-grade",
    }
    assert state in allowed
