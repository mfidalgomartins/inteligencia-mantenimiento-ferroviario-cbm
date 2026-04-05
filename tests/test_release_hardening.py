from __future__ import annotations

from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
REPORTS = ROOT / "outputs" / "reports"


def test_release_hardening_artifacts_exist():
    expected = [
        REPORTS / "release_artifact_manifest.csv",
        REPORTS / "release_hardening_checks.csv",
        REPORTS / "release_hardening_report.md",
        REPORTS / "release_hardening_status.json",
    ]
    for path in expected:
        assert path.exists(), f"Falta artefacto de release hardening: {path}"


def test_release_hardening_checks_minimum_coverage():
    checks = pd.read_csv(REPORTS / "release_hardening_checks.csv")
    required_ids = {
        "release_single_dashboard_artifact",
        "release_dashboard_signature_consistency",
        "release_dashboard_offline_mode",
        "release_dashboard_decision_alignment_ssot",
        "release_readiness_not_blocked",
        "release_validation_has_results",
    }
    assert required_ids.issubset(set(checks["check_id"].astype(str)))
    assert checks["passed"].all(), "Release hardening contém checks falhados"


def test_release_manifest_hash_integrity():
    manifest = pd.read_csv(REPORTS / "release_artifact_manifest.csv")
    assert manifest["artifact_path"].is_unique
    existing = manifest[manifest["exists"] == True]  # noqa: E712
    assert not existing.empty
    assert (existing["size_bytes"] > 0).all()
    assert existing["sha256"].astype(str).str.len().eq(64).all()

