from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"


def test_runtime_requirements_cover_core_stack():
    req = (ROOT / "requirements.txt").read_text(encoding="utf-8").lower()
    for pkg in ["pandas", "numpy", "matplotlib", "pytest", "duckdb", "tabulate"]:
        assert pkg in req, f"Missing runtime dependency in requirements.txt: {pkg}"


def test_lockfile_exists_and_has_core_stack():
    lock = ROOT / "requirements-lock.txt"
    assert lock.exists(), "Missing requirements-lock.txt"
    lock_text = lock.read_text(encoding="utf-8").lower()
    for pkg in ["pandas==", "numpy==", "matplotlib==", "pytest==", "duckdb==", "tabulate=="]:
        assert pkg in lock_text, f"Missing pinned dependency in lockfile: {pkg}"


def test_governance_contract_publish_blockers_pass():
    checks_path = PROCESSED / "governance_contract_checks.csv"
    assert checks_path.exists(), "Missing governance contract checks artifact"
    checks = pd.read_csv(checks_path)
    blockers = checks[checks["publish_blocker"] == True]  # noqa: E712
    assert blockers["passed"].all(), f"Governance blockers present: {len(blockers[~blockers['passed']])}"


def test_repo_architecture_points_to_active_dashboard_artifact():
    doc = (ROOT / "docs" / "repo_architecture.md").read_text(encoding="utf-8")
    assert "outputs/dashboard/centro-control-mantenimiento-ferroviario.html" in doc
