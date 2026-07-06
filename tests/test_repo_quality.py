from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"


def test_runtime_requirements_cover_core_stack():
    req = (ROOT / "requirements.txt").read_text(encoding="utf-8").lower()
    for pkg in ["pandas", "numpy", "matplotlib", "pytest", "pytest-cov", "ruff", "duckdb", "tabulate", "weasyprint"]:
        assert pkg in req, f"Falta dependencia de ejecución en requirements.txt: {pkg}"


def test_lockfile_exists_and_has_core_stack():
    lock = ROOT / "requirements-lock.txt"
    assert lock.exists(), "Falta requirements-lock.txt"
    lock_text = lock.read_text(encoding="utf-8").lower()
    for pkg in [
        "pandas==",
        "numpy==",
        "matplotlib==",
        "pytest==",
        "pytest-cov==",
        "coverage==",
        "ruff==",
        "duckdb==",
        "tabulate==",
        "weasyprint==",
    ]:
        assert pkg in lock_text, f"Falta dependencia fijada en lockfile: {pkg}"


def test_governance_contract_publish_blockers_pass():
    checks_path = PROCESSED / "governance_contract_checks.csv"
    assert checks_path.exists(), "Falta artefacto de validaciones de contratos de gobernanza"
    checks = pd.read_csv(checks_path)
    blockers = checks[checks["publish_blocker"] == True]  # noqa: E712
    assert blockers["passed"].all(), f"Bloqueos de gobernanza presentes: {len(blockers[~blockers['passed']])}"


def test_repo_architecture_points_to_active_dashboard_artifact():
    doc = (ROOT / "docs" / "repo_architecture.md").read_text(encoding="utf-8")
    assert "outputs/dashboard/centro-control-mantenimiento-ferroviario.html" in doc


def test_public_text_files_do_not_contain_local_absolute_paths():
    paths = [
        ROOT / "README.md",
        *sorted((ROOT / "docs").glob("*.md")),
    ]
    for path in paths:
        text = path.read_text(encoding="utf-8")
        assert "/Users/" not in text, f"Ruta absoluta local encontrada en {path.relative_to(ROOT)}"


def test_pipeline_syncs_narrative_before_publication_gates():
    from src.run_pipeline import PIPELINE_STEPS

    labels = [label for label, _ in PIPELINE_STEPS]
    assert labels.index("Evaluar inspección automática") < labels.index("Comparar estrategias")
    sync_idx = labels.index("Sincronizar métricas y texto ejecutivo")
    assert sync_idx < labels.index("Validar contratos de gobernanza")
    assert sync_idx < labels.index("Construir panel de control")


def test_redundant_or_empty_publication_artifacts_are_absent():
    redundant_files = [
        ROOT / "docs" / "index.html",
        ROOT / "docs" / ".nojekyll",
        ROOT / "outputs" / "charts",
        ROOT / "outputs" / "reports" / "governance_contract_blockers.csv",
    ]
    assert not [path for path in redundant_files if path.exists()]
    assert "Graphs" not in {path.name for path in (ROOT / "outputs").iterdir()}


def test_readme_local_links_resolve():
    import re

    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    local_targets = [
        target.split("#", maxsplit=1)[0]
        for target in re.findall(r"!?\[[^\]]*\]\(([^)]+)\)", readme)
        if not target.startswith(("http://", "https://", "mailto:"))
    ]
    missing = [target for target in local_targets if target and not (ROOT / target).exists()]
    assert not missing, f"Enlaces locales rotos en README: {missing}"


def test_public_text_files_end_with_newline():
    paths = [ROOT / "README.md", ROOT / "index.html", *sorted((ROOT / "docs").glob("*.md"))]
    assert not [path for path in paths if not path.read_bytes().endswith(b"\n")]
