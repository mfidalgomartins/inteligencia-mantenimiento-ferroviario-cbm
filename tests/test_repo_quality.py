import tomllib
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"


def test_pep639_license_expression_does_not_use_legacy_classifier():
    metadata = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    assert metadata["license"] == "MIT"
    assert "License :: OSI Approved :: MIT License" not in metadata["classifiers"]


def test_runtime_requirements_cover_runtime_stack():
    req = (ROOT / "requirements.txt").read_text(encoding="utf-8").lower()
    for pkg in ["pandas", "numpy", "scipy", "matplotlib", "duckdb", "tabulate", "weasyprint"]:
        assert pkg in req, f"Falta dependencia de ejecución en requirements.txt: {pkg}"
    assert not [line for line in req.splitlines() if line.startswith(("pytest", "pytest-cov", "ruff", "pip-audit"))]


def test_pyproject_dependencies_match_runtime_requirements():
    metadata = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    project_dependencies = {dependency.split(">=", maxsplit=1)[0].lower() for dependency in metadata["dependencies"]}
    requirement_dependencies = {
        line.split(">=", maxsplit=1)[0].lower()
        for line in (ROOT / "requirements.txt").read_text(encoding="utf-8").splitlines()
        if line and not line.startswith(("#", "-r"))
    }
    assert project_dependencies == requirement_dependencies


def test_development_requirements_cover_quality_stack():
    req = (ROOT / "requirements-dev.txt").read_text(encoding="utf-8").lower()
    for pkg in ["pip-audit", "pytest", "pytest-cov", "ruff"]:
        assert pkg in req, f"Falta dependencia de desarrollo en requirements-dev.txt: {pkg}"


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
        "pip-audit==",
    ]:
        assert pkg in lock_text, f"Falta dependencia fijada en lockfile: {pkg}"


def test_governance_contract_publish_blockers_pass():
    checks_path = PROCESSED / "governance_contract_checks.csv"
    assert checks_path.exists(), "Falta artefacto de validaciones de contratos de gobernanza"
    checks = pd.read_csv(checks_path)
    blockers = checks[checks["publish_blocker"]]
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
    from railway_cbm.run_pipeline import PIPELINE_STEPS

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


def test_dependency_lock_covers_manifests_and_active_environment():
    from scripts.check_lock_drift import check_drift, declared_requirements, read_lock

    locked, structural_errors = read_lock()
    assert not structural_errors
    assert declared_requirements().issubset(locked)
    assert not check_drift()


def test_shell_entrypoints_respect_explicit_python_bin():
    for script_name in ["run_pipeline.sh", "run_tests.sh", "run_coverage.sh"]:
        script = (ROOT / "scripts" / script_name).read_text(encoding="utf-8")
        assert 'if [[ -z "${PYTHON_BIN:-}" ]]' in script


def test_generated_data_cleanup_removes_stale_artifacts_and_preserves_gitkeep(tmp_path):
    from railway_cbm.config import _clear_generated_directory

    generated_dir = tmp_path / "generated"
    nested_dir = generated_dir / "legacy"
    nested_dir.mkdir(parents=True)
    (generated_dir / ".gitkeep").write_text("", encoding="utf-8")
    (generated_dir / "stale.csv").write_text("legacy", encoding="utf-8")
    (nested_dir / "stale.db").write_text("legacy", encoding="utf-8")

    _clear_generated_directory(generated_dir)

    assert [path.name for path in generated_dir.iterdir()] == [".gitkeep"]


def test_ci_actions_are_pinned_to_commit_shas():
    import re

    workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    action_refs = re.findall(r"uses:\s+([^\s]+)", workflow)
    assert action_refs
    assert all(re.search(r"@[0-9a-f]{40}$", ref) for ref in action_refs)


def test_report_builder_enables_accessibility_and_metadata():
    script = (ROOT / "scripts" / "build_report_pdf.py").read_text(encoding="utf-8")
    for token in [
        'lang="es"',
        "<title>Informe analítico CBM ferroviario</title>",
        'name="author"',
        'name="description"',
        "pdf_tags=True",
    ]:
        assert token in script
