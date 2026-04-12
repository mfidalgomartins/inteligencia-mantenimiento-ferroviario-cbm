from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"
DOCS = ROOT / "docs"


def test_strategy_framework_artifacts_exist():
    expected = [
        PROCESSED / "comparativo_estrategias.csv",
        PROCESSED / "comparativo_estrategias_sensibilidad.csv",
        PROCESSED / "comparativo_estrategias_escenarios.csv",
        PROCESSED / "comparativo_estrategias_value_ranges.csv",
        PROCESSED / "maintenance_strategy_observed_inputs.csv",
        PROCESSED / "maintenance_strategy_structural_assumptions.csv",
        PROCESSED / "maintenance_strategy_scenario_assumptions.csv",
        PROCESSED / "maintenance_strategy_sensitivity_definition.csv",
        DOCS / "maintenance_strategy_comparison_framework.md",
    ]
    for path in expected:
        assert path.exists(), f"Falta artefacto de estrategia: {path}"


def test_strategy_scenario_and_sensitivity_coverage():
    summary = pd.read_csv(PROCESSED / "comparativo_estrategias_escenarios.csv")
    sim = pd.read_csv(PROCESSED / "comparativo_estrategias_sensibilidad.csv")

    scenarios = set(summary["scenario_profile"].astype(str).unique())
    assert scenarios == {"conservador", "base", "agresivo"}
    assert summary.groupby("scenario_profile")["estrategia"].nunique().min() >= 3

    per_combo = sim.groupby(["scenario_profile", "sensitivity_id"])["estrategia"].nunique()
    assert (per_combo == sim["estrategia"].nunique()).all()
    assert len(sim) >= 3000


def test_strategy_value_semantics_and_ranges():
    base = pd.read_csv(PROCESSED / "comparativo_estrategias.csv")
    ranges = pd.read_csv(PROCESSED / "comparativo_estrategias_value_ranges.csv")

    react = base[base["estrategia"] == "reactiva"].iloc[0]
    cbm = base[base["estrategia"] == "basada_en_condicion"].iloc[0]
    expected_saving = float(react["coste_operativo_proxy"]) - float(cbm["coste_operativo_proxy"])
    assert abs(float(cbm["ahorro_neto_vs_reactiva"]) - expected_saving) <= 1e-6

    assert (ranges["coste_total_p10"] <= ranges["coste_total_p50"]).all()
    assert (ranges["coste_total_p50"] <= ranges["coste_total_p90"]).all()
    assert (ranges["ahorro_neto_p10_vs_reactiva"] <= ranges["ahorro_neto_p50_vs_reactiva"]).all()
    assert (ranges["ahorro_neto_p50_vs_reactiva"] <= ranges["ahorro_neto_p90_vs_reactiva"]).all()
    assert (ranges["downside_case"] <= ranges["ahorro_neto_p10_vs_reactiva"]).all()


def test_strategy_uncertainty_not_collapsed():
    sim = pd.read_csv(PROCESSED / "comparativo_estrategias_sensibilidad.csv")
    cbm = sim[sim["estrategia"] == "basada_en_condicion"]
    assert cbm["ahorro_neto_vs_reactiva"].std() > 100_000
