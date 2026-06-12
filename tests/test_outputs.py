from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def test_archivos_clave_existen():
    expected = [
        ROOT / "data" / "processed" / "scoring_componentes.csv",
        ROOT / "data" / "processed" / "rul_instancia.csv",
        ROOT / "data" / "processed" / "priorizacion_intervenciones.csv",
        ROOT / "data" / "processed" / "plan_taller_14d.csv",
        ROOT / "outputs" / "dashboard" / "centro-control-mantenimiento-ferroviario.html",
    ]
    for file in expected:
        assert file.exists(), f"No existe: {file}"


def test_graficos_publicados_existen_y_no_estan_vacios():
    graph_dir = ROOT / "outputs" / "graphs"
    expected = {
        "01_tendencia_disponibilidad.png",
        "02_valor_estrategias.png",
        "03_coste_diferimiento.png",
        "04_ranking_intervenciones.png",
        "05_saturacion_depositos.png",
        "06_distribucion_riesgo_unidades.png",
        "07_scheduling_antes_despues.png",
        "08_determinantes_riesgo.png",
        "09_salud_vs_riesgo.png",
        "10_riesgo_por_familia.png",
        "11_concentracion_backlog.png",
        "12_pareto_fallos_repetitivos.png",
        "13_cohortes_rul_familia.png",
        "14_rul_antes_despues.png",
        "15_inspeccion_por_familia.png",
        "16_utilizacion_capacidad.png",
        "17_ranking_indisponibilidad.png",
        "18_variancia_escenarios.png",
        "19_gobernanza_validaciones.png",
    }
    found = {path.name for path in graph_dir.glob("*.png")}
    assert found == expected
    assert all((graph_dir / name).stat().st_size > 20_000 for name in expected)


def test_rangos_scoring():
    df = pd.read_csv(ROOT / "data" / "processed" / "scoring_componentes.csv")
    assert df["health_score"].between(0, 100).all()
    assert df["prob_fallo_30d"].between(0, 1).all()
    assert df["riesgo_ajustado_negocio"].between(0, 100).all()


def test_rul_y_priorizacion():
    rul = pd.read_csv(ROOT / "data" / "processed" / "rul_instancia.csv")
    prio = pd.read_csv(ROOT / "data" / "processed" / "priorizacion_intervenciones.csv")
    assert (rul["rul_dias"] > 0).all()
    assert prio["indice_prioridad"].between(0, 100).all()


def test_backlog_snapshot_has_unique_order_identity():
    backlog = pd.read_csv(ROOT / "data" / "raw" / "backlog_mantenimiento.csv")
    assert backlog["backlog_id"].notna().all()
    assert not backlog.duplicated(["fecha", "backlog_id"]).any()


def test_synthetic_plausibility_checks_pass():
    checks = pd.read_csv(ROOT / "data" / "raw" / "validaciones_plausibilidad.csv")
    assert checks["aprobado"].astype(bool).all(), checks.loc[~checks["aprobado"].astype(bool)].to_dict(orient="records")


def test_sql_blocking_validations_pass():
    zero_outputs = [
        "val_null_rates_critical.csv",
        "val_sensor_ranges.csv",
        "val_temporal_coherence.csv",
        "val_backlog_semantic_consistency.csv",
    ]
    for name in zero_outputs:
        df = pd.read_csv(ROOT / "data" / "processed" / name)
        assert (df.select_dtypes(include="number") == 0).all().all(), f"Validación SQL fallida: {name}"


def test_mtbf_proxy_reconciles_to_available_hours_per_failure():
    unit_day = pd.read_csv(ROOT / "data" / "processed" / "mart_unit_day.csv")
    fleet_week = pd.read_csv(ROOT / "data" / "processed" / "mart_fleet_week.csv")
    unit_day["week_start"] = pd.to_datetime(unit_day["fecha"]).dt.to_period("W-SUN").dt.start_time
    expected = unit_day.groupby(["week_start", "flota_id"], as_index=False).agg(
        available_hours=("horas_disponibles", "sum"),
        failures_count=("failures_count", "sum"),
    )
    expected["expected_mtbf"] = expected["available_hours"] / expected["failures_count"].replace(0, pd.NA)
    fleet_week["week_start"] = pd.to_datetime(fleet_week["week_start"])
    merged = fleet_week.merge(expected, on=["week_start", "flota_id"], how="inner")
    with_failures = merged["failures_count"] > 0
    error = (merged.loc[with_failures, "mtbf_proxy"] - merged.loc[with_failures, "expected_mtbf"]).abs().max()
    assert float(error) <= 1e-6
