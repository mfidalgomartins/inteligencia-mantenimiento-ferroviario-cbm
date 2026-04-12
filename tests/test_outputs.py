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
