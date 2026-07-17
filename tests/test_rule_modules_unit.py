from __future__ import annotations

import pandas as pd

from railway_cbm import early_warning, impact_analysis


def test_run_defer_impact_analysis_writes_monotonic_scenarios(tmp_path, monkeypatch):
    processed_dir = tmp_path / "processed"
    processed_dir.mkdir()
    monkeypatch.setattr(impact_analysis, "DATA_PROCESSED_DIR", processed_dir)

    priorities = pd.DataFrame(
        {
            "unidad_id": ["U1", "U2"],
            "componente_id": ["C1", "C2"],
            "deposito_recomendado": ["D1", "D1"],
            "intervention_priority_score": [80.0, 35.0],
            "deferral_risk_score": [90.0, 20.0],
            "service_impact_score": [70.0, 25.0],
            "prob_fallo_30d": [0.40, 0.10],
        }
    )
    priorities.to_csv(processed_dir / "workshop_priority_table.csv", index=False)

    detalle, resumen = impact_analysis.run_defer_impact_analysis()

    assert len(detalle) == 12
    assert set(resumen["defer_dias"]) == {0, 7, 14, 21, 30, 45}
    assert resumen["costo_total_eur"].is_monotonic_increasing
    assert resumen["downtime_total_h"].is_monotonic_increasing
    assert detalle["prob_fallo_ajustada"].between(0, 0.98).all()
    assert (processed_dir / "impacto_diferimiento_detalle.csv").exists()
    assert (processed_dir / "impacto_diferimiento_resumen.csv").exists()


def test_run_early_warning_rules_merges_confidence_and_writes_alerts(tmp_path, monkeypatch):
    monkeypatch.setattr(early_warning, "DATA_PROCESSED_DIR", tmp_path)
    scoring = pd.DataFrame(
        {
            "fecha": ["2026-01-01"] * 5,
            "unidad_id": ["U1", "U2", "U3", "U4", "U5"],
            "componente_id": ["C1", "C2", "C3", "C4", "C5"],
            "component_family": ["wheel"] * 5,
            "health_score": [96, 72, 48, 24, 35],
            "prob_fallo_30d": [0.05, 0.46, 0.68, 0.88, 0.70],
            "riesgo_ajustado_negocio": [5, 40, 65, 95, 70],
            "main_risk_driver": ["degradacion", "degradacion", "pendientes", "anomalias", "repetitividad"],
            "confidence_flag": ["alta", None, "media", None, "baja"],
        }
    )
    rul = pd.DataFrame(
        {
            "unidad_id": ["U1", "U2", "U3", "U4", "U5"],
            "componente_id": ["C1", "C2", "C3", "C4", "C5"],
            "component_rul_estimate": [220, 80, 35, 8, 12],
            "confidence_flag": ["alta", "media", "media", "baja", "alta"],
        }
    )
    scoring.to_csv(tmp_path / "scoring_componentes.csv", index=False)
    rul.to_csv(tmp_path / "component_rul_estimate.csv", index=False)

    out = early_warning.run_early_warning_rules().set_index("unidad_id")

    assert out.loc["U1", "nivel_alerta"] == "sin_alerta"
    assert out.loc["U1", "accion_recomendada"] == "monitorizar_intensivamente"
    assert out.loc["U4", "nivel_alerta"] == "critica"
    assert out.loc["U4", "accion_recomendada"] == "intervenir_ahora"
    assert out.loc["U2", "confidence_flag"] == "media"
    assert out.loc["U5", "confidence_flag"] == "baja"
    assert out["n_reglas_activas"].between(0, 4).all()
    assert (tmp_path / "alertas_tempranas.csv").exists()


def test_run_early_warning_rules_takes_confidence_from_rul_when_scoring_lacks_it(tmp_path, monkeypatch):
    # Sin colisión de columnas: la puntuación no trae confidence_flag, así que la unión
    # produce una única columna confidence_flag proveniente del RUL.
    monkeypatch.setattr(early_warning, "DATA_PROCESSED_DIR", tmp_path)
    scoring = pd.DataFrame(
        {
            "fecha": ["2026-01-01"] * 2,
            "unidad_id": ["U1", "U2"],
            "componente_id": ["C1", "C2"],
            "component_family": ["wheel"] * 2,
            "health_score": [90, 30],
            "prob_fallo_30d": [0.05, 0.85],
            "riesgo_ajustado_negocio": [5, 90],
            "main_risk_driver": ["degradacion", "anomalias"],
        }
    )
    rul = pd.DataFrame(
        {
            "unidad_id": ["U1", "U2"],
            "componente_id": ["C1", "C2"],
            "component_rul_estimate": [240, 10],
            "confidence_flag": ["alta", "baja"],
        }
    )
    scoring.to_csv(tmp_path / "scoring_componentes.csv", index=False)
    rul.to_csv(tmp_path / "component_rul_estimate.csv", index=False)

    out = early_warning.run_early_warning_rules().set_index("unidad_id")

    assert "confidence_flag_x" not in out.columns
    assert out.loc["U1", "confidence_flag"] == "alta"
    assert out.loc["U2", "confidence_flag"] == "baja"
