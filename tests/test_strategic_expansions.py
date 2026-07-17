from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from railway_cbm import cli, decision_governance, ingestion
from railway_cbm.capacity_optimization import (
    _is_acceptable_solution,
    _solve_capacity_allocation,
    _solver_status_label,
    assess_optimization_need,
)
from railway_cbm.decision_governance import _decision_id
from railway_cbm.feature_engineering import _aggregate_backlog_component_day
from railway_cbm.governance_contracts import _metric_contracts
from railway_cbm.ingestion import RawTableContract, _read_validated_csv
from railway_cbm.model_monitoring import _roc_auc
from railway_cbm.run_pipeline import build_pipeline_steps


def test_external_mode_requires_input_directory():
    with pytest.raises(ValueError, match="input_dir es obligatorio"):
        build_pipeline_steps(source_mode="external")


def test_external_snapshot_is_validated_before_generated_data_is_cleared(tmp_path: Path):
    labels = [label for label, _ in build_pipeline_steps(source_mode="external", input_dir=tmp_path)]
    assert labels.index("Validar snapshot externo") < labels.index("Limpiar datos generados")


def test_raw_contract_rejects_duplicate_primary_key(tmp_path: Path):
    source = tmp_path / "sample.csv"
    source.write_text("id,fecha,value\nA,2026-01-01,1\nA,2026-01-02,2\n", encoding="utf-8")
    contract = RawTableContract(("id", "fecha", "value"), ("id",))
    with pytest.raises(ValueError, match="claves primarias duplicadas"):
        _read_validated_csv(source, contract)


def test_raw_validation_loads_only_columns_needed_for_integrity(tmp_path: Path):
    source = tmp_path / "sample.csv"
    source.write_text("id,fecha,parent_id,payload\nA,2026-01-01,P1,large-value\n", encoding="utf-8")
    contract = RawTableContract(
        ("id", "fecha", "parent_id", "payload"),
        ("id",),
        (("parent_id", "parents", "parent_id"),),
    )
    validated = _read_validated_csv(source, contract)
    assert set(validated.columns) == {"id", "fecha", "parent_id"}


def test_raw_contract_rejects_missing_column_and_invalid_date(tmp_path: Path):
    contract = RawTableContract(("id", "fecha", "payload"), ("id",))
    missing = tmp_path / "missing.csv"
    missing.write_text("id,fecha\nA,2026-01-01\n", encoding="utf-8")
    with pytest.raises(ValueError, match="faltan columnas"):
        _read_validated_csv(missing, contract)

    invalid_date = tmp_path / "invalid_date.csv"
    invalid_date.write_text("id,fecha,payload\nA,not-a-date,value\n", encoding="utf-8")
    with pytest.raises(ValueError, match="valores temporales inválidos"):
        _read_validated_csv(invalid_date, contract)


def test_external_snapshot_validates_foreign_keys_and_copies_only_after_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    source = tmp_path / "source"
    managed_raw = tmp_path / "managed_raw"
    source.mkdir()
    contracts = {
        "parents": RawTableContract(("parent_id",), ("parent_id",)),
        "children": RawTableContract(
            ("child_id", "parent_id", "fecha"),
            ("child_id",),
            (("parent_id", "parents", "parent_id"),),
        ),
    }
    monkeypatch.setattr(ingestion, "RAW_TABLE_CONTRACTS", contracts)
    monkeypatch.setattr(ingestion, "DATA_RAW_DIR", managed_raw)
    (source / "parents.csv").write_text("parent_id\nP1\n", encoding="utf-8")
    (source / "children.csv").write_text("child_id,parent_id,fecha\nC1,P1,2026-01-01\n", encoding="utf-8")

    validated = ingestion.validate_external_snapshot(source)
    assert {name: len(frame) for name, frame in validated.items()} == {"parents": 1, "children": 1}
    ingestion.ingest_external_snapshot(source)
    assert sorted(path.name for path in managed_raw.glob("*.csv")) == ["children.csv", "parents.csv"]

    (source / "children.csv").write_text("child_id,parent_id,fecha\nC1,UNKNOWN,2026-01-01\n", encoding="utf-8")
    with pytest.raises(ValueError, match="sin referencia"):
        ingestion.validate_external_snapshot(source)


def test_input_manifest_records_content_hash_and_cardinality(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    raw = tmp_path / "raw"
    processed = tmp_path / "processed"
    raw.mkdir()
    processed.mkdir()
    (raw / "sample.csv").write_text("id,value\n1,A\n2,B\n", encoding="utf-8")
    monkeypatch.setattr(ingestion, "DATA_RAW_DIR", raw)
    monkeypatch.setattr(ingestion, "DATA_PROCESSED_DIR", processed)
    manifest = ingestion.write_input_manifest(source_mode="external", source_name="snapshot-1")
    assert manifest.iloc[0]["row_count"] == 2
    assert len(manifest.iloc[0]["sha256"]) == 64
    assert (processed / "input_data_manifest.csv").exists()


def test_backlog_is_aggregated_to_component_day_before_feature_join():
    backlog = pd.DataFrame(
        [
            {
                "fecha": "2026-01-01",
                "unidad_id": "U1",
                "componente_id": "C1",
                "antiguedad_backlog_dias": 10,
                "riesgo_acumulado": 30.0,
                "severidad_pendiente": "media",
            },
            {
                "fecha": "2026-01-01",
                "unidad_id": "U1",
                "componente_id": "C1",
                "antiguedad_backlog_dias": 40,
                "riesgo_acumulado": 80.0,
                "severidad_pendiente": "critica",
            },
        ]
    )
    aggregated = _aggregate_backlog_component_day(backlog)
    assert len(aggregated) == 1
    assert aggregated.iloc[0]["antiguedad_backlog_dias"] == 40
    assert aggregated.iloc[0]["riesgo_acumulado"] == 80.0
    assert aggregated.iloc[0]["severidad_pendiente"] == "critica"


def test_approval_contract_rejects_non_governed_status():
    from railway_cbm.ingestion import validate_approval_events

    approvals = pd.DataFrame(
        [
            {
                "decision_id": "DEC-1",
                "approval_status": "auto_approved",
                "reviewer_id": "reviewer-1",
                "reviewed_at": "2026-07-14T10:00:00Z",
                "comment": "",
            }
        ]
    )
    with pytest.raises(ValueError, match="Estados de aprobación no permitidos"):
        validate_approval_events(approvals)


def test_approval_contract_rejects_incomplete_or_duplicate_events():
    from railway_cbm.ingestion import validate_approval_events

    incomplete = pd.DataFrame(
        [
            {
                "decision_id": "DEC-1",
                "approval_status": "approved",
                "reviewer_id": "",
                "reviewed_at": "bad",
                "comment": "",
            }
        ]
    )
    with pytest.raises(ValueError, match="revisiones incompletas"):
        validate_approval_events(incomplete)

    duplicate = pd.DataFrame(
        [
            {
                "decision_id": "DEC-1",
                "approval_status": "approved",
                "reviewer_id": "R1",
                "reviewed_at": "2026-01-01",
                "comment": "",
            },
            {
                "decision_id": "DEC-1",
                "approval_status": "rejected",
                "reviewer_id": "R2",
                "reviewed_at": "2026-01-01",
                "comment": "",
            },
        ]
    )
    with pytest.raises(ValueError, match="eventos de revisión duplicados"):
        validate_approval_events(duplicate)


def test_capacity_gate_requires_persistent_saturation_and_pending_case():
    capacity = pd.DataFrame(
        {
            "total_used_h": [90.0, 92.0, 20.0, 10.0],
            "total_capacity_h": [100.0, 100.0, 100.0, 100.0],
        }
    )
    schedule = pd.DataFrame({"estado_intervencion": ["pendiente_capacidad", "programada"]})
    assessment = assess_optimization_need(capacity, schedule)
    assert assessment["formal_optimization_required"] is True

    schedule["estado_intervencion"] = "programada"
    assert assess_optimization_need(capacity, schedule)["formal_optimization_required"] is False


def test_optimizer_accepts_auditable_incumbent_within_gap_limit():
    feasible = SimpleNamespace(success=False, status=1, x=[1.0], mip_gap=0.06)
    weak = SimpleNamespace(success=False, status=1, x=[1.0], mip_gap=0.20)
    assert _is_acceptable_solution(feasible)
    assert not _is_acceptable_solution(weak)


def test_solver_status_does_not_overstate_tolerance_solution():
    assert _solver_status_label(status=0, mip_gap=0.03) == "optimal_within_tolerance"
    assert _solver_status_label(status=0, mip_gap=0.0) == "proven_optimal"
    assert _solver_status_label(status=1, mip_gap=0.06) == "feasible_within_limit"


def test_formal_optimizer_respects_weekly_capacity():
    priorities = pd.DataFrame(
        [
            {
                "unidad_id": "U1",
                "componente_id": "C1",
                "candidate_depots": "D1",
                "suggested_window_days": 2,
                "bucket_prioridad": "P1",
                "hours_required": 6.0,
                "queue_score": 90.0,
                "deferral_risk_score": 80.0,
                "deposito_id": "D1",
            },
            {
                "unidad_id": "U2",
                "componente_id": "C2",
                "candidate_depots": "D1",
                "suggested_window_days": 2,
                "bucket_prioridad": "P1",
                "hours_required": 6.0,
                "queue_score": 70.0,
                "deferral_risk_score": 60.0,
                "deposito_id": "D1",
            },
        ]
    )
    capacity = pd.DataFrame(
        {
            "deposito_id": ["D1"],
            "slot_date": ["2026-01-01"],
            "total_capacity_h": [10.0],
        }
    )
    allocation, utilization = _solve_capacity_allocation(priorities, capacity)
    assert (allocation["allocation_status"] == "allocated").sum() == 1
    assert (allocation["allocation_status"] == "unallocated").sum() == 1
    assert utilization["utilization"].max() <= 1.0


def test_perfect_ranking_has_unit_roc_auc():
    outcome = pd.Series([0, 0, 1, 1])
    score = pd.Series([0.1, 0.2, 0.8, 0.9])
    assert _roc_auc(outcome, score) == pytest.approx(1.0)


def test_decision_identity_is_stable_and_sensitive_to_rule():
    row = pd.Series(
        {
            "fecha": "2026-01-01",
            "unidad_id": "U1",
            "componente_id": "C1",
            "decision_rule_id": "D01",
        }
    )
    first = _decision_id(row)
    assert first == _decision_id(row.copy())
    changed = row.copy()
    changed["decision_rule_id"] = "D02"
    assert first != _decision_id(changed)


def test_human_review_loader_rejects_unknown_decision(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    approvals = pd.DataFrame(
        [
            {
                "decision_id": "DEC-UNKNOWN",
                "approval_status": "approved",
                "reviewer_id": "reviewer-1",
                "reviewed_at": "2026-07-14T10:00:00Z",
                "comment": "checked",
            }
        ]
    )
    approvals.to_csv(tmp_path / "decision_approvals.csv", index=False)
    monkeypatch.setattr(decision_governance, "DATA_RAW_DIR", tmp_path)
    with pytest.raises(ValueError, match="decisiones desconocidas"):
        decision_governance._load_approvals({"DEC-KNOWN"})


def test_cli_delegates_run_arguments(monkeypatch: pytest.MonkeyPatch):
    captured: dict[str, object] = {}

    def fake_run_pipeline(**kwargs: object) -> None:
        captured.update(kwargs)

    monkeypatch.setattr(cli, "run_pipeline", fake_run_pipeline)
    assert cli.main(["run", "--source", "synthetic", "--seed", "7"]) == 0
    assert captured == {"source_mode": "synthetic", "input_dir": None, "seed": 7}


def test_cli_validates_snapshot_without_copying(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    monkeypatch.setattr(cli, "validate_external_snapshot", lambda _: {"table": pd.DataFrame({"id": [1, 2]})})
    assert cli.main(["validate-input", "--input-dir", "/external/snapshot"]) == 0
    assert "1 tablas, 2 filas" in capsys.readouterr().out


def test_cli_rejects_incompatible_source_arguments():
    with pytest.raises(SystemExit):
        cli.main(["run", "--source", "external"])
    with pytest.raises(SystemExit):
        cli.main(["run", "--source", "synthetic", "--input-dir", "/external/snapshot"])


def test_strategic_model_metrics_have_governance_contracts():
    technical_names = {contract.technical_name for contract in _metric_contracts()}
    assert {"calibrated_probability_30d", "autonomous_use_allowed"}.issubset(technical_names)
