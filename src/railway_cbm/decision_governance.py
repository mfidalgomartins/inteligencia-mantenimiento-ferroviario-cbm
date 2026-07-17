"""Materializa decisiones en sombra y su trazabilidad de aprobación humana."""

from __future__ import annotations

import hashlib

import pandas as pd

from railway_cbm.config import DATA_PROCESSED_DIR, DATA_RAW_DIR, DOCS_DIR
from railway_cbm.ingestion import APPROVAL_COLUMNS, validate_approval_events

NO_ACTION_DECISION = "no acción por ahora"


def _decision_id(row: pd.Series) -> str:
    identity = "|".join(
        [
            str(row["fecha"]),
            str(row["unidad_id"]),
            str(row["componente_id"]),
            str(row["decision_rule_id"]),
        ]
    )
    return f"DEC-{hashlib.sha256(identity.encode('utf-8')).hexdigest()[:16].upper()}"


def _load_approvals(valid_decision_ids: set[str]) -> pd.DataFrame:
    path = DATA_RAW_DIR / "decision_approvals.csv"
    if not path.exists():
        return pd.DataFrame(columns=[*APPROVAL_COLUMNS, "record_origin"])
    approvals = validate_approval_events(pd.read_csv(path, dtype={"decision_id": str, "reviewer_id": str}))
    unknown = sorted(set(approvals["decision_id"]) - valid_decision_ids)
    if unknown:
        raise ValueError(f"El registro de aprobaciones referencia decisiones desconocidas: {unknown[:5]}")
    approvals["record_origin"] = "external_human_review"
    return approvals.sort_values(["decision_id", "reviewed_at"]).reset_index(drop=True)


def _governance_checks(register: pd.DataFrame, reviews: pd.DataFrame) -> pd.DataFrame:
    critical = register["approval_required"]
    approved = register["approval_status"] == "approved"
    checks = [
        (
            "decision_id_unique",
            not register["decision_id"].duplicated().any(),
            int(register["decision_id"].duplicated().sum()),
            "0 duplicate ids",
        ),
        (
            "shadow_mode_enforced",
            register["operating_mode"].eq("shadow").all(),
            int(register["operating_mode"].ne("shadow").sum()),
            "0 decisions outside shadow mode",
        ),
        (
            "automatic_execution_disabled",
            not register["auto_execution_allowed"].any(),
            int(register["auto_execution_allowed"].sum()),
            "0 automatically executable decisions",
        ),
        (
            "critical_decisions_require_approval",
            register.loc[critical, "approval_required"].all(),
            int(critical.sum()),
            "all material actions",
        ),
        (
            "approved_decisions_have_reviewer",
            register.loc[approved, "reviewer_id"].fillna("").str.strip().ne("").all(),
            int(register.loc[approved, "reviewer_id"].fillna("").str.strip().eq("").sum()),
            "0 approved decisions without reviewer",
        ),
        (
            "review_event_identity_unique",
            not reviews.duplicated(["decision_id", "reviewed_at"]).any(),
            int(reviews.duplicated(["decision_id", "reviewed_at"]).sum()),
            "0 duplicate review events",
        ),
    ]
    return pd.DataFrame(
        [
            {
                "check": name,
                "passed": bool(passed),
                "observed": observed,
                "expected": expected,
                "publish_blocker": True,
            }
            for name, passed, observed, expected in checks
        ]
    )


def _write_decision_doc(register: pd.DataFrame, checks: pd.DataFrame) -> None:
    status_counts = (
        register["approval_status"].value_counts().rename_axis("approval_status").reset_index(name="decisions")
    )
    lines = [
        "# Gobierno de decisiones y modo sombra",
        "",
        "El sistema produce recomendaciones trazables, pero no ejecuta órdenes de mantenimiento. Todas las decisiones "
        "se mantienen en modo sombra hasta superar la puerta de modelo y una transición operativa aprobada.",
        "",
        "## Identidad y revisión",
        "",
        "- `decision_id` es estable para fecha, unidad, componente y regla.",
        "- Las revisiones humanas se incorporan mediante `data/raw/decision_approvals.csv`.",
        "- Columnas exigidas: `decision_id`, `approval_status`, `reviewer_id`, `reviewed_at`, `comment`.",
        "- Estados permitidos: `approved`, `rejected`, `escalated`.",
        "- Una aprobación registrada no habilita ejecución automática mientras el modo sea `shadow`.",
        "",
        "## Estado actual",
        "",
        status_counts.to_markdown(index=False),
        "",
        "## Controles bloqueantes",
        "",
        checks.to_markdown(index=False),
    ]
    (DOCS_DIR / "decision_governance.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_decision_governance() -> pd.DataFrame:
    """Construye el registro de decisiones, revisiones y controles bloqueantes."""
    priorities = pd.read_csv(DATA_PROCESSED_DIR / "workshop_priority_table.csv")
    schedule = pd.read_csv(DATA_PROCESSED_DIR / "workshop_scheduling_recommendation.csv")
    deployment_gate = pd.read_csv(DATA_PROCESSED_DIR / "model_deployment_gate.csv")

    priorities["fecha"] = pd.to_datetime(priorities["fecha"], errors="raise").dt.date.astype(str)
    register = priorities[
        [
            "fecha",
            "unidad_id",
            "componente_id",
            "decision_type",
            "decision_rule_id",
            "decision_rationale",
            "bucket_prioridad",
            "recommended_entry_sequence",
            "intervention_priority_score",
            "deferral_risk_score",
            "service_impact_score",
            "prob_fallo_30d",
            "component_rul_estimate",
            "confidence_flag",
            "deposito_recomendado",
        ]
    ].copy()
    register["decision_id"] = register.apply(_decision_id, axis=1)
    register = register.merge(
        schedule[
            [
                "unidad_id",
                "componente_id",
                "ventana_temporal_sugerida",
                "estado_intervencion",
                "estado_detalle",
            ]
        ],
        on=["unidad_id", "componente_id"],
        how="left",
        validate="one_to_one",
    )
    register["approval_required"] = register["decision_type"].ne(NO_ACTION_DECISION)
    register["model_autonomous_use_allowed"] = bool(deployment_gate.iloc[0]["autonomous_use_allowed"])
    register["operating_mode"] = "shadow"
    register["auto_execution_allowed"] = False
    register["execution_authorized"] = False

    reviews = _load_approvals(set(register["decision_id"]))
    if reviews.empty:
        latest_reviews = pd.DataFrame(columns=[*APPROVAL_COLUMNS, "record_origin"])
    else:
        latest_reviews = reviews.sort_values("reviewed_at").groupby("decision_id", as_index=False).tail(1)
    register = register.merge(
        latest_reviews[[*APPROVAL_COLUMNS, "record_origin"]],
        on="decision_id",
        how="left",
        validate="one_to_one",
    )
    register["approval_status"] = register["approval_status"].fillna("pending")
    register["record_origin"] = register["record_origin"].fillna("system_pending_review")
    register = register.sort_values("recommended_entry_sequence").reset_index(drop=True)

    review_register = register[
        [
            "decision_id",
            "approval_required",
            "approval_status",
            "reviewer_id",
            "reviewed_at",
            "comment",
            "record_origin",
        ]
    ].copy()
    checks = _governance_checks(register, reviews)
    if not checks["passed"].all():
        failed = checks.loc[~checks["passed"], "check"].tolist()
        raise RuntimeError(f"Controles de gobierno de decisión fallidos: {failed}")

    register.to_csv(DATA_PROCESSED_DIR / "decision_register.csv", index=False)
    review_register.to_csv(DATA_PROCESSED_DIR / "decision_review_register.csv", index=False)
    checks.to_csv(DATA_PROCESSED_DIR / "decision_governance_checks.csv", index=False)
    _write_decision_doc(register, checks)
    return register
