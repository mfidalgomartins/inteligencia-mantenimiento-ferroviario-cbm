from __future__ import annotations

from datetime import timedelta

import numpy as np
import pandas as pd

from src.config import DATA_PROCESSED_DIR, DATA_RAW_DIR, DOCS_DIR, OUTPUTS_REPORTS_DIR
from src.recommendation_engine import (
    assign_operational_decisions,
    build_recommendation_before_after_outputs,
    write_recommendation_logic_doc,
)


SCHED_STATUS_PROGRAMADA = "programada"
SCHED_STATUS_PROGRAMABLE = "programable_proxima_ventana"
SCHED_STATUS_PEND_CAP = "pendiente_capacidad"
SCHED_STATUS_PEND_REP = "pendiente_repuesto"
SCHED_STATUS_PEND_OPS = "pendiente_conflicto_operativo"
SCHED_STATUS_ESCALAR = "escalar_decision"

ACTIONABLE_STATUSES = {SCHED_STATUS_PROGRAMADA, SCHED_STATUS_PROGRAMABLE}
PENDING_STATUSES = {
    SCHED_STATUS_PEND_CAP,
    SCHED_STATUS_PEND_REP,
    SCHED_STATUS_PEND_OPS,
    SCHED_STATUS_ESCALAR,
}


def _normalize(s: pd.Series) -> pd.Series:
    s = s.astype(float)
    lo = s.min()
    hi = s.max()
    if (hi - lo) < 1e-9:
        return pd.Series(np.zeros(len(s)), index=s.index)
    return (s - lo) / (hi - lo)


def _rank_depots_for_component(row: pd.Series, depositos: pd.DataFrame, depot_pressure: pd.DataFrame) -> list[tuple[str, float]]:
    ranking: list[tuple[str, float]] = []
    for dep in depositos.itertuples(index=False):
        spec = str(dep.especializacion_tecnica).lower()
        spec_match = 1.0 if (row["subsistema"].lower() in spec or row["tipo_componente"].lower() in spec) else 0.42

        sat = float(
            depot_pressure.loc[depot_pressure["deposito_id"] == dep.deposito_id, "saturation_ratio"].mean()
            if (depot_pressure["deposito_id"] == dep.deposito_id).any()
            else 0.90
        )
        cap = float(dep.capacidad_taller)
        sat_factor = 1 - min(max(sat, 0), 1.7) / 1.9
        fit = spec_match * 0.62 + sat_factor * 0.24 + min(cap / 95, 1) * 0.14
        ranking.append((str(dep.deposito_id), float(np.clip(fit * 100, 0, 100))))

    ranking.sort(key=lambda x: x[1], reverse=True)
    return ranking


def _priority_bucket(row: pd.Series) -> str:
    if (
        row["decision_type"] == "intervención inmediata"
        or row["intervention_priority_score"] >= 88
        or row["deferral_risk_score"] >= 75
        or ((row["component_rul_estimate"] <= 12) and (row["prob_fallo_30d"] >= 0.75))
    ):
        return "P1"
    if (
        row["decision_type"] in {"intervención en próxima ventana", "escalado técnico/manual review"}
        or row["intervention_priority_score"] >= 74
        or row["deferral_risk_score"] >= 60
    ):
        return "P2"
    if (
        row["decision_type"] in {"inspección prioritaria", "monitorización intensiva"}
        or row["intervention_priority_score"] >= 55
    ):
        return "P3"
    return "P4"


def _carry_over_days(bucket: str) -> int:
    return {"P1": 14, "P2": 21, "P3": 14, "P4": 10}.get(bucket, 14)


def _build_capacity_calendar(
    *,
    depositos: pd.DataFrame,
    dep_sat_map: dict[str, float],
    horizon_days: int,
    start_day: pd.Timestamp,
) -> pd.DataFrame:
    rows: list[dict[str, float | int | str]] = []
    for dep in depositos.itertuples(index=False):
        dep_id = str(dep.deposito_id)
        cap = float(dep.capacidad_taller)
        sat = float(dep_sat_map.get(dep_id, 0.95))
        sat_adj = 0.88 if sat > 1.15 else (0.94 if sat > 1.0 else (1.05 if sat < 0.75 else 1.0))
        base_regular = max(3.0, cap * 0.38 * sat_adj)

        for day in range(1, horizon_days + 1):
            slot_date = start_day + timedelta(days=day)
            wd = slot_date.weekday()
            wd_factor = 1.0 if wd < 5 else (0.75 if wd == 5 else 0.60)
            regular_capacity = max(4.0, base_regular * wd_factor)
            flex_capacity = regular_capacity * (0.10 if day <= 14 else 0.05)
            rows.append(
                {
                    "deposito_id": dep_id,
                    "day_offset": day,
                    "slot_date": slot_date.date().isoformat(),
                    "regular_capacity_h": round(float(regular_capacity), 4),
                    "flex_capacity_h": round(float(flex_capacity), 4),
                }
            )
    return pd.DataFrame(rows)


def _schedule_legacy(
    *,
    priorities: pd.DataFrame,
    latest: pd.Timestamp,
    depositos: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    horizon_days = 21
    capacity = depositos.set_index("deposito_id")["capacidad_taller"].astype(float).to_dict()
    remaining = {(dep, d): cap * 0.78 for dep, cap in capacity.items() for d in range(1, horizon_days + 1)}
    plan_rows: list[dict[str, object]] = []

    queue = priorities.sort_values("recommended_entry_sequence", ascending=True).copy()
    for row in queue.itertuples(index=False):
        dep = str(row.deposito_recomendado)
        hrs = float(row.hours_required)
        assigned = False

        max_day = int(max(1, row.suggested_window_days))
        max_day = min(max_day, horizon_days)
        for d in range(1, max_day + 1):
            key = (dep, d)
            if key in remaining and remaining[key] >= hrs:
                remaining[key] -= hrs
                plan_rows.append(
                    {
                        "unidad_id": row.unidad_id,
                        "componente_id": row.componente_id,
                        "deposito_recomendado": dep,
                        "ventana_temporal_sugerida": (latest + timedelta(days=d)).date().isoformat(),
                        "prioridad": int(row.recommended_entry_sequence),
                        "decision_type": row.decision_type,
                        "intervention_priority_score": round(float(row.intervention_priority_score), 3),
                        "deferral_risk_score": round(float(row.deferral_risk_score), 3),
                        "service_impact_score": round(float(row.service_impact_score), 3),
                        "workshop_fit_score": round(float(row.workshop_fit_score), 3),
                        "horas_programadas": round(hrs, 3),
                        "estado_intervencion": SCHED_STATUS_PROGRAMADA,
                        "razon_principal": row.reason_main,
                        "bucket_prioridad": row.bucket_prioridad,
                        "queue_score": round(float(row.queue_score), 3),
                        "dias_fuera_ventana_preferida": 0,
                        "capacidad_slot_tipo": "regular",
                        "reasignacion_deposito_flag": 0,
                        "estado_detalle": "dentro_ventana_preferida",
                    }
                )
                assigned = True
                break

        if not assigned:
            plan_rows.append(
                {
                    "unidad_id": row.unidad_id,
                    "componente_id": row.componente_id,
                    "deposito_recomendado": dep,
                    "ventana_temporal_sugerida": None,
                    "prioridad": int(row.recommended_entry_sequence),
                    "decision_type": row.decision_type,
                    "intervention_priority_score": round(float(row.intervention_priority_score), 3),
                    "deferral_risk_score": round(float(row.deferral_risk_score), 3),
                    "service_impact_score": round(float(row.service_impact_score), 3),
                    "workshop_fit_score": round(float(row.workshop_fit_score), 3),
                    "horas_programadas": round(hrs, 3),
                    "estado_intervencion": SCHED_STATUS_PEND_CAP,
                    "razon_principal": row.reason_main,
                    "bucket_prioridad": row.bucket_prioridad,
                    "queue_score": round(float(row.queue_score), 3),
                    "dias_fuera_ventana_preferida": None,
                    "capacidad_slot_tipo": "none",
                    "reasignacion_deposito_flag": 0,
                    "estado_detalle": "sin_slot_hasta_ventana",
                }
            )

    scheduling = pd.DataFrame(plan_rows).sort_values("prioridad").reset_index(drop=True)
    ledger_rows = []
    for dep, cap in capacity.items():
        for d in range(1, horizon_days + 1):
            total = cap * 0.78
            rem = remaining[(dep, d)]
            ledger_rows.append(
                {
                    "deposito_id": dep,
                    "day_offset": d,
                    "slot_date": (latest + timedelta(days=d)).date().isoformat(),
                    "regular_capacity_h": total,
                    "flex_capacity_h": 0.0,
                    "regular_used_h": total - rem,
                    "flex_used_h": 0.0,
                    "total_capacity_h": total,
                    "total_used_h": total - rem,
                }
            )
    ledger = pd.DataFrame(ledger_rows)
    return scheduling, ledger


def _schedule_redesigned(
    *,
    priorities: pd.DataFrame,
    latest: pd.Timestamp,
    depositos: pd.DataFrame,
    dep_sat_map: dict[str, float],
    resources_index: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    horizon_days = 35
    cap_calendar = _build_capacity_calendar(
        depositos=depositos,
        dep_sat_map=dep_sat_map,
        horizon_days=horizon_days,
        start_day=latest,
    )

    regular_remaining = {(r.deposito_id, int(r.day_offset)): float(r.regular_capacity_h) for r in cap_calendar.itertuples(index=False)}
    flex_remaining = {(r.deposito_id, int(r.day_offset)): float(r.flex_capacity_h) for r in cap_calendar.itertuples(index=False)}
    day_units: dict[tuple[str, int], set[str]] = {}
    day_families: dict[tuple[str, int], set[str]] = {}

    queue = priorities.copy()
    queue["bucket_rank"] = queue["bucket_prioridad"].map({"P1": 1, "P2": 2, "P3": 3, "P4": 4}).fillna(3).astype(int)
    queue["queue_score"] = queue["queue_score"].fillna(0.0).astype(float)
    queue = queue.sort_values(["bucket_rank", "queue_score", "recommended_entry_sequence"], ascending=[True, False, True]).reset_index(drop=True)

    sensitive_types = {"pantograph_head", "gearbox_drive", "brake_disc_pad"}
    intervention_types = {"intervención inmediata", "intervención en próxima ventana"}
    plan_rows: list[dict[str, object]] = []

    for row in queue.itertuples(index=False):
        preferred_window = int(max(1, row.suggested_window_days))
        carry = _carry_over_days(str(row.bucket_prioridad))
        extended_window = min(horizon_days, preferred_window + carry)
        candidate_depots = [d for d in str(row.candidate_depots).split("|") if d]
        if str(row.deposito_id) not in candidate_depots:
            candidate_depots.append(str(row.deposito_id))
        if str(row.deposito_recomendado) not in candidate_depots:
            candidate_depots.insert(0, str(row.deposito_recomendado))
        candidate_depots = list(dict.fromkeys(candidate_depots))[:3]

        hrs_nominal = float(row.hours_required)
        assigned = False
        chosen_dep = str(row.deposito_recomendado)
        chosen_day = None
        slot_type = "none"
        assigned_status = None

        for stage, day_range in [
            ("preferred", range(1, min(preferred_window, horizon_days) + 1)),
            ("extended", range(min(preferred_window, horizon_days) + 1, extended_window + 1)),
        ]:
            if assigned:
                break
            for dep in candidate_depots:
                if assigned:
                    break
                for d in day_range:
                    key = (dep, d)
                    if key not in regular_remaining:
                        continue
                    booked_units = day_units.setdefault(key, set())
                    booked_families = day_families.setdefault(key, set())
                    setup_h = 1.10 if row.unidad_id not in booked_units else 0.35
                    family_bonus = -0.15 if str(row.component_family) in booked_families else 0.0
                    hrs_effective = max(1.8, hrs_nominal + setup_h + family_bonus)

                    use_flex_allowed = (stage == "extended") and (row.bucket_prioridad in {"P1", "P2"})
                    if regular_remaining[key] >= hrs_effective:
                        regular_remaining[key] -= hrs_effective
                        booked_units.add(str(row.unidad_id))
                        booked_families.add(str(row.component_family))
                        chosen_dep = dep
                        chosen_day = d
                        slot_type = "regular"
                        assigned_status = SCHED_STATUS_PROGRAMADA if stage == "preferred" else SCHED_STATUS_PROGRAMABLE
                        assigned = True
                        break
                    total_available = regular_remaining[key] + (flex_remaining[key] if use_flex_allowed else 0.0)
                    if use_flex_allowed and total_available >= hrs_effective:
                        from_regular = regular_remaining[key]
                        from_flex = hrs_effective - from_regular
                        regular_remaining[key] = 0.0
                        flex_remaining[key] -= from_flex
                        booked_units.add(str(row.unidad_id))
                        booked_families.add(str(row.component_family))
                        chosen_dep = dep
                        chosen_day = d
                        slot_type = "flex"
                        assigned_status = SCHED_STATUS_PROGRAMABLE
                        assigned = True
                        break

        if assigned and chosen_day is not None:
            days_out = max(0, chosen_day - preferred_window)
            plan_rows.append(
                {
                    "unidad_id": row.unidad_id,
                    "componente_id": row.componente_id,
                    "deposito_recomendado": chosen_dep,
                    "ventana_temporal_sugerida": (latest + timedelta(days=int(chosen_day))).date().isoformat(),
                    "prioridad": int(row.recommended_entry_sequence),
                    "decision_type": row.decision_type,
                    "intervention_priority_score": round(float(row.intervention_priority_score), 3),
                    "deferral_risk_score": round(float(row.deferral_risk_score), 3),
                    "service_impact_score": round(float(row.service_impact_score), 3),
                    "workshop_fit_score": round(float(row.workshop_fit_score), 3),
                    "horas_programadas": round(float(hrs_nominal), 3),
                    "estado_intervencion": assigned_status,
                    "razon_principal": row.reason_main,
                    "bucket_prioridad": row.bucket_prioridad,
                    "queue_score": round(float(row.queue_score), 3),
                    "dias_fuera_ventana_preferida": int(days_out),
                    "capacidad_slot_tipo": slot_type,
                    "reasignacion_deposito_flag": int(chosen_dep != str(row.deposito_recomendado)),
                    "estado_detalle": "dentro_ventana_preferida" if days_out == 0 else "carry_over_controlado",
                }
            )
            continue

        spare_risk = (
            (resources_index < 0.50)
            and (str(row.tipo_componente) in sensitive_types)
            and (float(row.intervention_priority_score) >= 62)
        )
        operational_conflict = (
            (float(row.ventana_operativa_disponible) <= 0)
            and (str(row.decision_type) in intervention_types)
            and (str(row.bucket_prioridad) in {"P1", "P2"})
            and (float(row.service_impact_score) >= 82)
            and (float(row.criticidad_servicio) >= 0.70)
        )
        escalation_required = (
            (str(row.decision_type) == "escalado técnico/manual review")
            or (int(row.decision_conflict_flag) == 1)
            or ((str(row.bucket_prioridad) == "P1") and (str(row.confidence_flag) == "baja"))
        )

        if escalation_required:
            unresolved_status = SCHED_STATUS_ESCALAR
            detail = "riesgo_alto_baja_confianza_o_conflicto"
        elif operational_conflict:
            unresolved_status = SCHED_STATUS_PEND_OPS
            detail = "sin_ventana_operativa_factible"
        elif spare_risk:
            unresolved_status = SCHED_STATUS_PEND_REP
            detail = "riesgo_repuesto_bajo_recursos"
        else:
            unresolved_status = SCHED_STATUS_PEND_CAP
            detail = "capacidad_insuficiente_en_horizonte"

        plan_rows.append(
            {
                "unidad_id": row.unidad_id,
                "componente_id": row.componente_id,
                "deposito_recomendado": str(row.deposito_recomendado),
                "ventana_temporal_sugerida": None,
                "prioridad": int(row.recommended_entry_sequence),
                "decision_type": row.decision_type,
                "intervention_priority_score": round(float(row.intervention_priority_score), 3),
                "deferral_risk_score": round(float(row.deferral_risk_score), 3),
                "service_impact_score": round(float(row.service_impact_score), 3),
                "workshop_fit_score": round(float(row.workshop_fit_score), 3),
                "horas_programadas": round(float(hrs_nominal), 3),
                "estado_intervencion": unresolved_status,
                "razon_principal": row.reason_main,
                "bucket_prioridad": row.bucket_prioridad,
                "queue_score": round(float(row.queue_score), 3),
                "dias_fuera_ventana_preferida": None,
                "capacidad_slot_tipo": "none",
                "reasignacion_deposito_flag": 0,
                "estado_detalle": detail,
            }
        )

    scheduling = pd.DataFrame(plan_rows).sort_values("prioridad").reset_index(drop=True)

    cap_ledger = cap_calendar.copy()
    cap_ledger["regular_remaining_h"] = cap_ledger.apply(
        lambda r: regular_remaining[(r["deposito_id"], int(r["day_offset"]))], axis=1
    )
    cap_ledger["flex_remaining_h"] = cap_ledger.apply(
        lambda r: flex_remaining[(r["deposito_id"], int(r["day_offset"]))], axis=1
    )
    cap_ledger["regular_used_h"] = cap_ledger["regular_capacity_h"] - cap_ledger["regular_remaining_h"]
    cap_ledger["flex_used_h"] = cap_ledger["flex_capacity_h"] - cap_ledger["flex_remaining_h"]
    cap_ledger["total_capacity_h"] = cap_ledger["regular_capacity_h"] + cap_ledger["flex_capacity_h"]
    cap_ledger["total_used_h"] = cap_ledger["regular_used_h"] + cap_ledger["flex_used_h"]
    return scheduling, cap_ledger


def _compute_schedule_metrics(
    *,
    schedule: pd.DataFrame,
    priorities: pd.DataFrame,
    capacity_ledger: pd.DataFrame,
    label: str,
) -> dict[str, float | str]:
    total = max(len(schedule), 1)
    actionable_mask = schedule["estado_intervencion"].isin(ACTIONABLE_STATUSES)
    pending_mask = schedule["estado_intervencion"].isin(PENDING_STATUSES)

    total_capacity = float(capacity_ledger["total_capacity_h"].sum()) if not capacity_ledger.empty else 0.0
    total_used = float(capacity_ledger["total_used_h"].sum()) if not capacity_ledger.empty else 0.0
    utilization = total_used / total_capacity if total_capacity > 0 else 0.0

    plan_cols = ["unidad_id", "componente_id", "estado_intervencion"]
    merged = priorities.merge(schedule[plan_cols], on=["unidad_id", "componente_id"], how="left")
    unresolved = merged[~merged["estado_intervencion"].isin(ACTIONABLE_STATUSES)].copy()

    risk_weight = 0.6 * merged["deferral_risk_score"].fillna(0) + 0.4 * merged["service_impact_score"].fillna(0)
    unresolved_weight = 0.6 * unresolved["deferral_risk_score"].fillna(0) + 0.4 * unresolved["service_impact_score"].fillna(0)
    residual_risk_pct = float(unresolved_weight.sum() / max(risk_weight.sum(), 1e-6) * 100)

    capture_factor = {
        SCHED_STATUS_PROGRAMADA: 1.0,
        SCHED_STATUS_PROGRAMABLE: 0.65,
        SCHED_STATUS_PEND_REP: 0.28,
        SCHED_STATUS_PEND_CAP: 0.16,
        SCHED_STATUS_PEND_OPS: 0.22,
        SCHED_STATUS_ESCALAR: 0.12,
    }
    merged["capture_factor"] = merged["estado_intervencion"].map(capture_factor).fillna(0.0)
    total_value = float(merged["coste_retraso_proxy"].fillna(0).sum())
    captured_value = float((merged["coste_retraso_proxy"].fillna(0) * merged["capture_factor"]).sum())
    non_captured_value = total_value - captured_value

    return {
        "scenario": label,
        "total_casos": int(total),
        "programadas_pct": round(float((schedule["estado_intervencion"] == SCHED_STATUS_PROGRAMADA).mean() * 100), 3),
        "programables_proxima_ventana_pct": round(float((schedule["estado_intervencion"] == SCHED_STATUS_PROGRAMABLE).mean() * 100), 3),
        "pendientes_total_pct": round(float(pending_mask.mean() * 100), 3),
        "pendiente_capacidad_pct": round(float((schedule["estado_intervencion"] == SCHED_STATUS_PEND_CAP).mean() * 100), 3),
        "pendiente_repuesto_pct": round(float((schedule["estado_intervencion"] == SCHED_STATUS_PEND_REP).mean() * 100), 3),
        "pendiente_conflicto_operativo_pct": round(float((schedule["estado_intervencion"] == SCHED_STATUS_PEND_OPS).mean() * 100), 3),
        "escalar_decision_pct": round(float((schedule["estado_intervencion"] == SCHED_STATUS_ESCALAR).mean() * 100), 3),
        "actionable_pct": round(float(actionable_mask.mean() * 100), 3),
        "capacidad_utilizada_pct": round(float(utilization * 100), 3),
        "horas_taller_usadas": round(float(total_used), 3),
        "riesgo_residual_no_atendido_pct": round(float(residual_risk_pct), 3),
        "valor_capturado_proxy": round(float(captured_value), 3),
        "valor_no_capturado_proxy": round(float(non_captured_value), 3),
    }


def _write_scheduling_framework_doc(
    *,
    before_after: pd.DataFrame,
    bottlenecks: pd.DataFrame,
    statuses_after: pd.DataFrame,
) -> None:
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Scheduling Framework",
        "",
        "## Objetivo",
        "Reducir salida no ejecutable del plan de taller sin forzar programaciones irreales.",
        "",
        "## Diagnóstico del colapso original",
        "- Horizonte corto (21 días) con ventanas estrictas por caso.",
        "- Concentración de carga en pocos depósitos especializados.",
        "- Sin carry-over controlado ni capacidad flexible explícita.",
        "- Un único estado de no ejecución (`pendiente_capacidad`) sin distinción de causa.",
        "",
        "## Rediseño heurístico aplicado",
        "1. Horizonte multiperiodo: 35 días.",
        "2. Calendario de capacidad por depósito/día: capacidad regular + bolsa flexible.",
        "3. Bucketización por criticidad (`P1..P4`) y cola priorizada con aging.",
        "4. Carry-over controlado por bucket para programar fuera de ventana preferida cuando es viable.",
        "5. Candidatos de depósito (top-N por fit técnico-operativo) para aliviar cuellos.",
        "6. Nuevos estados operativos de salida para separar causa de no ejecución.",
        "",
        "## Estados de salida",
        "- `programada`: asignada dentro de ventana preferida.",
        "- `programable_proxima_ventana`: asignada fuera de ventana preferida, dentro del horizonte extendido.",
        "- `pendiente_repuesto`: no programable por riesgo de suministro de repuesto (proxy).",
        "- `pendiente_capacidad`: no programable por falta de capacidad en horizonte.",
        "- `pendiente_conflicto_operativo`: no programable por ventana operativa/conflicto de servicio.",
        "- `escalar_decision`: requiere revisión técnica/manual (alto riesgo + conflicto/información insuficiente).",
        "",
        "## Métricas Before/After",
        before_after.to_markdown(index=False),
        "",
        "## Cuellos de botella principales (baseline)",
        bottlenecks.to_markdown(index=False),
        "",
        "## Distribución de estados (after)",
        statuses_after.to_markdown(index=False),
        "",
        "## Trade-offs introducidos",
        "- Mayor capacidad de ejecución mediante carry-over y flexibilidad controlada.",
        "- Posible reasignación de depósito con coste logístico implícito (no modelado en detalle).",
        "- Mayor accionabilidad a cambio de complejidad heurística moderada.",
        "",
        "## Limitaciones del enfoque heurístico",
        "- No garantiza optimalidad global multiobjetivo.",
        "- Modela repuestos y conflicto operativo con proxies, no con ERP real.",
        "- No incorpora secuenciación fina de recursos técnicos por skill-hora.",
        "",
        "## Cuándo usar optimización formal",
        "- Si la red opera con saturación estructural persistente >85%.",
        "- Si hay restricciones duras de SLA/seguridad en múltiples depósitos simultáneos.",
        "- Si se necesita minimización explícita de coste+risk con constraints de recursos/repuestos.",
        "- Si se requiere plan robusto multi-semana con replanificación automática.",
    ]
    (DOCS_DIR / "scheduling_framework.md").write_text("\n".join(lines), encoding="utf-8")


def run_workshop_prioritization() -> tuple[pd.DataFrame, pd.DataFrame]:
    features = pd.read_csv(DATA_PROCESSED_DIR / "workshop_priority_features.csv")
    scoring = pd.read_csv(DATA_PROCESSED_DIR / "scoring_componentes.csv")
    rul = pd.read_csv(DATA_PROCESSED_DIR / "component_rul_estimate.csv")
    alertas = pd.read_csv(DATA_PROCESSED_DIR / "alertas_tempranas.csv")
    depositos = pd.read_csv(DATA_RAW_DIR / "depositos.csv")
    unidades = pd.read_csv(DATA_RAW_DIR / "unidades.csv")
    escenarios = pd.read_csv(DATA_RAW_DIR / "escenarios_mantenimiento.csv")
    depot_pressure = pd.read_csv(DATA_PROCESSED_DIR / "vw_depot_maintenance_pressure.csv")

    features["fecha"] = pd.to_datetime(features["fecha"], errors="coerce")
    features["fecha_programada"] = pd.to_datetime(features["fecha_programada"], errors="coerce")
    depot_pressure["fecha"] = pd.to_datetime(depot_pressure["fecha"], errors="coerce")
    escenarios["fecha"] = pd.to_datetime(escenarios["fecha"], errors="coerce")

    latest = features["fecha"].max()
    base = features[features["fecha"] == latest].copy()

    base = base.merge(
        scoring[
            [
                "unidad_id",
                "componente_id",
                "component_family",
                "health_score",
                "prob_fallo_30d",
                "riesgo_ajustado_negocio",
                "recommended_action_initial",
                "main_risk_driver",
                "confidence_flag",
            ]
        ],
        on=["unidad_id", "componente_id"],
        how="left",
    )
    base = base.merge(
        rul[["unidad_id", "componente_id", "component_rul_estimate", "confidence_rul"]],
        on=["unidad_id", "componente_id"],
        how="left",
    )
    base = base.merge(
        alertas[["unidad_id", "componente_id", "nivel_alerta", "n_reglas_activas"]],
        on=["unidad_id", "componente_id"],
        how="left",
    )

    # Scores obligatorios
    base["intervention_priority_score"] = (
        base["technical_risk_inputs"].fillna(0) * 0.45
        + base["service_impact_inputs"].fillna(0) * 0.30
        + base["urgency_inputs"].fillna(0) * 0.15
        + (100 - base["workshop_efficiency_inputs"].fillna(50)) * 0.10
    ).clip(0, 100)

    base["deferral_risk_score"] = (
        base["deferral_risk_inputs"].fillna(0) * 0.50
        + base["prob_fallo_30d"].fillna(0) * 100 * 0.30
        + _normalize(365 - base["component_rul_estimate"].fillna(365)) * 100 * 0.20
    ).clip(0, 100)

    base["service_impact_score"] = (
        base["service_impact_inputs"].fillna(0) * 0.70
        + base["predicted_unavailability_risk"].fillna(0) * 100 * 0.30
    ).clip(0, 100)

    # Mejor depósito recomendado por especialización + carga
    dep_latest = depot_pressure[depot_pressure["fecha"] == depot_pressure["fecha"].max()].copy()
    dep_sat_map = dep_latest.groupby("deposito_id")["saturation_ratio"].mean().to_dict()
    dep_sat = dep_latest.groupby("deposito_id", as_index=False)["saturation_ratio"].mean()
    base = base.merge(dep_sat, on="deposito_id", how="left")
    base = base.merge(
        unidades[["unidad_id", "criticidad_servicio", "disponibilidad_objetivo"]],
        on="unidad_id",
        how="left",
    )

    depot_rankings = base.apply(lambda row: _rank_depots_for_component(row, depositos, dep_latest), axis=1).tolist()
    base["deposito_recomendado"] = [r[0][0] for r in depot_rankings]
    base["workshop_fit_score"] = [r[0][1] for r in depot_rankings]
    base["candidate_depots"] = ["|".join([x[0] for x in rank[:4]]) for rank in depot_rankings]

    base["coste_retraso_proxy"] = (
        base["deferral_risk_score"] * 180
        + base["service_impact_score"] * 95
        + (100 - base["workshop_fit_score"]) * 40
    )

    base["recommended_entry_sequence"] = (
        base["intervention_priority_score"] * 0.50
        + base["deferral_risk_score"] * 0.30
        + base["service_impact_score"] * 0.20
    ).rank(method="first", ascending=False).astype(int)

    # Motor jerárquico de decisión operativa (no colapsado).
    base = assign_operational_decisions(base)

    # Ventana sugerida
    base["suggested_window_days"] = 21
    base.loc[base["decision_type"] == "intervención inmediata", "suggested_window_days"] = 2
    base.loc[base["decision_type"] == "intervención en próxima ventana", "suggested_window_days"] = 7
    base.loc[base["decision_type"] == "inspección prioritaria", "suggested_window_days"] = 3
    base.loc[base["decision_type"] == "monitorización intensiva", "suggested_window_days"] = 10
    base.loc[base["decision_type"] == "mantener bajo observación", "suggested_window_days"] = 14
    base.loc[base["decision_type"] == "no acción por ahora", "suggested_window_days"] = 21
    base.loc[base["decision_type"] == "escalado técnico/manual review", "suggested_window_days"] = 1

    base["reason_main"] = base["decision_rule_id"].fillna("D04_monitorizacion") + " | " + base["main_risk_driver"].fillna("degradacion")
    base["bucket_prioridad"] = base.apply(_priority_bucket, axis=1)
    base["aging_score"] = (
        ((22 - base["suggested_window_days"].fillna(21)).clip(1, 21) / 21) * 100
        + base["deferral_risk_score"].fillna(0) * 0.25
    ).clip(0, 100)
    base["queue_score"] = (
        base["intervention_priority_score"].fillna(0) * 0.58
        + base["deferral_risk_score"].fillna(0) * 0.22
        + base["service_impact_score"].fillna(0) * 0.12
        + base["aging_score"].fillna(0) * 0.08
    )

    # Horas requeridas por intervención (proxy)
    base["hours_required"] = (
        2.0
        + base["intervention_priority_score"] / 25
        + np.where(base["decision_type"] == "intervención inmediata", 2.8, 0)
        + np.where(base["decision_type"] == "intervención en próxima ventana", 1.8, 0)
        + np.where(base["decision_type"] == "inspección prioritaria", 1.0, 0)
        + np.where(base["decision_type"] == "escalado técnico/manual review", 0.6, 0)
    ).clip(2, 14)

    priority_cols = [
        "fecha",
        "unidad_id",
        "componente_id",
        "component_family",
        "subsistema",
        "tipo_componente",
        "linea_servicio",
        "deposito_id",
        "deposito_recomendado",
        "candidate_depots",
        "intervention_priority_score",
        "deferral_risk_score",
        "service_impact_score",
        "workshop_fit_score",
        "recommended_entry_sequence",
        "decision_type",
        "decision_rule_id",
        "decision_conflict_flag",
        "decision_rationale",
        "suggested_window_days",
        "reason_main",
        "health_score",
        "prob_fallo_30d",
        "component_rul_estimate",
        "confidence_flag",
        "ventana_operativa_disponible",
        "criticidad_servicio",
        "bucket_prioridad",
        "aging_score",
        "queue_score",
        "hours_required",
        "coste_retraso_proxy",
    ]
    priorities = base[priority_cols].sort_values("recommended_entry_sequence").reset_index(drop=True)

    latest_scen = escenarios[escenarios["fecha"] == escenarios["fecha"].max()].copy()
    resources_index = float(latest_scen["disponibilidad_recursos_indice"].mean()) if not latest_scen.empty else 0.55

    # Baseline (before) para auditoría de mejora.
    schedule_before, cap_before = _schedule_legacy(priorities=priorities, latest=pd.to_datetime(latest), depositos=depositos)
    metrics_before = _compute_schedule_metrics(
        schedule=schedule_before,
        priorities=priorities,
        capacity_ledger=cap_before,
        label="baseline_greedy_21d",
    )

    # Scheduling rediseñado (after).
    scheduling, cap_after = _schedule_redesigned(
        priorities=priorities,
        latest=pd.to_datetime(latest),
        depositos=depositos,
        dep_sat_map={str(k): float(v) for k, v in dep_sat_map.items()},
        resources_index=resources_index,
    )
    metrics_after = _compute_schedule_metrics(
        schedule=scheduling,
        priorities=priorities,
        capacity_ledger=cap_after,
        label="heuristica_redisenada_35d",
    )

    # Salidas principales
    priorities.to_csv(DATA_PROCESSED_DIR / "workshop_priority_table.csv", index=False)
    scheduling.to_csv(DATA_PROCESSED_DIR / "workshop_scheduling_recommendation.csv", index=False)

    cap_after.to_csv(DATA_PROCESSED_DIR / "workshop_capacity_calendar.csv", index=False)

    status_before = (
        schedule_before["estado_intervencion"].value_counts(normalize=True).rename("share").reset_index().rename(columns={"index": "estado_intervencion"})
    )
    status_before["scenario"] = "baseline_greedy_21d"
    status_after = (
        scheduling["estado_intervencion"].value_counts(normalize=True).rename("share").reset_index().rename(columns={"index": "estado_intervencion"})
    )
    status_after["scenario"] = "heuristica_redisenada_35d"
    status_dist = pd.concat([status_before, status_after], ignore_index=True)
    status_dist["share_pct"] = (status_dist["share"] * 100).round(3)
    status_dist.to_csv(DATA_PROCESSED_DIR / "scheduling_status_distribution.csv", index=False)

    before_after = pd.DataFrame([metrics_before, metrics_after])
    before_after.to_csv(DATA_PROCESSED_DIR / "scheduling_before_after_metrics.csv", index=False)

    metric_cols = [
        c
        for c in before_after.columns
        if c not in {"scenario"}
    ]
    baseline_row = before_after[before_after["scenario"] == "baseline_greedy_21d"].iloc[0]
    redesign_row = before_after[before_after["scenario"] == "heuristica_redisenada_35d"].iloc[0]
    before_after_delta = pd.DataFrame(
        {
            "metric": metric_cols,
            "baseline_greedy_21d": [baseline_row[c] for c in metric_cols],
            "heuristica_redisenada_35d": [redesign_row[c] for c in metric_cols],
            "delta_after_minus_before": [float(redesign_row[c]) - float(baseline_row[c]) for c in metric_cols],
        }
    )
    before_after_delta.to_csv(DATA_PROCESSED_DIR / "scheduling_before_after_deltas.csv", index=False)

    bottleneck = (
        schedule_before.groupby("deposito_recomendado", as_index=False)
        .agg(
            casos=("unidad_id", "count"),
            horas_requeridas=("horas_programadas", "sum"),
            pendientes_capacidad=("estado_intervencion", lambda s: int((s == SCHED_STATUS_PEND_CAP).sum())),
        )
        .rename(columns={"deposito_recomendado": "deposito_id"})
    )
    bottleneck["pending_rate_pct"] = (bottleneck["pendientes_capacidad"] / bottleneck["casos"] * 100).round(2)
    bottleneck = bottleneck.sort_values(["pendientes_capacidad", "horas_requeridas"], ascending=[False, False]).reset_index(drop=True)
    bottleneck.to_csv(DATA_PROCESSED_DIR / "scheduling_bottleneck_diagnosis.csv", index=False)

    # Compatibilidad con outputs legacy
    legacy_prior = priorities.rename(
        columns={
            "intervention_priority_score": "indice_prioridad",
            "recommended_entry_sequence": "ranking_prioridad_taller",
            "component_rul_estimate": "rul_dias",
        }
    )
    legacy_prior.to_csv(DATA_PROCESSED_DIR / "priorizacion_intervenciones.csv", index=False)

    legacy_plan = scheduling.rename(
        columns={
            "deposito_recomendado": "deposito_id",
            "ventana_temporal_sugerida": "dia_horizonte",
            "prioridad": "ranking_prioridad_taller",
        }
    )
    legacy_plan.to_csv(DATA_PROCESSED_DIR / "plan_taller_14d.csv", index=False)

    # Reportes para dirección
    OUTPUTS_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    priorities.head(50).to_csv(OUTPUTS_REPORTS_DIR / "priorizacion_intervenciones.csv", index=False)
    scheduling.head(100).to_csv(OUTPUTS_REPORTS_DIR / "plan_taller_14d.csv", index=False)
    before_after.to_csv(OUTPUTS_REPORTS_DIR / "scheduling_before_after_metrics.csv", index=False)
    before_after_delta.to_csv(OUTPUTS_REPORTS_DIR / "scheduling_before_after_deltas.csv", index=False)
    bottleneck.to_csv(OUTPUTS_REPORTS_DIR / "scheduling_bottleneck_diagnosis.csv", index=False)
    status_dist.to_csv(OUTPUTS_REPORTS_DIR / "scheduling_status_distribution.csv", index=False)
    cap_after.to_csv(OUTPUTS_REPORTS_DIR / "workshop_capacity_calendar.csv", index=False)

    tradeoffs = pd.DataFrame(
        {
            "tradeoff": [
                "capacidad_flexible_vs_coste_operativo_taller",
                "carry_over_controlado_vs_riesgo_de_espera",
                "reasignacion_deposito_vs_eficiencia_local",
                "escalar_decision_vs_ejecucion_automatica",
            ],
            "lectura": [
                "La bolsa flexible reduce pendientes críticos, pero debe limitarse para no simular sobrecapacidad estructural.",
                "Carry-over mejora ejecutabilidad sin maquillar urgencias: fuera de ventana preferida queda etiquetado explícitamente.",
                "Reasignar depósitos reduce cuellos, pero implica coordinación logística adicional.",
                "Escalar decisión preserva seguridad cuando la señal no permite automatizar intervención.",
            ],
        }
    )
    tradeoffs.to_csv(OUTPUTS_REPORTS_DIR / "workshop_tradeoffs.csv", index=False)

    _write_scheduling_framework_doc(
        before_after=before_after_delta,
        bottlenecks=bottleneck.head(10),
        statuses_after=status_after.assign(share_pct=(status_after["share"] * 100).round(3))[["estado_intervencion", "share_pct"]].sort_values(
            "share_pct", ascending=False
        ),
    )

    # Artefactos de hardening de recomendación (before/after + reglas + ejemplos).
    score_after = pd.read_csv(DATA_PROCESSED_DIR / "scoring_componentes.csv")
    build_recommendation_before_after_outputs(score_after=score_after, priorities_after=priorities)
    examples = (
        base.sort_values("intervention_priority_score", ascending=False)
        .groupby("decision_type", as_index=False, sort=False)
        .head(1)[
            [
                "unidad_id",
                "componente_id",
                "decision_type",
                "decision_rule_id",
                "recommended_action_initial",
                "prob_fallo_30d",
                "health_score",
                "component_rul_estimate",
                "service_impact_score",
                "deferral_risk_score",
                "workshop_fit_score",
                "confidence_flag",
                "decision_rationale",
            ]
        ]
        .reset_index(drop=True)
    )
    write_recommendation_logic_doc(examples_df=examples)

    return priorities, scheduling


if __name__ == "__main__":
    run_workshop_prioritization()
