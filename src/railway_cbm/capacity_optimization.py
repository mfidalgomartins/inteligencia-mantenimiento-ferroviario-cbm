"""Diagnostica saturación y activa una asignación MILP sólo cuando aporta valor."""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
from scipy.optimize import Bounds, LinearConstraint, milp
from scipy.sparse import coo_matrix

from railway_cbm.config import DATA_PROCESSED_DIR, DOCS_DIR

SATURATION_TRIGGER = 0.85
MIN_SATURATED_DAY_SHARE = 0.10
PENDING_CAPACITY_STATUS = "pendiente_capacidad"
MAX_ACCEPTABLE_MIP_GAP = 0.10


def assess_optimization_need(capacity: pd.DataFrame, schedule: pd.DataFrame) -> dict[str, object]:
    """Evalúa si la saturación observada justifica optimización formal."""
    frame = capacity.copy()
    frame["utilization"] = frame["total_used_h"] / frame["total_capacity_h"].replace(0, np.nan)
    valid = frame["utilization"].notna()
    max_utilization = float(frame.loc[valid, "utilization"].max()) if valid.any() else 0.0
    saturated_day_share = float((frame.loc[valid, "utilization"] >= SATURATION_TRIGGER).mean()) if valid.any() else 0.0
    pending_capacity = int((schedule["estado_intervencion"] == PENDING_CAPACITY_STATUS).sum())
    required = (
        max_utilization >= SATURATION_TRIGGER
        and saturated_day_share >= MIN_SATURATED_DAY_SHARE
        and pending_capacity > 0
    )
    return {
        "formal_optimization_required": required,
        "max_daily_utilization": max_utilization,
        "saturated_depot_day_share": saturated_day_share,
        "pending_capacity_cases": pending_capacity,
        "saturation_trigger": SATURATION_TRIGGER,
        "minimum_saturated_day_share": MIN_SATURATED_DAY_SHARE,
        "trigger_reason": (
            "persistent_saturation_and_pending_capacity" if required else "heuristic_capacity_is_sufficient"
        ),
    }


def _build_stress_scenarios(capacity: pd.DataFrame, schedule: pd.DataFrame) -> pd.DataFrame:
    total_capacity = float(capacity["total_capacity_h"].sum())
    used_hours = float(capacity["total_used_h"].sum())
    pending_hours = float(
        schedule.loc[schedule["estado_intervencion"] == PENDING_CAPACITY_STATUS, "horas_programadas"].sum()
    )
    scenarios = [
        ("baseline", 1.00, 1.00),
        ("demand_plus_15pct", 1.15, 1.00),
        ("capacity_minus_10pct", 1.00, 0.90),
        ("combined_stress", 1.15, 0.90),
    ]
    rows = []
    for name, demand_factor, capacity_factor in scenarios:
        demand = (used_hours + pending_hours) * demand_factor
        available = total_capacity * capacity_factor
        rows.append(
            {
                "scenario": name,
                "demand_factor": demand_factor,
                "capacity_factor": capacity_factor,
                "demand_hours": demand,
                "available_hours": available,
                "load_ratio": demand / available if available else np.nan,
                "capacity_gap_hours": available - demand,
                "stress_breach": demand > available * SATURATION_TRIGGER,
            }
        )
    return pd.DataFrame(rows)


def _candidate_variables(
    priorities: pd.DataFrame, weekly_capacity: pd.DataFrame
) -> tuple[list[dict[str, object]], dict[str, int]]:
    available_keys = {(str(row.deposito_id), int(row.week_index)) for row in weekly_capacity.itertuples(index=False)}
    variables: list[dict[str, object]] = []
    unscheduled_index: dict[str, int] = {}
    carry_days = {"P1": 14, "P2": 21, "P3": 14, "P4": 10}

    for case_index, row in priorities.reset_index(drop=True).iterrows():
        case_id = f"{row['unidad_id']}|{row['componente_id']}"
        max_days = min(35, int(row["suggested_window_days"]) + carry_days.get(str(row["bucket_prioridad"]), 14))
        max_week = max(1, int(math.ceil(max_days / 7)))
        depots = [depot for depot in str(row["candidate_depots"]).split("|") if depot]
        for depot_rank, depot_id in enumerate(depots):
            for week_index in range(1, max_week + 1):
                if (depot_id, week_index) not in available_keys:
                    continue
                variables.append(
                    {
                        "case_index": case_index,
                        "case_id": case_id,
                        "depot_id": depot_id,
                        "week_index": week_index,
                        "hours": float(row["hours_required"]),
                        "cost": (
                            depot_rank * 4.0
                            + (week_index - 1) * (1.0 + float(row["queue_score"]) / 25.0)
                            + (0.0 if depot_id == str(row["deposito_id"]) else 2.0)
                        ),
                        "is_unscheduled": False,
                    }
                )
        unscheduled_index[case_id] = len(variables)
        variables.append(
            {
                "case_index": case_index,
                "case_id": case_id,
                "depot_id": None,
                "week_index": None,
                "hours": float(row["hours_required"]),
                "cost": 100.0 + float(row["queue_score"]) * 5.0 + float(row["deferral_risk_score"]) * 2.0,
                "is_unscheduled": True,
            }
        )
    return variables, unscheduled_index


def _is_acceptable_solution(result: object) -> bool:
    """Acepta óptimos o incumbentes factibles con brecha controlada."""
    if getattr(result, "x", None) is None:
        return False
    if bool(getattr(result, "success", False)):
        return True
    gap = float(getattr(result, "mip_gap", np.inf))
    return int(getattr(result, "status", -1)) == 1 and np.isfinite(gap) and gap <= MAX_ACCEPTABLE_MIP_GAP


def _solver_status_label(*, status: int, mip_gap: float) -> str:
    if status == 0 and mip_gap <= 1e-9:
        return "proven_optimal"
    if status == 0:
        return "optimal_within_tolerance"
    return "feasible_within_limit"


def _solve_capacity_allocation(priorities: pd.DataFrame, capacity: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    cap = capacity.copy()
    cap["slot_date"] = pd.to_datetime(cap["slot_date"], errors="raise")
    start = cap["slot_date"].min()
    cap["week_index"] = ((cap["slot_date"] - start).dt.days // 7 + 1).astype(int)
    weekly_capacity = (
        cap.groupby(["deposito_id", "week_index"], as_index=False)["total_capacity_h"]
        .sum()
        .sort_values(["deposito_id", "week_index"])
    )
    variables, _ = _candidate_variables(priorities, weekly_capacity)
    variable_count = len(variables)
    case_count = len(priorities)
    capacity_keys = [
        (str(row.deposito_id), int(row.week_index), float(row.total_capacity_h))
        for row in weekly_capacity.itertuples(index=False)
    ]

    matrix_rows: list[int] = []
    matrix_cols: list[int] = []
    matrix_data: list[float] = []
    lower: list[float] = []
    upper: list[float] = []

    for case_index in range(case_count):
        row_index = len(lower)
        for variable_index, variable in enumerate(variables):
            if int(variable["case_index"]) == case_index:
                matrix_rows.append(row_index)
                matrix_cols.append(variable_index)
                matrix_data.append(1.0)
        lower.append(1.0)
        upper.append(1.0)

    for depot_id, week_index, available_hours in capacity_keys:
        row_index = len(lower)
        for variable_index, variable in enumerate(variables):
            if variable["depot_id"] == depot_id and variable["week_index"] == week_index:
                matrix_rows.append(row_index)
                matrix_cols.append(variable_index)
                matrix_data.append(float(variable["hours"]))
        lower.append(0.0)
        upper.append(available_hours)

    matrix = coo_matrix(
        (matrix_data, (matrix_rows, matrix_cols)), shape=(len(lower), variable_count), dtype=float
    ).tocsr()
    result = milp(
        c=np.asarray([float(variable["cost"]) for variable in variables]),
        integrality=np.ones(variable_count, dtype=int),
        bounds=Bounds(np.zeros(variable_count), np.ones(variable_count)),
        constraints=LinearConstraint(matrix, np.asarray(lower), np.asarray(upper)),
        options={"time_limit": 20.0, "mip_rel_gap": 0.05},
    )
    if not _is_acceptable_solution(result):
        raise RuntimeError(f"La optimización de capacidad no encontró solución: {result.message}")

    chosen = [variables[index] for index, value in enumerate(result.x) if value >= 0.5]
    allocation = pd.DataFrame(chosen).merge(
        priorities.reset_index(drop=True).reset_index(names="case_index")[
            [
                "case_index",
                "unidad_id",
                "componente_id",
                "bucket_prioridad",
                "queue_score",
                "deferral_risk_score",
            ]
        ],
        on="case_index",
        how="left",
    )
    allocation["allocation_status"] = np.where(allocation["is_unscheduled"], "unallocated", "allocated")
    allocation["solver_status"] = _solver_status_label(status=int(result.status), mip_gap=float(result.mip_gap))
    allocation["objective_value"] = float(result.fun)
    allocation["optimality_gap"] = float(result.mip_gap)
    allocation = allocation.drop(columns=["case_id", "case_index", "is_unscheduled", "cost"])

    used = (
        allocation[allocation["allocation_status"] == "allocated"]
        .groupby(["depot_id", "week_index"], as_index=False)["hours"]
        .sum()
        .rename(columns={"depot_id": "deposito_id", "hours": "allocated_hours"})
    )
    utilization = weekly_capacity.merge(used, on=["deposito_id", "week_index"], how="left")
    utilization["allocated_hours"] = utilization["allocated_hours"].fillna(0.0)
    utilization["utilization"] = utilization["allocated_hours"] / utilization["total_capacity_h"].replace(0, np.nan)
    if (utilization["utilization"] > 1 + 1e-8).any():
        raise RuntimeError("La solución MILP excede la capacidad semanal")
    return allocation, utilization


def _write_capacity_doc(gate: pd.DataFrame, stress: pd.DataFrame, allocation: pd.DataFrame | None) -> None:
    required = bool(gate.iloc[0]["formal_optimization_required"])
    lines = [
        "# Gobierno de capacidad y optimización",
        "",
        "La heurística diaria sigue siendo la propuesta operativa base. La optimización formal sólo se activa cuando "
        f"la utilización supera {SATURATION_TRIGGER:.0%}, la saturación es persistente y existen casos pendientes por capacidad.",
        "",
        "## Puerta de activación",
        "",
        gate.to_markdown(index=False),
        "",
        "## Pruebas de estrés",
        "",
        stress.to_markdown(index=False),
        "",
        "## Resultado formal",
        "",
    ]
    if required and allocation is not None:
        allocated_share = float((allocation["allocation_status"] == "allocated").mean())
        lines.extend(
            [
                f"El MILP depósito-semana asignó {allocated_share:.1%} de los casos respetando capacidad finita.",
                "Su salida es una recomendación de capacidad en modo sombra; no sustituye la secuenciación diaria, "
                "la validación de repuestos ni la aprobación humana.",
            ]
        )
    else:
        lines.append("No se ejecutó MILP porque la puerta objetiva no se activó.")
    (DOCS_DIR / "capacity_optimization.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_capacity_optimization() -> pd.DataFrame:
    """Materializa diagnóstico, estrés y, si procede, asignación formal."""
    priorities = pd.read_csv(DATA_PROCESSED_DIR / "workshop_priority_table.csv")
    schedule = pd.read_csv(DATA_PROCESSED_DIR / "workshop_scheduling_recommendation.csv")
    capacity = pd.read_csv(DATA_PROCESSED_DIR / "workshop_capacity_calendar.csv")
    gate = pd.DataFrame([assess_optimization_need(capacity, schedule)])
    gate.insert(0, "gate_name", "formal_capacity_optimization")
    stress = _build_stress_scenarios(capacity, schedule)
    allocation: pd.DataFrame | None = None
    if bool(gate.iloc[0]["formal_optimization_required"]):
        allocation, utilization = _solve_capacity_allocation(priorities, capacity)
        allocation.to_csv(DATA_PROCESSED_DIR / "formal_capacity_allocation.csv", index=False)
        utilization.to_csv(DATA_PROCESSED_DIR / "formal_capacity_utilization.csv", index=False)

    gate.to_csv(DATA_PROCESSED_DIR / "capacity_optimization_gate.csv", index=False)
    stress.to_csv(DATA_PROCESSED_DIR / "capacity_stress_scenarios.csv", index=False)
    _write_capacity_doc(gate, stress, allocation)
    return gate
