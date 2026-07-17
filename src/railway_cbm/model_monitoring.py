"""Valida temporalmente el score de riesgo y gobierna su uso operativo."""

from __future__ import annotations

from collections.abc import Iterable

import numpy as np
import pandas as pd

from railway_cbm.config import DATA_PROCESSED_DIR, DATA_RAW_DIR, DOCS_DIR
from railway_cbm.risk_scoring import calculate_component_scores

OUTCOME_WINDOW_DAYS = 30
MIN_TRAINING_ROWS = 500
MIN_TRAINING_EVENTS = 20
DRIFT_FEATURES = (
    "deterioration_index",
    "degradation_velocity",
    "operating_stress_index",
    "anomaly_count_30d",
    "maintenance_restoration_index",
)


def _roc_auc(y_true: pd.Series, score: pd.Series) -> float:
    positives = int(y_true.sum())
    negatives = int(len(y_true) - positives)
    if positives == 0 or negatives == 0:
        return float("nan")
    ranks = score.rank(method="average")
    positive_rank_sum = float(ranks[y_true == 1].sum())
    return (positive_rank_sum - positives * (positives + 1) / 2) / (positives * negatives)


def _average_precision(y_true: pd.Series, score: pd.Series) -> float:
    order = np.argsort(-score.to_numpy(), kind="stable")
    ordered = y_true.to_numpy(dtype=int)[order]
    positives = int(ordered.sum())
    if positives == 0:
        return float("nan")
    precision_at_rank = np.cumsum(ordered) / np.arange(1, len(ordered) + 1)
    return float(precision_at_rank[ordered == 1].sum() / positives)


def _expected_calibration_error(y_true: pd.Series, probability: pd.Series, bins: int = 10) -> float:
    valid = probability.notna()
    if not valid.any():
        return float("nan")
    work = pd.DataFrame({"outcome": y_true[valid], "probability": probability[valid]})
    work["bin"] = pd.cut(work["probability"], np.linspace(0, 1, bins + 1), include_lowest=True)
    grouped = work.groupby("bin", observed=False).agg(
        rows=("outcome", "size"),
        observed_rate=("outcome", "mean"),
        predicted_rate=("probability", "mean"),
    )
    grouped = grouped[grouped["rows"] > 0]
    return float(
        ((grouped["rows"] / grouped["rows"].sum()) * (grouped["observed_rate"] - grouped["predicted_rate"]).abs()).sum()
    )


def _fit_monotonic_calibrator(training: pd.DataFrame) -> tuple[np.ndarray, np.ndarray] | None:
    if len(training) < MIN_TRAINING_ROWS or int(training["failure_in_30d"].sum()) < MIN_TRAINING_EVENTS:
        return None
    score = training["risk_score"].astype(float)
    quantiles = np.unique(score.quantile(np.linspace(0, 1, 11)).to_numpy())
    if len(quantiles) < 4:
        return None
    inner_edges = quantiles[1:-1]
    bin_index = np.searchsorted(inner_edges, score.to_numpy(), side="right")
    rates = []
    for idx in range(len(inner_edges) + 1):
        outcomes = training.loc[bin_index == idx, "failure_in_30d"]
        rates.append((float(outcomes.sum()) + 1.0) / (len(outcomes) + 2.0))
    return inner_edges, np.maximum.accumulate(np.asarray(rates, dtype=float))


def _apply_calibrator(score: pd.Series, calibrator: tuple[np.ndarray, np.ndarray] | None) -> pd.Series:
    if calibrator is None:
        return pd.Series(np.nan, index=score.index, dtype=float)
    inner_edges, rates = calibrator
    indices = np.searchsorted(inner_edges, score.to_numpy(dtype=float), side="right")
    return pd.Series(rates[indices], index=score.index, dtype=float)


def _score_temporal_snapshots(component_day: pd.DataFrame, failures: pd.DataFrame) -> pd.DataFrame:
    component_day = component_day.copy()
    component_day["fecha"] = pd.to_datetime(component_day["fecha"], errors="raise")
    failures = failures.copy()
    failures["fecha_falla"] = pd.to_datetime(failures["fecha_falla"], errors="raise")

    max_date = component_day["fecha"].max()
    mature_limit = max_date - pd.Timedelta(days=OUTCOME_WINDOW_DAYS)
    candidate_dates = (
        component_day.loc[component_day["fecha"] <= mature_limit, ["fecha"]]
        .assign(month=lambda frame: frame["fecha"].dt.to_period("M"))
        .groupby("month", as_index=False)["fecha"]
        .max()["fecha"]
        .sort_values()
        .tolist()
    )
    if len(candidate_dates) < 6:
        raise ValueError("Se requieren al menos seis cortes mensuales maduros para validación temporal")

    event_map = {
        str(component_id): group["fecha_falla"].sort_values().to_numpy(dtype="datetime64[ns]")
        for component_id, group in failures.groupby("componente_id", sort=False)
    }
    snapshots: list[pd.DataFrame] = []
    for cutoff in candidate_dates:
        snapshot = component_day[component_day["fecha"] == cutoff].copy()
        snapshot = calculate_component_scores(snapshot)
        cutoff_np = np.datetime64(cutoff)
        horizon_np = np.datetime64(cutoff + pd.Timedelta(days=OUTCOME_WINDOW_DAYS))
        snapshot["failure_in_30d"] = [
            int(((events > cutoff_np) & (events <= horizon_np)).any())
            if (events := event_map.get(str(component_id))) is not None
            else 0
            for component_id in snapshot["componente_id"]
        ]
        snapshots.append(
            snapshot.rename(columns={"component_failure_risk_score": "risk_score"})[
                [
                    "fecha",
                    "unidad_id",
                    "componente_id",
                    "component_family",
                    "risk_score",
                    "failure_in_30d",
                ]
            ]
        )
    validation = pd.concat(snapshots, ignore_index=True)
    validation["calibrated_probability_30d"] = np.nan
    validation["calibration_status"] = "insufficient_prior_history"

    for cutoff in candidate_dates:
        training_limit = cutoff - pd.Timedelta(days=OUTCOME_WINDOW_DAYS)
        training = validation[validation["fecha"] <= training_limit]
        calibrator = _fit_monotonic_calibrator(training)
        target = validation["fecha"] == cutoff
        validation.loc[target, "calibrated_probability_30d"] = _apply_calibrator(
            validation.loc[target, "risk_score"], calibrator
        )
        if calibrator is not None:
            validation.loc[target, "calibration_status"] = "rolling_out_of_sample"
    return validation


def _fold_metrics(validation: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    groups: Iterable[tuple[object, pd.DataFrame]] = [
        *(validation.groupby("fecha", sort=True)),
        ("overall", validation),
    ]
    for cutoff, frame in groups:
        y_true = frame["failure_in_30d"].astype(int)
        score = frame["risk_score"].astype(float)
        probability = frame["calibrated_probability_30d"].astype(float)
        prevalence = float(y_true.mean())
        top_count = max(1, int(np.ceil(len(frame) * 0.10)))
        top_rate = float(frame.nlargest(top_count, "risk_score")["failure_in_30d"].mean())
        valid_probability = probability.notna()
        rows.append(
            {
                "cutoff": cutoff if isinstance(cutoff, str) else pd.Timestamp(cutoff).date().isoformat(),
                "rows": len(frame),
                "failures_30d": int(y_true.sum()),
                "prevalence": prevalence,
                "roc_auc": _roc_auc(y_true, score),
                "average_precision": _average_precision(y_true, score),
                "top_decile_lift": top_rate / prevalence if prevalence > 0 else np.nan,
                "raw_score_brier": float(np.mean((score - y_true) ** 2)),
                "calibrated_rows": int(valid_probability.sum()),
                "calibrated_brier": (
                    float(np.mean((probability[valid_probability] - y_true[valid_probability]) ** 2))
                    if valid_probability.any()
                    else np.nan
                ),
                "calibration_error": _expected_calibration_error(y_true, probability),
            }
        )
    return pd.DataFrame(rows)


def _calibration_table(validation: pd.DataFrame) -> pd.DataFrame:
    calibrated = validation.dropna(subset=["calibrated_probability_30d"]).copy()
    if calibrated.empty:
        return pd.DataFrame(columns=["probability_bin", "rows", "predicted_rate", "observed_rate"])
    calibrated["probability_bin"] = pd.cut(
        calibrated["calibrated_probability_30d"], np.linspace(0, 1, 11), include_lowest=True
    )
    return (
        calibrated.groupby("probability_bin", observed=False)
        .agg(
            rows=("failure_in_30d", "size"),
            predicted_rate=("calibrated_probability_30d", "mean"),
            observed_rate=("failure_in_30d", "mean"),
        )
        .reset_index()
        .assign(probability_bin=lambda frame: frame["probability_bin"].astype(str))
    )


def _population_stability_index(reference: pd.Series, current: pd.Series) -> float:
    reference = pd.to_numeric(reference, errors="coerce").dropna()
    current = pd.to_numeric(current, errors="coerce").dropna()
    if reference.empty or current.empty:
        return float("nan")
    edges = np.unique(reference.quantile(np.linspace(0, 1, 11)).to_numpy())
    if len(edges) < 3:
        return 0.0
    edges[0], edges[-1] = -np.inf, np.inf
    reference_share = pd.cut(reference, edges, include_lowest=True).value_counts(normalize=True, sort=False)
    current_share = pd.cut(current, edges, include_lowest=True).value_counts(normalize=True, sort=False)
    epsilon = 1e-6
    ref = reference_share.to_numpy(dtype=float).clip(epsilon)
    cur = current_share.to_numpy(dtype=float).clip(epsilon)
    return float(np.sum((cur - ref) * np.log(cur / ref)))


def _drift_report(component_day: pd.DataFrame) -> pd.DataFrame:
    frame = component_day.copy()
    frame["fecha"] = pd.to_datetime(frame["fecha"], errors="raise")
    first_date, last_date = frame["fecha"].min(), frame["fecha"].max()
    reference = frame[frame["fecha"] <= first_date + pd.Timedelta(days=89)]
    current = frame[frame["fecha"] >= last_date - pd.Timedelta(days=89)]
    rows = []
    for feature in DRIFT_FEATURES:
        psi = _population_stability_index(reference[feature], current[feature])
        rows.append(
            {
                "feature": feature,
                "reference_start": first_date.date().isoformat(),
                "reference_end": reference["fecha"].max().date().isoformat(),
                "current_start": current["fecha"].min().date().isoformat(),
                "current_end": last_date.date().isoformat(),
                "psi": psi,
                "drift_level": "high" if psi >= 0.25 else ("moderate" if psi >= 0.10 else "low"),
            }
        )
    return pd.DataFrame(rows)


def _source_mode() -> str:
    manifest_path = DATA_PROCESSED_DIR / "input_data_manifest.csv"
    if not manifest_path.exists():
        return "unknown"
    manifest = pd.read_csv(manifest_path)
    return str(manifest["source_mode"].iloc[0])


def _readiness_assessment(metrics: pd.DataFrame, drift: pd.DataFrame, source_mode: str) -> pd.DataFrame:
    overall = metrics[metrics["cutoff"] == "overall"].iloc[0]
    fold_count = int((metrics["cutoff"] != "overall").sum())
    max_psi = float(drift["psi"].max())
    checks = [
        ("external_historical_source", source_mode == "external", source_mode, "external"),
        ("mature_temporal_folds", fold_count >= 6, fold_count, ">=6"),
        ("observed_failures", int(overall["failures_30d"]) >= 30, int(overall["failures_30d"]), ">=30"),
        ("discrimination_roc_auc", float(overall["roc_auc"]) >= 0.65, float(overall["roc_auc"]), ">=0.65"),
        (
            "out_of_sample_calibration_error",
            pd.notna(overall["calibration_error"]) and float(overall["calibration_error"]) <= 0.10,
            float(overall["calibration_error"]) if pd.notna(overall["calibration_error"]) else np.nan,
            "<=0.10",
        ),
        ("feature_drift_psi", max_psi < 0.25, max_psi, "<0.25"),
    ]
    return pd.DataFrame(
        [
            {
                "check": name,
                "status": "passed" if passed else "failed",
                "observed": observed,
                "threshold": threshold,
                "blocks_autonomous_use": not passed,
            }
            for name, passed, observed, threshold in checks
        ]
    )


def _write_monitoring_doc(source_mode: str, readiness: pd.DataFrame) -> None:
    failed = readiness.loc[readiness["status"] == "failed", "check"].tolist()
    autonomous_use = not failed
    lines = [
        "# Validación temporal y monitorización del riesgo",
        "",
        f"- Fuente evaluada: `{source_mode}`.",
        f"- Uso autónomo permitido: `{'sí' if autonomous_use else 'no'}`.",
        f"- Ventana de resultado: {OUTCOME_WINDOW_DAYS} días.",
        "- Esquema: cortes mensuales; cada calibrador sólo usa cortes anteriores con resultado ya maduro.",
        "- Discriminación: ROC AUC, average precision y lift del decil superior.",
        "- Calibración: Brier y error de calibración fuera de muestra.",
        "- Deriva: PSI entre los primeros y últimos 90 días disponibles.",
        "",
        "## Puerta de despliegue",
        "",
        readiness.to_markdown(index=False),
        "",
        "La fuente sintética sirve para reproducibilidad y prueba del sistema, pero nunca habilita uso autónomo. "
        "La aprobación exige histórico externo, resultados maduros, discriminación, calibración y estabilidad dentro de umbral.",
    ]
    (DOCS_DIR / "model_monitoring.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_model_monitoring() -> pd.DataFrame:
    """Genera validación temporal, calibración, deriva y puerta de despliegue."""
    component_day = pd.read_csv(DATA_PROCESSED_DIR / "component_day_features.csv", low_memory=False)
    failures = pd.read_csv(DATA_RAW_DIR / "fallas_historicas.csv")
    validation = _score_temporal_snapshots(component_day, failures)
    metrics = _fold_metrics(validation)
    calibration = _calibration_table(validation)
    drift = _drift_report(component_day)
    source_mode = _source_mode()
    readiness = _readiness_assessment(metrics, drift, source_mode)
    autonomous_use = not (readiness["status"] == "failed").any()
    failed_checks = "|".join(readiness.loc[readiness["status"] == "failed", "check"])
    deployment_gate = pd.DataFrame(
        [
            {
                "gate_name": "risk_model_autonomous_use",
                "source_mode": source_mode,
                "autonomous_use_allowed": autonomous_use,
                "operating_mode": "controlled" if autonomous_use else "shadow",
                "failed_checks": failed_checks,
            }
        ]
    )

    validation.to_csv(DATA_PROCESSED_DIR / "risk_temporal_validation.csv", index=False)
    metrics.to_csv(DATA_PROCESSED_DIR / "risk_temporal_performance.csv", index=False)
    calibration.to_csv(DATA_PROCESSED_DIR / "risk_calibration.csv", index=False)
    drift.to_csv(DATA_PROCESSED_DIR / "feature_drift_report.csv", index=False)
    readiness.to_csv(DATA_PROCESSED_DIR / "model_readiness_assessment.csv", index=False)
    deployment_gate.to_csv(DATA_PROCESSED_DIR / "model_deployment_gate.csv", index=False)
    _write_monitoring_doc(source_mode, readiness)
    return readiness
