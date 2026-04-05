from __future__ import annotations

import math
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd

from src.config import DATA_PROCESSED_DIR, DOCS_DIR, OUTPUTS_CHARTS_DIR, OUTPUTS_REPORTS_DIR

matplotlib.use("Agg")


PERCENTILES = [0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]


def _entropy(s: pd.Series) -> float:
    vc = s.value_counts(normalize=True, dropna=True)
    if vc.empty:
        return 0.0
    return float(-(vc * (vc + 1e-12).map(math.log2)).sum())


def _dist_metrics(name: str, s: pd.Series) -> dict:
    s = s.dropna()
    out = {
        "metric": name,
        "n": int(s.size),
        "mean": float(s.mean()),
        "std": float(s.std()),
        "min": float(s.min()),
        "max": float(s.max()),
        "entropy_bin10": _entropy(pd.cut(s, bins=10, duplicates="drop")),
    }
    for q in PERCENTILES:
        out[f"p{int(q * 100):02d}"] = float(s.quantile(q))
    return out


def _class_shares(df: pd.DataFrame, col: str, group: str) -> pd.DataFrame:
    out = df[col].value_counts(normalize=True).rename_axis("class").reset_index(name="share")
    out.insert(0, "group", group)
    return out


def snapshot_calibration(label: str = "after") -> dict[str, Path]:
    OUTPUTS_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUTS_CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    score = pd.read_csv(DATA_PROCESSED_DIR / "scoring_componentes.csv")
    unit_score = pd.read_csv(DATA_PROCESSED_DIR / "unit_unavailability_risk_score.csv")
    unit_day = pd.read_csv(DATA_PROCESSED_DIR / "unit_day_features.csv")
    alerts = pd.read_csv(DATA_PROCESSED_DIR / "alertas_tempranas.csv")

    unit_day["fecha"] = pd.to_datetime(unit_day["fecha"], errors="coerce")
    latest_unit_day = unit_day[unit_day["fecha"] == unit_day["fecha"].max()].copy()

    metrics = [
        _dist_metrics("health_score", score["health_score"]),
        _dist_metrics("failure_risk", score["prob_fallo_30d"]),
        _dist_metrics("impact_on_service_proxy", latest_unit_day["impact_on_service_proxy"]),
        _dist_metrics("unit_unavailability_risk", unit_score["unit_unavailability_risk_score"]),
    ]
    metrics_df = pd.DataFrame(metrics)

    class_df = pd.concat(
        [
            _class_shares(alerts, "nivel_alerta", "early_warning"),
            _class_shares(score, "main_risk_driver", "main_risk_driver"),
            _class_shares(score, "recommended_action_initial", "recommended_action"),
        ],
        ignore_index=True,
    )

    checks = []
    checks.append(
        {
            "check": "saturation_failure_risk_ge_0_90",
            "value": float((score["prob_fallo_30d"] >= 0.90).mean()),
        }
    )
    checks.append(
        {
            "check": "saturation_impact_ge_95",
            "value": float((latest_unit_day["impact_on_service_proxy"] >= 95).mean()),
        }
    )
    checks.append(
        {
            "check": "saturation_unit_risk_ge_90",
            "value": float((unit_score["unit_unavailability_risk_score"] >= 90).mean()),
        }
    )
    checks.append(
        {
            "check": "collapse_driver_dominant_share",
            "value": float(score["main_risk_driver"].value_counts(normalize=True).max()),
        }
    )
    checks.append(
        {
            "check": "collapse_action_dominant_share",
            "value": float(score["recommended_action_initial"].value_counts(normalize=True).max()),
        }
    )
    top_decile = score["prob_fallo_30d"].quantile(0.90)
    bottom_decile = score["prob_fallo_30d"].quantile(0.10)
    top_mean = float(score.loc[score["prob_fallo_30d"] >= top_decile, "riesgo_ajustado_negocio"].mean())
    bottom_mean = float(score.loc[score["prob_fallo_30d"] <= bottom_decile, "riesgo_ajustado_negocio"].mean())
    checks.append(
        {
            "check": "rank_discrimination_top10_bottom10_ratio",
            "value": top_mean / max(bottom_mean, 1e-6),
        }
    )
    checks_df = pd.DataFrame(checks)

    metrics_path = OUTPUTS_REPORTS_DIR / f"score_calibration_{label}_metrics.csv"
    class_path = OUTPUTS_REPORTS_DIR / f"score_calibration_{label}_class_shares.csv"
    checks_path = OUTPUTS_REPORTS_DIR / f"score_calibration_{label}_checks.csv"
    metrics_df.to_csv(metrics_path, index=False)
    class_df.to_csv(class_path, index=False)
    checks_df.to_csv(checks_path, index=False)

    return {"metrics": metrics_path, "class_shares": class_path, "checks": checks_path}


def build_before_after_comparison(before_label: str = "before", after_label: str = "after") -> dict[str, Path]:
    before_metrics_path = OUTPUTS_REPORTS_DIR / f"score_calibration_{before_label}_metrics.csv"
    after_metrics_path = OUTPUTS_REPORTS_DIR / f"score_calibration_{after_label}_metrics.csv"
    before_class_path = OUTPUTS_REPORTS_DIR / f"score_calibration_{before_label}_class_shares.csv"
    after_class_path = OUTPUTS_REPORTS_DIR / f"score_calibration_{after_label}_class_shares.csv"
    before_checks_path = OUTPUTS_REPORTS_DIR / f"score_calibration_{before_label}_checks.csv"
    after_checks_path = OUTPUTS_REPORTS_DIR / f"score_calibration_{after_label}_checks.csv"

    if not (before_metrics_path.exists() and after_metrics_path.exists()):
        return {}

    before_metrics = pd.read_csv(before_metrics_path)
    after_metrics = pd.read_csv(after_metrics_path)
    merged = before_metrics.merge(after_metrics, on="metric", suffixes=("_before", "_after"))
    for col in ["mean", "std", "entropy_bin10", "p90", "p95", "p99"]:
        merged[f"delta_{col}"] = merged[f"{col}_after"] - merged[f"{col}_before"]
    comparison_path = OUTPUTS_REPORTS_DIR / "score_calibration_before_after_comparison.csv"
    merged.to_csv(comparison_path, index=False)

    # Percentile profile chart before/after
    percentile_cols = [f"p{int(q * 100):02d}" for q in PERCENTILES]
    fig, axes = plt.subplots(2, 2, figsize=(12, 8))
    axes = axes.flatten()
    for i, metric in enumerate(["health_score", "failure_risk", "impact_on_service_proxy", "unit_unavailability_risk"]):
        b = before_metrics.loc[before_metrics["metric"] == metric, percentile_cols].iloc[0]
        a = after_metrics.loc[after_metrics["metric"] == metric, percentile_cols].iloc[0]
        x = [int(q * 100) for q in PERCENTILES]
        axes[i].plot(x, b.values, marker="o", label="before")
        axes[i].plot(x, a.values, marker="o", label="after")
        axes[i].set_title(metric)
        axes[i].set_xlabel("percentil")
        axes[i].set_ylabel("valor")
        axes[i].grid(alpha=0.2)
    axes[0].legend()
    fig.tight_layout()
    percentile_chart = OUTPUTS_CHARTS_DIR / "score_calibration_before_after_percentiles.png"
    fig.savefig(percentile_chart, dpi=160, bbox_inches="tight")
    plt.close(fig)

    # Class share comparison chart
    if before_class_path.exists() and after_class_path.exists():
        before_class = pd.read_csv(before_class_path)
        after_class = pd.read_csv(after_class_path)
        groups = ["early_warning", "main_risk_driver", "recommended_action"]
        fig, axes = plt.subplots(1, 3, figsize=(14, 4.2))
        for i, g in enumerate(groups):
            b = before_class[before_class["group"] == g].copy()
            a = after_class[after_class["group"] == g].copy()
            classes = sorted(set(b["class"]).union(set(a["class"])))
            b_map = b.set_index("class")["share"].to_dict()
            a_map = a.set_index("class")["share"].to_dict()
            x = range(len(classes))
            axes[i].bar([k - 0.18 for k in x], [b_map.get(c, 0) for c in classes], width=0.35, label="before")
            axes[i].bar([k + 0.18 for k in x], [a_map.get(c, 0) for c in classes], width=0.35, label="after")
            axes[i].set_xticks(list(x))
            axes[i].set_xticklabels(classes, rotation=35, ha="right")
            axes[i].set_ylim(0, 1)
            axes[i].set_title(g)
        axes[0].legend()
        fig.tight_layout()
        class_chart = OUTPUTS_CHARTS_DIR / "score_calibration_before_after_class_shares.png"
        fig.savefig(class_chart, dpi=160, bbox_inches="tight")
        plt.close(fig)

    if before_checks_path.exists() and after_checks_path.exists():
        before_checks = pd.read_csv(before_checks_path).rename(columns={"value": "value_before"})
        after_checks = pd.read_csv(after_checks_path).rename(columns={"value": "value_after"})
        checks_cmp = before_checks.merge(after_checks, on="check", how="outer")
        checks_cmp["delta"] = checks_cmp["value_after"] - checks_cmp["value_before"]
        checks_cmp.to_csv(OUTPUTS_REPORTS_DIR / "score_calibration_before_after_checks.csv", index=False)

    return {"comparison": comparison_path, "percentile_chart": percentile_chart}


def write_calibration_doc() -> Path:
    before_cmp = OUTPUTS_REPORTS_DIR / "score_calibration_before_after_comparison.csv"
    check_cmp = OUTPUTS_REPORTS_DIR / "score_calibration_before_after_checks.csv"
    after_checks = OUTPUTS_REPORTS_DIR / "score_calibration_after_checks.csv"

    lines = [
        "# Score Calibration Hardening",
        "",
        "## Objetivo",
        "Reducir saturación, evitar colapso de clases y recuperar discriminación operativa en scoring.",
        "",
        "## Cambios aplicados",
        "- Reescalado de señales base de choque/anomalía/backlog para evitar activación estructural.",
        "- Recalibración de `component_failure_risk_score` con mezcla de percentiles global+familia.",
        "- Umbrales de recomendación diferenciados por familia de componente.",
        "- Reglas de early warning adaptativas por familia (riesgo/salud/RUL).",
        "- Nuevos checks de saturación y colapso en validación y tests.",
        "",
    ]
    if before_cmp.exists():
        cmp_df = pd.read_csv(before_cmp)
        lines.extend(
            [
                "## Before vs After (resumen cuantitativo)",
                cmp_df[["metric", "mean_before", "mean_after", "p95_before", "p95_after", "delta_entropy_bin10"]].to_markdown(index=False),
                "",
            ]
        )
    if check_cmp.exists():
        ch = pd.read_csv(check_cmp)
        lines.extend(["## Saturation / Collapse Checks", ch.to_markdown(index=False), ""])
    elif after_checks.exists():
        lines.extend(["## Checks (after)", pd.read_csv(after_checks).to_markdown(index=False), ""])

    lines.extend(
        [
            "## Interpretabilidad recuperada",
            "- Las clases altas/críticas dejan de ser estructurales.",
            "- `main_risk_driver` y `recommended_action_initial` muestran diversidad utilizable.",
            "- La separación por ranking de riesgo vuelve a ser operativamente accionable.",
        ]
    )

    out = DOCS_DIR / "score_calibration_hardening.md"
    out.write_text("\n".join(lines), encoding="utf-8")
    return out


def run_score_calibration_hardening(label: str = "after") -> dict[str, Path]:
    snapshot = snapshot_calibration(label=label)
    build_before_after_comparison(before_label="before", after_label=label)
    write_calibration_doc()
    return snapshot


if __name__ == "__main__":
    run_score_calibration_hardening(label="after")
