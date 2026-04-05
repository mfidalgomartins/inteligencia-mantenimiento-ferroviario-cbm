from __future__ import annotations

import pandas as pd

from src.config import DATA_PROCESSED_DIR, OUTPUTS_REPORTS_DIR


def run_defer_impact_analysis() -> tuple[pd.DataFrame, pd.DataFrame]:
    priorities = pd.read_csv(DATA_PROCESSED_DIR / "workshop_priority_table.csv")

    universe = priorities.head(450).copy()
    scenarios = [0, 7, 14, 21, 30, 45]

    rows = []
    for defer_days in scenarios:
        factor = 1 + (defer_days / 45)
        for row in universe.itertuples(index=False):
            prob_increment = min(0.25, (row.deferral_risk_score / 100) * 0.35 * (defer_days / 45))
            prob_fallo_ajustada = min(0.98, row.prob_fallo_30d + prob_increment)

            downtime_h_esperado = prob_fallo_ajustada * (2.2 + row.service_impact_score / 25) * factor
            impacto_servicio = downtime_h_esperado * (250 + row.service_impact_score * 9)
            coste_tecnico = prob_fallo_ajustada * (1200 + row.intervention_priority_score * 18)
            coste_total = impacto_servicio + coste_tecnico

            rows.append(
                {
                    "defer_dias": defer_days,
                    "unidad_id": row.unidad_id,
                    "componente_id": row.componente_id,
                    "deposito_recomendado": row.deposito_recomendado,
                    "intervention_priority_score": row.intervention_priority_score,
                    "deferral_risk_score": row.deferral_risk_score,
                    "service_impact_score": row.service_impact_score,
                    "prob_fallo_ajustada": round(prob_fallo_ajustada, 6),
                    "downtime_h_esperado": round(downtime_h_esperado, 4),
                    "impacto_servicio_proxy": round(impacto_servicio, 2),
                    "costo_esperado_eur": round(coste_total, 2),
                }
            )

    detalle = pd.DataFrame(rows)
    resumen = (
        detalle.groupby("defer_dias", as_index=False)
        .agg(
            prob_fallo_media=("prob_fallo_ajustada", "mean"),
            downtime_total_h=("downtime_h_esperado", "sum"),
            impacto_servicio_total=("impacto_servicio_proxy", "sum"),
            costo_total_eur=("costo_esperado_eur", "sum"),
        )
        .sort_values("defer_dias")
    )

    detalle.to_csv(DATA_PROCESSED_DIR / "impacto_diferimiento_detalle.csv", index=False)
    resumen.to_csv(DATA_PROCESSED_DIR / "impacto_diferimiento_resumen.csv", index=False)

    OUTPUTS_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    resumen.to_csv(OUTPUTS_REPORTS_DIR / "impacto_diferimiento_resumen.csv", index=False)

    return detalle, resumen


if __name__ == "__main__":
    run_defer_impact_analysis()
