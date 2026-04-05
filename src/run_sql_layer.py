from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from src.config import DATA_PROCESSED_DIR, DATA_RAW_DIR, OUTPUTS_REPORTS_DIR, SQL_DIR


DUCKDB_PATH = DATA_PROCESSED_DIR / "railway_cbm.duckdb"

RAW_TABLES = [
    "flotas",
    "unidades",
    "depositos",
    "componentes_criticos",
    "sensores_componentes",
    "inspecciones_automaticas",
    "eventos_mantenimiento",
    "fallas_historicas",
    "alertas_operativas",
    "intervenciones_programadas",
    "disponibilidad_servicio",
    "asignacion_servicio",
    "backlog_mantenimiento",
    "parametros_operativos_contexto",
    "escenarios_mantenimiento",
]

SQL_SEQUENCE = [
    "01_staging_assets.sql",
    "02_staging_sensors.sql",
    "03_staging_maintenance.sql",
    "04_integrated_component_health.sql",
    "05_integrated_failures_and_alerts.sql",
    "06_integrated_availability.sql",
    "07_analytical_mart_component_day.sql",
    "08_analytical_mart_unit_day.sql",
    "09_analytical_mart_fleet_week.sql",
    "10_kpi_queries.sql",
    "11_validation_queries.sql",
]

EXPORT_OBJECTS = [
    "mart_component_day",
    "mart_unit_day",
    "mart_fleet_week",
    "vw_component_daily_health",
    "vw_unit_operational_risk",
    "vw_depot_maintenance_pressure",
    "vw_failure_repetition_patterns",
    "vw_condition_based_value",
    "kpi_top_unidades_por_riesgo",
    "kpi_top_componentes_por_criticidad",
    "kpi_top_depositos_por_saturacion",
    "kpi_fallas_repetitivas_mas_frecuentes",
    "kpi_unidades_mayor_indisponibilidad",
    "kpi_backlog_mas_critico",
    "kpi_familias_peor_health_trend",
    "kpi_inspeccion_automatica_por_familia",
    "kpi_valor_potencial_cbm",
    "val_row_counts",
    "val_null_rates_critical",
    "val_sensor_ranges",
    "val_temporal_coherence",
    "val_consistency_scores_actions",
    "val_semantic_health_deterioration",
    "val_backlog_semantic_consistency",
]


def run_sql_layer() -> None:
    DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUTS_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(DUCKDB_PATH))
    try:
        _load_raw_tables(con)
        _run_sql_scripts(con)
        _export_objects(con)
    finally:
        con.close()


def _load_raw_tables(con: duckdb.DuckDBPyConnection) -> None:
    for name in RAW_TABLES:
        path = DATA_RAW_DIR / f"{name}.csv"
        rel_name = f"raw_{name}"
        con.execute(f"DROP TABLE IF EXISTS {rel_name}")
        df = pd.read_csv(path)
        con.register("tmp_df", df)
        con.execute(f"CREATE TABLE {rel_name} AS SELECT * FROM tmp_df")
        con.unregister("tmp_df")


def _run_sql_scripts(con: duckdb.DuckDBPyConnection) -> None:
    for sql_file in SQL_SEQUENCE:
        sql_path = SQL_DIR / sql_file
        script = sql_path.read_text(encoding="utf-8")
        con.execute(script)


def _export_objects(con: duckdb.DuckDBPyConnection) -> None:
    for name in EXPORT_OBJECTS:
        df = con.execute(f"SELECT * FROM {name}").df()
        df.to_csv(DATA_PROCESSED_DIR / f"{name}.csv", index=False)

    # Resumen de arquitectura SQL para trazabilidad
    execution_df = pd.DataFrame(
        {
            "sql_script": SQL_SEQUENCE,
            "execution_order": range(1, len(SQL_SEQUENCE) + 1),
            "dialecto": ["DuckDB"] * len(SQL_SEQUENCE),
        }
    )
    execution_df.to_csv(OUTPUTS_REPORTS_DIR / "sql_execution_manifest.csv", index=False)


if __name__ == "__main__":
    run_sql_layer()
