# Arquitectura del Repositorio (Source of Truth)

## Objetivo
Eliminar arquitectura paralela y dejar una única ruta activa por capa.

## Source of Truth oficial
1. Pipeline
- Entrada única: `src/run_pipeline.py`.

2. SQL dialect y capa analítica
- Dialecto único: DuckDB SQL.
- Runner único: `src/run_sql_layer.py`.
- Scripts oficiales: `sql/01_*.sql` ... `sql/11_*.sql`.

3. Modelo y scoring
- Features: `src/feature_engineering.py`.
- Scores: `src/risk_scoring.py`, `src/rul_estimation.py`, `src/early_warning.py`.
- Priorización/scheduling: `src/workshop_prioritization.py`.

4. Dashboard
- Generador único: `src/build_dashboard.py`.
- Outputs: `outputs/dashboard/index.html` + `centro-control-mantenimiento-ferroviario.html`.
- Publicación GitHub Pages: `docs/index.html`.

5. Reporting
- Governance de narrativa: `src/reporting_governance.py`.
- Artefactos ejecutivos: `src/reporting.py`.

## Activos clave
- `src/explore_data_audit.py`
- `src/run_sql_layer.py`
- `src/validation.py`
- `src/build_dashboard.py`

## Convenciones
- Gobierno de métricas y datos en `docs/gobierno_metricas.md`.
- Validación publish-blocker en `outputs/reports/publish_blockers.csv`.
- Entrypoints de ejecución para reviewers: `scripts/run_pipeline.sh` y `scripts/run_tests.sh`.
- Configuración operativa viva en `src/config.py`; carpeta `configs/` reservada para futura externalización declarativa.

## Criterio de revisión externa
Un reviewer debe poder entender el repo en <10 minutos siguiendo:
1) `README.md`
2) `docs/repo_architecture.md`
3) `src/run_pipeline.py`
4) `docs/gobierno_metricas.md`
