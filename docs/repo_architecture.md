# Arquitectura del Repositorio

## Objetivo
Definir la ruta de ejecución y los artefactos oficiales por capa.

## Ruta oficial por capa
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
- Output final: `outputs/dashboard/centro-control-mantenimiento-ferroviario.html`.
- Publicación GitHub Pages: `index.html` redirige al output canónico en `outputs/dashboard/`.

5. Métricas y narrativa
- Governance de narrativa: `src/reporting_governance.py`.
- Memo ejecutivo: `docs/memo_ejecutivo_es.md`.

## Activos clave
- `src/run_sql_layer.py`
- `src/build_dashboard.py`

## Convenciones
- Gobierno de métricas y datos en `docs/gobierno_metricas.md`.
- Entrypoints de ejecución para reviewers: `scripts/run_pipeline.sh` y `scripts/run_tests.sh`.
- Configuración operativa centralizada en `src/config.py`.

## Ruta de lectura recomendada
1) `README.md`
2) `docs/repo_architecture.md`
3) `src/run_pipeline.py`
4) `docs/gobierno_metricas.md`
