# Arquitectura del Repositorio

## Objetivo
Definir la ruta de ejecución y los artefactos oficiales por capa.

## Ruta oficial por capa
1. Pipeline
- Entrada única: `src/run_pipeline.py`.
- Wrapper operativo: `scripts/run_pipeline.sh`.

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

## Flujo de datos
| Capa | Entrada | Salida oficial | Responsable lógico |
|------|---------|----------------|--------------------|
| Raw sintético | `src/generate_synthetic_data.py` | `data/raw/*.csv` | generación y plausibilidad |
| SQL analítico | `data/raw/*.csv`, `sql/*.sql` | `data/processed/mart_*`, `vw_*`, `kpi_*` | integración y KPIs base |
| Features/scoring | marts y señales raw | `component_day_features.csv`, `scoring_componentes.csv`, `component_rul_estimate.csv` | reliability analytics |
| Decisión taller | scoring, RUL, backlog, capacidad | `workshop_priority_table.csv`, `workshop_scheduling_recommendation.csv` | planificación y scheduling |
| Narrativa | outputs procesados | `narrative_metrics_official.csv`, memo, README sincronizado | analytics governance |
| Experiencia ejecutiva | registro oficial y tablas canónicas | dashboard HTML, gráficos, informe | reporting |

## Activos clave
- `src/run_sql_layer.py`
- `src/build_dashboard.py`
- `src/reporting_governance.py`
- `src/governance_contracts.py`

## Convenciones
- Gobierno de métricas y datos en `docs/gobierno_metricas.md`.
- Entrypoints de ejecución para reviewers: `scripts/run_pipeline.sh` y `scripts/run_tests.sh`.
- Configuración operativa centralizada en `src/config.py`.
- Métricas visibles para dirección: consumir registro oficial, no CSV intermedio ad hoc.
- Datos sintéticos y outputs generados: regenerar por pipeline antes de comparar resultados.

## Contratos obligatorios
- Fuente oficial de cada métrica: `docs/gobierno_metricas.md`.
- Diccionario de datos raw: `docs/diccionario_datos.md`.
- Diccionario de métricas: `docs/diccionario_metricas.md`.
- Definiciones SQL: `docs/sql_metric_definitions.md`.
- Checks materializados: `data/processed/governance_contract_checks.csv`.

## Gates de cambio
Antes de publicar o entregar un cambio funcional:
```bash
./scripts/run_pipeline.sh
./scripts/run_tests.sh
git diff --check
```

Un cambio no debe considerarse listo si rompe:
- unicidad de PK o columnas obligatorias en contratos,
- rangos semánticos (`health` alto=mejor, `risk` alto=peor),
- separación entre backlog físico y riesgo de diferimiento,
- consistencia entre README, memo, dashboard y `narrative_metrics_official.csv`.

## Ruta de lectura recomendada
1. `README.md`
2. `docs/repo_architecture.md`
3. `src/run_pipeline.py`
4. `docs/reproducibility.md`
5. `docs/gobierno_metricas.md`
6. `docs/production_readiness.md`
