# Arquitectura del Repositorio

## Objetivo
Definir la ruta de ejecución y los artefactos oficiales por capa.

## Ruta oficial por capa
1. Flujo
- Entrada única: `src/run_pipeline.py`.
- Envoltorio operativo: `scripts/run_pipeline.sh`.

2. Dialecto SQL y capa analítica
- Dialecto único: DuckDB SQL.
- Ejecutor único: `src/run_sql_layer.py`.
- Scripts oficiales: `sql/01_*.sql` ... `sql/11_*.sql`.

3. Modelo y puntuación
- Variables: `src/feature_engineering.py`.
- Puntuaciones: `src/risk_scoring.py`, `src/rul_estimation.py`, `src/early_warning.py`.
- Priorización/planificación: `src/workshop_prioritization.py`.

4. Panel de control
- Generador único: `src/build_dashboard.py`.
- Salida final: `outputs/dashboard/centro-control-mantenimiento-ferroviario.html`.
- Publicación GitHub Pages: `index.html` redirige a la salida canónica en `outputs/dashboard/`.

5. Métricas y texto ejecutivo
- Gobierno de métricas ejecutivas: `src/reporting_governance.py`.
- Memo ejecutivo: `docs/memo_ejecutivo_es.md`.

## Flujo de datos
| Capa | Entrada | Salida oficial | Responsable lógico |
|------|---------|----------------|--------------------|
| Datos brutos sintéticos | `src/generate_synthetic_data.py` | `data/raw/*.csv` | generación y plausibilidad |
| SQL analítico | `data/raw/*.csv`, `sql/*.sql` | `data/processed/mart_*`, `vw_*`, `kpi_*` | integración e indicadores base |
| Variables/puntuación | tablas analíticas y señales brutas | `component_day_features.csv`, `scoring_componentes.csv`, `component_rul_estimate.csv` | analítica de fiabilidad |
| Decisión taller | puntuación, RUL, pendientes, capacidad | `workshop_priority_table.csv`, `workshop_scheduling_recommendation.csv` | planificación |
| Texto ejecutivo | salidas procesadas | `narrative_metrics_official.csv`, memorando, README sincronizado | gobernanza analítica |
| Experiencia ejecutiva | registro oficial y tablas canónicas | panel HTML, gráficos, informe | informes |

## Activos clave
- `src/run_sql_layer.py`
- `src/build_dashboard.py`
- `src/reporting_governance.py`
- `src/governance_contracts.py`

## Convenciones
- Gobierno de métricas y datos en `docs/gobierno_metricas.md`.
- Puntos de entrada de ejecución para revisión: `scripts/run_pipeline.sh` y `scripts/run_tests.sh`.
- Configuración operativa centralizada en `src/config.py`.
- Métricas visibles para dirección: consumir registro oficial, no CSV intermedio ad hoc.
- Datos sintéticos y salidas generadas: regenerar por el flujo antes de comparar resultados.

## Contratos obligatorios
- Fuente oficial de cada métrica: `docs/gobierno_metricas.md`.
- Diccionario de datos brutos: `docs/diccionario_datos.md`.
- Diccionario de métricas: `docs/diccionario_metricas.md`.
- Definiciones SQL: `docs/sql_metric_definitions.md`.
- Validaciones materializadas: `data/processed/governance_contract_checks.csv`.

## Puertas de cambio
Antes de publicar o entregar un cambio funcional:
```bash
./scripts/run_pipeline.sh
./scripts/run_tests.sh
git diff --check
```

Un cambio no debe considerarse listo si rompe:
- unicidad de PK o columnas obligatorias en contratos,
- rangos semánticos (`health` alto=mejor, `risk` alto=peor),
- separación entre pendientes físicos y riesgo de diferimiento,
- consistencia entre README, memo, panel de control y `narrative_metrics_official.csv`.

## Ruta de lectura recomendada
1. `README.md`
2. `docs/repo_architecture.md`
3. `src/run_pipeline.py`
4. `docs/reproducibility.md`
5. `docs/gobierno_metricas.md`
6. `docs/production_readiness.md`
