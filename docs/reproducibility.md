# Guía de reproducibilidad

## Entorno

- Python 3.12 o superior.
- Al menos 4 GB libres para datos y artefactos generados.
- Ejecución desde la raíz del repositorio.
- `data/raw/` y `data/processed/` son áreas gestionadas: no guardar allí fuentes originales.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-lock.txt
python -m pip install --no-deps -e .
```

El lock contiene el entorno validado completo. La instalación editable registra el comando `railway-cbm` sin resolver versiones distintas.

## Fuente sintética

La ejecución oficial también construye gráficos e informe:

```bash
./scripts/run_pipeline.sh
```

Para ejecutar sólo el núcleo analítico y el panel:

```bash
railway-cbm run --source synthetic --seed 42
```

## Snapshot externo

La fuente debe estar fuera de `data/raw/` y contener las 15 tablas canónicas documentadas en `src/railway_cbm/ingestion.py`.

```bash
railway-cbm validate-input --input-dir /ruta/al/snapshot
railway-cbm run --source external --input-dir /ruta/al/snapshot
```

La validación ocurre antes de limpiar la ejecución anterior. Se comprueban UTF-8, columnas, fechas, PK y FK. Si existe `decision_approvals.csv`, también se validan estados, revisor, timestamp e identidad del evento.

## Orden del flujo

1. Preflight de fuente externa, cuando procede.
2. Limpieza controlada y carga/generación de datos.
3. Manifiesto de entrada con SHA-256, filas, columnas y bytes.
4. Auditoría de datos y capa SQL DuckDB.
5. Variables, score de riesgo, validación temporal, RUL y alertas.
6. Priorización, planificación, diagnóstico de capacidad y MILP condicionado.
7. Registro de decisiones en sombra y revisiones humanas.
8. Inspección, estrategia, diferimiento y métricas ejecutivas.
9. Contratos de gobernanza y panel de control.
10. Manifiesto de ejecución con estado y duración por etapa.

## Salidas técnicas clave

| Artefacto | Ruta |
|-----------|------|
| Linaje de entrada | `data/processed/input_data_manifest.csv` |
| Ejecución por etapa | `data/processed/pipeline_execution_manifest.csv` |
| Validación temporal | `data/processed/risk_temporal_validation.csv` |
| Puerta de modelo | `data/processed/model_deployment_gate.csv` |
| Deriva | `data/processed/feature_drift_report.csv` |
| Puerta de capacidad | `data/processed/capacity_optimization_gate.csv` |
| Asignación formal | `data/processed/formal_capacity_allocation.csv` |
| Registro de decisiones | `data/processed/decision_register.csv` |
| Contratos | `data/processed/governance_contract_checks.csv` |
| Panel final | `outputs/dashboard/centro-control-mantenimiento-ferroviario.html` |

## Determinismo

La fuente sintética, las transformaciones y el payload del panel son deterministas para una semilla y un entorno fijados. DuckDB usa una sola hebra para que el orden de agregación en coma flotante no cambie la firma del panel (`SET threads TO 1` en `src/railway_cbm/run_sql_layer.py`).

`pipeline_execution_manifest.csv` es deliberadamente observable, no byte-estable: contiene timestamps y duraciones reales. Esta excepción no afecta métricas ni decisiones.

```bash
railway-cbm run --source synthetic --seed 42
first=$(shasum -a 256 outputs/dashboard/*.html)
railway-cbm run --source synthetic --seed 42
second=$(shasum -a 256 outputs/dashboard/*.html)
test "$first" = "$second"
```

## Puertas de calidad

```bash
./scripts/run_tests.sh
./scripts/run_coverage.sh
python -m pip_audit -r requirements-lock.txt --progress-spinner off
git diff --check
```

La entrega requiere:

1. Pipeline completo sin etapas fallidas.
2. Cero bloqueos de gobernanza.
3. Lint, formato y suite completos.
4. Lock alineado con manifiestos y entorno.
5. Compatibilidad y auditoría de dependencias sin incidencias.
6. Cobertura end-to-end ≥90 %.
7. Un único HTML, 19 gráficos y un único PDF.

## Diagnóstico

| Síntoma | Artefacto de diagnóstico |
|---------|--------------------------|
| Fuente rechazada | salida de `railway-cbm validate-input` |
| Etapa fallida | `pipeline_execution_manifest.csv` |
| PK, columna o fuente no conforme | `governance_contract_checks.csv` |
| Modelo bloqueado | `model_readiness_assessment.csv` y `model_deployment_gate.csv` |
| Deriva alta | `feature_drift_report.csv` |
| Saturación | `capacity_optimization_gate.csv` y `capacity_stress_scenarios.csv` |
| Decisión sin revisión | `decision_review_register.csv` |

## Límites

- Los artefactos publicados usan la fuente sintética; no constituyen evidencia operacional.
- Costes y ahorros son aproximaciones de escenario.
- RUL es una ventana relativa, no una fecha física calibrada.
- El MILP asigna capacidad depósito-semana; repuestos, habilidades y secuencia diaria permanecen fuera del solver.
