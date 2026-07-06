# Guía de Reproducibilidad

## Objetivo
Ejecutar el proyecto de forma determinista en una máquina limpia y con puertas de calidad activas.

## Entorno
Requiere Python 3.12 o superior.
Reserve al menos 4 GB libres para los CSV y artefactos generados localmente.

## Preparación previa
- Ejecutar desde la raíz del repositorio.
- Usar `requirements-lock.txt` para resultados comparables.
- No editar manualmente `data/processed/`, `outputs/` ni notebooks generados antes de una corrida comparativa.
- Si se quiere usar otro intérprete, definir `PYTHON_BIN=/ruta/python`; los scripts prefieren `.venv/bin/python` si existe.

1. Crear entorno virtual:
```bash
python3 -m venv .venv
```
2. Activar:
```bash
source .venv/bin/activate
```
3. Instalar las versiones validadas:
```bash
python -m pip install -r requirements-lock.txt
```

## Ejecución completa
```bash
./scripts/run_pipeline.sh
```

El flujo genera datos sintéticos, tablas analíticas, puntuación, RUL, priorización, planificación, documentación derivada y panel de control.

### Orden operativo del flujo
1. Generación sintética y auditoría de datos brutos.
2. Capa SQL DuckDB y tablas analíticas.
3. Variables, puntuación, RUL y alertas.
4. Priorización, planificación, inspección, estrategia y diferimiento.
5. Sincronización de métricas ejecutivas, notebooks, contratos y panel de control.

### Salidas esperadas
| Artefacto | Ruta |
|-----------|------|
| Panel de control final | `outputs/dashboard/centro-control-mantenimiento-ferroviario.html` |
| Métricas narrativas oficiales | `data/processed/narrative_metrics_official.csv` |
| Contratos de métricas | `data/processed/metric_contract_registry.csv` |
| Contratos de datos | `data/processed/data_contract_registry.csv` |
| Validaciones de gobernanza | `data/processed/governance_contract_checks.csv` |

## Determinismo byte a byte
El flujo es reproducible byte a byte entre corridas en el mismo entorno: dos
ejecuciones consecutivas producen un panel de control con idéntica `dashboard-signature`.

La fuente de no determinismo más sutil son las agregaciones en coma flotante de
DuckDB (`AVG`/`SUM`): con varias hebras, el orden de sumación varía y filtra ruido
en los últimos dígitos hacia puntuación, métricas y firma. Por eso la capa SQL fija la
conexión a una sola hebra (`SET threads TO 1`, en `src/run_sql_layer.py`). El test
`tests/test_pipeline_determinism.py` protege esta garantía.

Para verificar el determinismo de forma manual:
```bash
./scripts/run_pipeline.sh && cp outputs/dashboard/*.html /tmp/a.html
./scripts/run_pipeline.sh && diff /tmp/a.html outputs/dashboard/*.html && echo "identico-byte-a-byte"
```

## Verificación
```bash
./scripts/run_tests.sh
./scripts/run_coverage.sh
git diff --check
```

Publicación recomendada solo si:
1. El flujo termina sin validaciones de severidad alta fallidas.
2. Lint y suite de tests pasan completos.
3. `scripts/run_tests.sh` no detecta deriva entre `requirements-lock.txt` y el entorno activo.
4. `scripts/run_coverage.sh` genera cobertura para flujo + tests.
5. `python -m pip check` no reporta conflictos de dependencias.
6. `data/processed/narrative_metrics_official.csv` está alineado con README, memo y panel de control.
7. `git diff --check` no detecta errores de formato.

## Seguridad y dependencias
Ver también `docs/security_dependency_hygiene.md` para la política de actualización, auditoría de vulnerabilidades y endurecimiento HTML.

## Diagnóstico rápido
| Síntoma | Revisión práctica |
|---------|-------------------|
| Métricas del README no coinciden | Reejecutar pipeline y revisar `tests/test_reporting_consistency.py`. |
| Panel de control sin datos recientes | Verificar `data/processed/narrative_metrics_official.csv` y `outputs/dashboard/`. |
| Fallo de contrato | Abrir `data/processed/governance_contract_checks.csv` y corregir fuente, PK, rango o columna obligatoria. |
| Diferencias inesperadas entre corridas | Confirmar `requirements-lock.txt`, Python 3.12+ y ausencia de ediciones manuales en salidas generadas. |

## Criterio de reproducibilidad aceptado
- Corrida completa sin errores.
- Tests y lint sin fallos.
- Salidas generadas por el flujo, no editadas a mano.
- Cambios revisables por `git diff`, con diferencias explicables en datos, documentación o panel de control.

## Limitaciones conocidas
- Dataset sintético: no permite inferencia causal de producción.
- Capa económica basada en aproximaciones de coste.
- RUL útil como ventana relativa, no como fecha de fallo calibrada.
- Planificación heurística, no optimización global matemática.
