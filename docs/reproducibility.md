# Runbook de Reproducibilidad

## Objetivo
Ejecutar el proyecto de forma determinista en una máquina limpia y con gates de calidad activos.

## Entorno
Requiere Python 3.12 o superior.
Reserve al menos 4 GB libres para los CSV y artefactos generados localmente.

## Preflight
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

La pipeline genera datos sintéticos, marts, scoring, RUL, priorización, scheduling, documentación derivada y dashboard.

### Orden operativo de la pipeline
1. Generación sintética y auditoría raw.
2. Capa SQL DuckDB y marts.
3. Features, scoring, RUL y alertas.
4. Priorización, scheduling, inspección, estrategia y diferimiento.
5. Sincronización de narrativa, notebooks, contratos y dashboard.

### Outputs esperados
| Artefacto | Ruta |
|-----------|------|
| Dashboard final | `outputs/dashboard/centro-control-mantenimiento-ferroviario.html` |
| Métricas narrativas oficiales | `data/processed/narrative_metrics_official.csv` |
| Contratos de métricas | `data/processed/metric_contract_registry.csv` |
| Contratos de datos | `data/processed/data_contract_registry.csv` |
| Validaciones de governance | `data/processed/governance_contract_checks.csv` |

## Verificación
```bash
./scripts/run_tests.sh
./scripts/run_coverage.sh
git diff --check
```

Publicación recomendada solo si:
1. La pipeline termina sin validaciones de severidad alta fallidas.
2. Lint y suite de tests pasan completos.
3. `scripts/run_tests.sh` no detecta drift entre `requirements-lock.txt` y el entorno activo.
4. `scripts/run_coverage.sh` genera cobertura para pipeline + tests.
5. `python -m pip check` no reporta conflictos de dependencias.
6. `data/processed/narrative_metrics_official.csv` está alineado con README, memo y dashboard.
7. `git diff --check` no detecta errores de formato.

## Seguridad y dependencias
Ver también `docs/security_dependency_hygiene.md` para la política de actualización, auditoría de vulnerabilidades y hardening HTML.

## Diagnóstico rápido
| Síntoma | Revisión práctica |
|---------|-------------------|
| Métricas del README no coinciden | Reejecutar pipeline y revisar `tests/test_reporting_consistency.py`. |
| Dashboard sin datos recientes | Verificar `data/processed/narrative_metrics_official.csv` y `outputs/dashboard/`. |
| Fallo de contrato | Abrir `data/processed/governance_contract_checks.csv` y corregir fuente, PK, rango o columna obligatoria. |
| Diferencias inesperadas entre corridas | Confirmar `requirements-lock.txt`, Python 3.12+ y ausencia de ediciones manuales en outputs generados. |

## Criterio de reproducibilidad aceptado
- Corrida completa sin errores.
- Tests y lint sin fallos.
- Outputs generados por pipeline, no editados a mano.
- Cambios revisables por `git diff`, con diferencias explicables en datos, docs o dashboard.

## Limitaciones conocidas
- Dataset sintético: no permite inferencia causal de producción.
- Capa económica basada en proxies de coste.
- RUL útil como ventana relativa, no como fecha de fallo calibrada.
- Scheduling heurístico, no optimización global matemática.
