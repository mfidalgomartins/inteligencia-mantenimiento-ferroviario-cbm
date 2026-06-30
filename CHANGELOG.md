# Changelog

Todos los cambios destacables de este proyecto se documentan en este fichero.

El formato sigue [Keep a Changelog](https://keepachangelog.com/es-ES/1.1.0/)
y el proyecto se adhiere a [Versionado Semántico](https://semver.org/lang/es/).

## [No publicado]

### Añadido
- Suelo de cobertura (`fail_under = 90`) aplicado en CI: la build falla si la cobertura cae.
- `scripts/check_lock_drift.py`: verificación reutilizable del *drift* del lock de dependencias.
- `CITATION.cff` para que el trabajo sea citable.

### Corregido
- **Determinismo de la pipeline:** la capa SQL fija DuckDB a una sola hebra
  (`SET threads TO 1`). Antes, la sumación en coma flotante multihebra de `AVG`/`SUM`
  variaba entre corridas y cambiaba la `dashboard-signature` en cada ejecución; ahora
  dos corridas consecutivas producen un dashboard byte a byte idéntico. Protegido por
  `tests/test_pipeline_determinism.py`.
- `scripts/run_coverage.sh` ahora genera los gráficos de publicación antes de `pytest`,
  por lo que funciona en un *checkout* limpio (antes dependía de artefactos ya presentes).

### Cambiado
- El pipeline de CI se ejecuta una sola vez bajo cobertura (lint → drift → pipeline+tests+cobertura),
  eliminando una ejecución redundante del pipeline.

## [1.0.0] - 2026-06-23

Primera *release* lista para publicación.

### Añadido
- Pipeline determinista de extremo a extremo: datos sintéticos → SQL por etapas (DuckDB) →
  feature engineering → scoring de riesgo → priorización y scheduling de taller → dashboard.
- Comparativa estratégica CBM vs. preventiva vs. reactiva con análisis de sensibilidad y diferimiento.
- Dashboard HTML autocontenido (offline, claro/oscuro) alimentado por el registro oficial de métricas.
- Pack de gráficos ejecutivos e informe analítico en PDF.
- Gobierno de métricas con contratos y validaciones que bloquean el pipeline ante fallos críticos.
- Documentación técnica: reproducibilidad, arquitectura, supuestos del modelo y diccionarios de datos/métricas.
- Puertas de calidad: `ruff`, `pytest` con cobertura y verificación de *drift* de dependencias.
