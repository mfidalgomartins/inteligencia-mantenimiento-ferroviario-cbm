# Production readiness

Checklist práctico para considerar el repositorio listo para una demo avanzada o una entrega interna.

## Validaciones obligatorias
- Ejecutar `./scripts/run_pipeline.sh` desde un entorno limpio.
- Ejecutar `./scripts/run_tests.sh` y confirmar que no hay fallos.
- Ejecutar `./scripts/run_coverage.sh` para revisar cobertura de reglas críticas.
- Confirmar `git diff --check` sin errores de whitespace.
- Verificar que `outputs/dashboard/centro-control-mantenimiento-ferroviario.html` se regenera sin edición manual.

## Contratos de datos
- Las claves primarias declaradas no deben tener duplicados.
- Las columnas obligatorias deben existir y no estar completamente vacías.
- Los rangos semánticos deben mantenerse: `health` alto = mejor, `risk` alto = peor.
- Las métricas narrativas deben estar alineadas entre README, informe, dashboard y `data/processed/narrative_metrics_official.csv`.

## Criterios de publicación
- Los datos son sintéticos y deben etiquetarse como tal en cualquier entrega.
- Los thresholds de mantenimiento requieren calibración antes de uso operacional.
- El RUL debe interpretarse como ventana relativa de intervención, no como fecha exacta de fallo.
- El scheduling es heurístico y no sustituye una optimización formal de capacidad.

