# Preparación para producción

## Posición actual

El repositorio implementa un núcleo batch instalable, reproducible y gobernado. Puede validar un snapshot externo y producir recomendaciones auditables, pero la ejecución publicada usa datos sintéticos. Por diseño, la puerta de modelo mantiene el sistema en modo sombra y desactiva toda ejecución automática.

## Capacidades implementadas

- Paquete Python `railway_cbm` con CLI `railway-cbm` y metadatos PEP 621.
- Dos modos de fuente: sintética determinista y snapshot externo prevalidado.
- Contratos de 15 tablas con columnas, UTF-8, fechas, PK y FK.
- Manifiestos de entrada y ejecución con SHA-256, cardinalidad, estado y duración.
- SQL DuckDB con controles bloqueantes de grano, referencias, rangos y reconciliación.
- Validación temporal mensual, calibración rodante sin futuro, ROC AUC, average precision, Brier, ECE y PSI.
- Puerta de despliegue que impide uso autónomo con evidencia insuficiente.
- Planificación heurística y pruebas de estrés de capacidad.
- MILP depósito-semana activado únicamente con saturación persistente ≥85 % y pendientes de capacidad.
- Registro de decisiones estable, revisiones humanas gobernadas, modo sombra y autoejecución desactivada.
- Contratos de datos, métricas y linaje con bloqueo de publicación.
- Dependencias fijadas, `pip check`, `pip-audit`, CI con acciones por SHA y Dependabot.
- Panel autocontenido sin llamadas externas; 19 gráficos y PDF final accesible.

## Evidencia de la ejecución sintética actual

| Control | Resultado |
|---------|-----------|
| Etapas del pipeline | 20/20 correctas |
| Grano componente-día | 729.291 filas, 0 duplicados |
| Bloqueos de gobernanza | 0 |
| Uso autónomo del modelo | bloqueado |
| Motivos de bloqueo | fuente sintética, ROC AUC bajo, deriva PSI alta |
| Puerta MILP | activada por saturación persistente |
| Asignación MILP | 985 asignadas, 167 no asignadas |
| Tolerancia máxima de incumbente MILP | 10 % |
| Decisiones en sombra | 1.152 |
| Decisiones autoejecutables | 0 |

Estos resultados prueban el funcionamiento de los controles, no la validez operacional del modelo.

## Responsabilidades que siguen fuera del repositorio

- Conectores autenticados a sistemas de sensores, EAM/ERP y operación.
- Ingestión incremental, CDC, esquema versionado y retención histórica gobernada.
- Almacén remoto, cifrado, secretos, copias de seguridad y recuperación.
- Orquestador con reintentos, alertas, SLA, métricas de infraestructura y escalado.
- Identidad, autorización por rol y segregación de funciones.
- Integración transaccional con órdenes de trabajo.
- Inventario de repuestos, habilidades por técnico y secuenciación intradía dentro del optimizador.
- Histórico real suficiente para recalibrar y superar la puerta del modelo.

## Criterios para avanzar

### P0 — evidencia real

1. Cargar un histórico autorizado mediante `--source external`.
2. Confirmar cobertura temporal, resultado maduro y calidad de fallos/mantenimiento.
3. Recalibrar umbrales y costes con responsables de Fiabilidad, Operaciones y Finanzas.
4. Exigir que `model_deployment_gate.csv` permita uso controlado antes de cualquier automatización.

### P1 — piloto en sombra

1. Ejecutar en un orquestador y almacenar cada manifiesto.
2. Incorporar revisiones humanas mediante `decision_approvals.csv` o una interfaz equivalente.
3. Comparar recomendaciones con decisiones reales y resultados a 30/60/90 días.
4. Definir SLA, responsables, alertas y plan de reversión.

### P2 — operación controlada

1. Integrar identidad, roles y segregación de aprobación/ejecución.
2. Conectar el registro aprobado con EAM/ERP mediante una interfaz transaccional idempotente.
3. Ampliar el MILP con repuestos, habilidades, ventanas duras y estabilidad de replanificación.
4. Mantener monitorización de discriminación, calibración, deriva y capacidad con bloqueo automático.

No debe avanzarse si la fase anterior carece de responsable, umbral y evidencia de aceptación.

## Verificación local

```bash
./scripts/run_pipeline.sh
./scripts/run_tests.sh
./scripts/run_coverage.sh
python -m pip_audit -r requirements-lock.txt --progress-spinner off
git diff --check
```
