# Estrategia de Notebooks Analíticos

## Objetivo
Convertir los notebooks en artefactos de decisión y defensa técnica, no en outputs decorativos.

## Diseño narrativo
Se estructuran 4 notebooks con una plantilla común:
1. Pregunta analítica.
2. Hipótesis.
3. Metodología.
4. Evidencia cuantitativa.
5. Interpretación operativa.
6. Limitaciones.
7. Decisión resultante.

## Notebooks
1. `notebooks/01_exploracion_y_auditoria.ipynb`
- Foco: calidad, coherencia y readiness.
- Output clave: issues por severidad y bloqueadores.

2. `notebooks/02_degradacion_riesgo_rul.ipynb`
- Foco: señales de deterioro, riesgo y RUL.
- Output clave: discriminación por familia y utilidad de buckets.

3. `notebooks/03_priorizacion_y_scheduling.ipynb`
- Foco: cola de taller, ejecutabilidad y riesgo residual.
- Output clave: secuencia sugerida y estados de programación.

4. `notebooks/04_estrategias_y_diferimiento.ipynb`
- Foco: comparación reactivo/preventivo/CBM + sensibilidad.
- Output clave: rango plausible de valor y downside.

## Alineación con governance
- Fuente oficial: `data/processed/` y `outputs/reports/`.
- Métricas y semántica: `docs/metric_contracts.md`.
- Validación cruzada: `src/validation.py`.

## Criterios de calidad
- Sin celdas de relleno.
- Cada gráfico/tabla debe responder una decisión.
- No se aceptan claims sin evidencia en tablas oficiales.
