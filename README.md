# Sistema de Inteligencia de Mantenimiento Basado en Condición para Flota Ferroviaria

Plataforma de decisión para mantenimiento ferroviario: identifica riesgo de indisponibilidad, prioriza taller y cuantifica el valor operativo del CBM.

## Problema de negocio
La flota pierde disponibilidad por degradación, fallas repetitivas y backlog. La decisión crítica es **qué intervenir primero, qué pasa si se difiere y dónde el CBM aporta valor real**.

## Qué hace el sistema
- Integra operación, sensores, inspección automática y mantenimiento.
- Genera scoring interpretable de salud, riesgo y RUL operativo.
- Prioriza intervenciones y propone secuencias de taller con trade-offs explícitos.
- Compara estrategias (reactiva, preventiva rígida, CBM) con sensibilidad.

## Decisiones que soporta
- Orden de entrada a taller por unidad y componente.
- Qué intervención conviene ahora vs diferir con riesgo controlado.
- Dónde escalar CBM y dónde el impacto es marginal.
- Qué depósitos están saturados y requieren rebalanceo operativo.

## Arquitectura del proyecto
1. Generación sintética y modelo ferroviario.
2. SQL por capas (staging → marts → KPIs).
3. Feature engineering y scoring interpretable.
4. Priorización + scheduling heurístico.
5. Comparativa estratégica y análisis de diferimiento.
6. Dashboard ejecutivo autocontenido.

## Estructura del repositorio
- `src/` lógica de datos, scoring y dashboard
- `sql/` capa SQL (staging, marts, KPIs)
- `data/` (raw/processed ignorado en GitHub)
- `outputs/` dashboard y reportes ejecutivos
- `docs/` documentación clave
- `tests/` validación y QA

## Outputs clave
- `outputs/dashboard/index.html`
- `outputs/reports/informe_analitico_avanzado.md`
- `outputs/reports/memo_ejecutivo_es.md`
- `outputs/reports/validation_report.md`
- `docs/gobierno_metricas.md`

## Por qué este proyecto es fuerte
- Trazabilidad real desde señal técnica → score → decisión.
- Gobernanza de métricas con contratos y checks publish‑blocker.
- Enfoque operativo: priorización y secuenciación, no solo reporting.

## Resultados clave (SSOT)
- disponibilidad media de flota: **90.45%**
- unidades de alto riesgo: **0**
- backlog físico: **2054 pendientes**
- backlog vencido: **2000 pendientes**
- backlog crítico físico: **1957 pendientes**
- casos alto riesgo de diferimiento: **64**

## Decisión actual (SSOT)
- **Unidad que debe entrar primero:** `UNI0057`
- **Componente que debe sustituirse primero:** `COMP000454`

## Cómo ejecutar
```bash
python -m src.run_pipeline
python -m src.build_dashboard
```

## Limitaciones
- Datos sintéticos; requieren calibración real.
- Costes económicos en proxy.
- Scheduling heurístico, no optimizador global.

## Herramientas
Python, SQL, DuckDB, pandas, Chart.js.