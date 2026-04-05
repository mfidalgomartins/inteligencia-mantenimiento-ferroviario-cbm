# Sistema de Inteligencia de Mantenimiento Basado en Condición, Riesgo Operativo y Priorización de Taller para Flota Ferroviaria

Plataforma analítica end-to-end orientada a material rodante, fiabilidad de activos y decisión operativa de taller en un contexto tipo CAF + ecosistema digital de condition monitoring.

## Executive Overview
Este proyecto responde una pregunta central de dirección de mantenimiento y operaciones: **qué componentes/unidades elevan el riesgo de indisponibilidad, en qué orden intervenir, qué pasa si se difiere la acción y cuánto valor genera CBM frente a estrategias tradicionales**.

## Project Architecture
1. `generate_synthetic_data.py`: ecosistema de tablas operativas/técnicas.
2. `run_sql_layer.py`: staging + integration + marts + KPIs + validación SQL.
3. `feature_engineering.py`: features multigranular para degradación, riesgo y priorización.
4. `risk_scoring.py` + `rul_estimation.py` + `early_warning.py`: scoring interpretable y RUL.
5. `workshop_prioritization.py`: priorización y scheduling heurístico.
6. `strategy_comparison.py` + `impact_analysis.py`: comparación de estrategia e impacto por diferimiento.
7. `inspection_module.py`: inspección automática wheel/brake/bogie/pantograph.
8. `build_dashboard.py`: dashboard HTML ejecutivo.
9. `reporting.py` + `reporting_governance.py`: sincronización narrativa automática.
10. `validation.py`: QA por capas con publish-blockers.

## What This Project Demonstrates
- Criterio de negocio en mantenimiento ferroviario basado en condición.
- Capacidad de pasar de señales técnicas a decisiones operativas.
- Gobernanza de reporting: una sola fuente de verdad para narrativa.

## Business Skills Demonstrated
- Priorización técnico-operativa por riesgo e impacto.
- Cuantificación de trade-offs de diferimiento.
- Traducción ejecutiva de resultados para mantenimiento y operaciones.

## Technical Skills Demonstrated
- Python modular reproducible.
- SQL analítico por capas (DuckDB).
- Feature engineering y scoring interpretable.
- QA framework con controles de coherencia semántica y cross-output.

## Questions This System Helps Answer
- ¿Qué unidad debe entrar primero a taller?
- ¿Qué componente explica el riesgo y debe intervenirse primero?
- ¿Cuál es el coste de diferir la acción?
- ¿Cuánto valor añade CBM frente a reactivo/preventivo rígido?

## Why This Is Relevant for a Company Like CAF
- Enfoca disponibilidad contractual, fiabilidad de activos y presión de taller.
- Integra inspección automática con decisión operativa, no solo reporting.
- Mantiene trazabilidad de métrica técnica a recomendación de intervención.

## Key Findings (última corrida)
- disponibilidad media de flota: **90.45%**
- unidades de alto riesgo: **0**
- backlog físico: **2054 pendientes**
- backlog vencido: **2000 pendientes**
- backlog crítico físico: **1957 pendientes**
- casos alto riesgo de diferimiento: **64**
- CBM vs reactiva: **+2.08 p.p.** en disponibilidad proxy
- ahorro operativo proxy CBM vs reactiva: **~20.76M EUR**
- rango plausible de ahorro CBM vs reactiva (P10-P90): **~-0.63M a 40.30M EUR**
- robustez del caso CBM (simulaciones con ahorro positivo): **88.9%**
- horas potencialmente evitables por inspección automática: **~5376 h**

## Recommendations
1. Gestionar backlog físico con KPIs separados (`físico`, `vencido`, `crítico físico`) por depósito.
2. Gestionar riesgo de diferimiento con `deferral_risk_score` como dimensión distinta (no backlog).
3. Priorizar la cola de taller por `intervention_priority_score` y `deferral_risk_score`.
4. Escalar CBM en familias con mayor riesgo y evidencia pre-falla.
5. Controlar diferimientos >14 días por su escalada de coste/downtime.
6. Rebalancear depósitos cuando la saturación local excede umbrales operativos.

## Validation Approach
- Controles por capa: raw, staging, marts, features, scores, recomendaciones, dashboard y docs.
- Checks de saturación, entropía, monotonicidad, consistencia semántica y cross-output.
- Bloqueo de publicación cuando fallan controles críticos/alta severidad configurados como blocker.

## Limitations
- Datos sintéticos; no sustituyen calibración contractual real.
- Costes económicos en proxy.
- Scheduling heurístico, no optimizador global exacto.

## Next Steps
1. Calibrar umbrales y costes con histórico real.
2. Introducir optimización matemática de secuenciación.
3. Cerrar loop con órdenes ejecutadas y outcomes reales.

## Decisión Final (respuesta explícita)
- **Unidad que debe entrar primero:** `UNI0057`
- **Componente que debe sustituirse primero:** `COMP000454` (familia `pantograph`)
- **Impacto operativo de intervenir ahora:** reducción esperada de downtime frente a diferir 14 días (~464.3 h).
- **Riesgo de retrasar la intervención:** incremento de coste proxy de ~0.39M EUR a 14 días.
- **Cuándo CBM genera más valor:** cuando hay saturación de depósito alta (actualmente DEP05 con 94.1%) y señales tempranas accionables.
- **Nota de gobierno de KPI:** backlog crítico del README se refiere a backlog físico (edad/severidad), no a riesgo de diferimiento.