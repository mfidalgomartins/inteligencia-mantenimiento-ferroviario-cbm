# Memo ejecutivo

## Tesis para comité

La plataforma no debe leerse como un dashboard de mantenimiento, sino como un sistema de decisión para proteger disponibilidad. La decisión inmediata es ejecutar una cola de taller basada en riesgo, corregir la asignación de capacidad y gobernar los diferimientos. La inversión en CBM debe pasar por una segunda puerta financiera: el modelo mejora disponibilidad, pero no demuestra ahorro bajo los supuestos actuales.

## Evidencia crítica

| Métrica | Lectura ejecutiva |
|---|---:|
| Disponibilidad media de flota | 95,75% |
| Unidades en cola estadística de alto riesgo | 9 |
| Backlog físico abierto | 2.056 pendientes |
| Backlog vencido | 2.011 pendientes |
| Backlog crítico físico | 1.955 pendientes |
| Casos con alto riesgo de diferimiento | 43 |
| Mejora CBM vs reactiva | +0,93 p.p. |
| Diferencial neto CBM vs reactiva | -48,7 M€ |
| Probabilidad modelada de ahorro positivo CBM | 0,0% |

Marcadores de decisión:

- Unidad prioritaria: UNI0055
- Componente prioritario: COMP000438
- Coste incremental proxy estimado CBM vs reactiva: 48.700.525 EUR

## Decisiones recomendadas

1. Ejecutar primero `UNI0055 / COMP000438`, componente de familia `pantograph`, con score de prioridad 91,4 y riesgo de diferimiento 85,8.
2. Adoptar la programación rediseñada a 35 días como baseline operativo: aumenta accionabilidad y reduce riesgo residual no atendido.
3. Tratar los 43 casos de alto riesgo de diferimiento como cartera ejecutiva, con revisión diaria y excepción documentada.
4. Rebalancear carga entre depósitos antes de solicitar capacidad estructural adicional.
5. No presentar CBM como caso de ahorro hasta recalibrar costes reales, valor de hora de servicio, inventario, capacitación y coste de habilitación.

## Trade-off económico

Diferir 14 días añade 329.610 EUR de coste proxy y 441,0 horas de indisponibilidad frente a intervenir en día cero. CBM preserva servicio frente a reactiva, pero exige valorar cada hora de servicio preservada en al menos 2.618 EUR para compensar su coste incremental proxy. Ese umbral debe validarse con economía real del operador.

## Riesgos y controles

Los datos son sintéticos y los costes son proxies técnico-operativos. Los resultados demuestran arquitectura, lógica de decisión y trazabilidad; no validan precisión externa ni business case contractual. Antes de uso productivo se requiere backtesting temporal, recalibración por familia técnica, revisión de falsos positivos y cierre del circuito alerta-orden-ejecución-resultado.

## Próximos 120 días

- 0-30 días: ejecutar P1/P2, registrar excepciones y estabilizar la heurística de 35 días.
- 31-60 días: rebalancear depósitos y lanzar planes de causa raíz para pantógrafos y fallos repetitivos.
- 61-120 días: recalibrar riesgo, RUL y economía CBM con histórico real y preparar decisión de inversión.
