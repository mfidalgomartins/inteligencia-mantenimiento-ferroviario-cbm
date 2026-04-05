# Memo Ejecutivo

## 1. Contexto
La red ferroviaria analizada opera con alta exigencia de disponibilidad y presión de taller en depósitos críticos.

## 2. Problema
Persisten fallas repetitivas y backlog técnico que elevan el riesgo de indisponibilidad y afectan servicio.

## 3. Enfoque metodológico
La narrativa se alimenta automáticamente desde métricas oficiales versionadas (SSOT),
evitando divergencias entre README, dashboard y reportes de resultados.

## 4. Hallazgos principales
- Disponibilidad media de flota: 90.45%.
- Unidades de alto riesgo: 0.
- Backlog físico: 2054 pendientes.
- Backlog vencido: 2000 pendientes.
- Backlog crítico físico: 1957 pendientes.
- Casos de alto riesgo de diferimiento: 64.
- CBM vs reactiva: mejora de disponibilidad 2.08 p.p.

## 5. Implicaciones operativas
Intervenciones anticipadas en componentes de alto riesgo reducen indisponibilidad y sustituciones no planificadas.

## 6. Implicaciones para taller
El depósito más saturado es DEP05 con 94.1% de ocupación.

## 7. Implicaciones económicas
Ahorro operativo proxy estimado CBM vs reactiva: 20764476 EUR.
Rango plausible de ahorro CBM vs reactiva: -628418 a 40297095 EUR.
Robustez del ahorro CBM (escenarios+sensitivity): 88.9% de casos con ahorro positivo.

## 8. Trade-offs principales
Diferir 14 días incrementa coste en 388592 EUR y downtime en 464.3 h.
Separación conceptual aplicada: backlog físico (cantidad/edad/severidad) y riesgo de diferimiento (score de decisión) se reportan por separado.

## 9. Prioridades de intervención
- Unidad prioritaria: UNI0057.
- Componente prioritario: COMP000454.
- Familia técnica asociada: pantograph.

## 10. Limitaciones
Datos sintéticos y costes proxy; los resultados no sustituyen calibración con datos reales de operación.

## 11. Próximos pasos
Validar umbrales con histórico real, incorporar optimización matemática de scheduling y cerrar loop con órdenes ejecutadas.