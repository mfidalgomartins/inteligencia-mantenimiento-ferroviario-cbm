# Memo Ejecutivo

## 1. Contexto
La red ferroviaria analizada opera con alta exigencia de disponibilidad y presión de taller en depósitos críticos.

## 2. Problema
Persisten fallas repetitivas y backlog técnico que elevan el riesgo de indisponibilidad y afectan servicio.

## 3. Enfoque metodológico
La narrativa se alimenta automáticamente desde el registro oficial de métricas,
evitando divergencias entre README, dashboard y reportes de resultados.

## 4. Hallazgos principales
- Disponibilidad media de flota: 95,75%.
- Unidades de alto riesgo: 9.
- Backlog físico: 2.056 pendientes.
- Backlog vencido: 2.011 pendientes.
- Backlog crítico físico: 1.955 pendientes.
- Casos de alto riesgo de diferimiento: 43.
- CBM vs reactiva: mejora de disponibilidad 0,93 p.p.

## 5. Implicaciones operativas
Intervenciones anticipadas en componentes de alto riesgo reducen indisponibilidad y sustituciones no planificadas.

## 6. Implicaciones para taller
El depósito más saturado es DEP08 con 45,2% de ocupación.

## 7. Implicaciones económicas
Coste incremental proxy estimado CBM vs reactiva: € 48.700.525.
Rango plausible del diferencial CBM vs reactiva: -€ 74.235.938 a -€ 44.730.809.
Robustez del ahorro CBM en escenarios y sensibilidades: 0,0% de casos con ahorro positivo.

## 8. Trade-offs principales
Diferir 14 días incrementa coste en 329.610 EUR y downtime en 441,0 h.
Separación conceptual aplicada: backlog físico (cantidad/edad/severidad) y riesgo de diferimiento (score de decisión) se reportan por separado.

## 9. Prioridades de intervención
- Unidad prioritaria: UNI0055.
- Componente prioritario: COMP000438.
- Familia técnica asociada: pantograph.

## 10. Limitaciones
Datos sintéticos y costes proxy; los resultados no sustituyen calibración con datos reales de operación.

## 11. Próximos pasos
Validar umbrales con histórico real, incorporar optimización matemática del scheduling y retroalimentar el modelo con órdenes ejecutadas.
