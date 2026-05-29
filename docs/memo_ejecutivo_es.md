# Memo Ejecutivo

## 1. Contexto
La red ferroviaria analizada opera con alta exigencia de disponibilidad y presión de taller en depósitos críticos.

## 2. Problema
Persisten fallas repetitivas y backlog técnico que elevan el riesgo de indisponibilidad y afectan el nivel de servicio.

## 3. Enfoque metodológico
La narrativa se alimenta automáticamente desde métricas oficiales versionadas (SSOT), evitando divergencias entre README, dashboard y reportes de resultados.

## 4. Hallazgos principales

| Métrica | Valor |
|---------|------:|
| Disponibilidad media de flota | 90,45 % |
| Unidades de alto riesgo (≥ media + 1,5σ) | 11 |
| Backlog físico | 2.054 pendientes |
| Backlog vencido | 2.000 pendientes |
| Backlog crítico físico | 1.957 pendientes |
| Casos de alto riesgo de diferimiento | 64 |
| Mejora de disponibilidad CBM vs reactiva | +2,08 p.p. |

## 5. Implicaciones operativas
Intervenciones anticipadas en componentes de alto riesgo reducen indisponibilidad y sustituyen correctivas no planificadas. El depósito más saturado es DEP05 con un 94,1 % de ocupación.

## 6. Implicaciones económicas
- Ahorro operativo proxy estimado CBM vs reactiva: **€ 20.764.476**
- Rango plausible: de −€ 628.418 a +€ 40.297.095 según parámetros de coste
- Robustez: el 88,9 % de los escenarios de sensibilidad arrojan ahorro positivo

## 7. Coste de diferir
Diferir 14 días incrementa el downtime en 464,3 h y el coste en € 388.592 adicionales.

## 8. Trade-offs principales
Backlog físico (cantidad/antigüedad/severidad) y riesgo de diferimiento (score de decisión) se reportan por separado para evitar confundir volumen con urgencia.

## 9. Prioridades de intervención
- Unidad prioritaria: `UNI0057`
- Componente prioritario: `COMP000454`
- Familia técnica: pantógrafo

## 10. Limitaciones
Datos sintéticos y costes proxy; los resultados no sustituyen calibración con datos reales de operación.

## 11. Próximos pasos
Validar umbrales con histórico real, incorporar optimización matemática de scheduling y cerrar el loop con órdenes ejecutadas.
