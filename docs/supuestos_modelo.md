# Supuestos del Modelo

## Datos y contexto
- El dato es sintético y calibrado para plausibilidad ferroviaria, no para replicar una red real específica.
- Se simula histórico diario de 24 meses con heterogeneidad por flota, depósito, línea y componente.

## Degradación y sensórica
- La degradación aumenta con edad, ciclos y estrés operativo/contextual.
- La sensórica incorpora ruido y picos (eventos de choque) con relación positiva al deterioro.
- La inspección automática detecta defectos útiles en parte de los casos (no cobertura perfecta).

## Modelado interpretable
- `component_health_score` se basa en reglas aditivas acotadas (0-100).
- `component_failure_risk_score` usa combinación lineal + sigmoide con horizonte de 30 días.
- `component_rul_estimate` combina salud, deterioro, estrés, restauración y umbrales por familia en una aproximación no lineal.
- `unit_unavailability_risk_score` agrega riesgo componente + exposición operativa.

## Priorización y planificación
- La priorización pondera riesgo técnico, impacto de servicio, riesgo de diferimiento y ajuste al taller.
- La planificación usa heurística multiperiodo (35 días), capacidad regular+flexible, arrastre por criticidad y reasignación controlada de depósito.
- La salida distingue estados operativos (`programada`, `programable_proxima_ventana`, `pendiente_repuesto`, `pendiente_capacidad`, `pendiente_conflicto_operativo`, `escalar_decision`).

## Comparación estratégica y economía
- Reactiva/preventiva/CBM se comparan con factores de ajuste explícitos sobre indicadores base.
- Los costes son aproximaciones técnico-operativas; no sustituyen costeo financiero corporativo.

## Limitaciones explícitas
- Resultados útiles para demostración técnica y lógica de decisión, pero no equivalen a un caso de negocio contractual cerrado.
- Cualquier despliegue real requiere recalibración con histórico real, SLA y estructura de costes del operador.
