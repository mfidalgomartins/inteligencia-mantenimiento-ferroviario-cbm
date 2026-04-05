# Informe Analítico Avanzado

## Bloque Resumen SSOT
- Disponibilidad media (SSOT): 90.45%
- Unidad prioritaria (SSOT): UNI0057
- Componente prioritario (SSOT): COMP000454
- Ahorro CBM vs reactiva (SSOT): 20764476 EUR

## 1. Salud general de la flota
- Insight principal: disponibilidad media de flota en 90.45% con tendencia reciente de -9.68 p.p.
- Evidencia cuantitativa: MTBF medio 16.10 | MTTR medio 6.95
- Lectura operativa: existe margen de mejora focalizando componentes de mayor degradación.
- Lectura estratégica: disponibilidad se sostiene mejor con intervención anticipada y backlog controlado.
- Caveats: métricas derivadas de datos sintéticos, no equivalentes a contrato de servicio real.
- Recomendación: reforzar seguimiento semanal de riesgo por flota y depósito.

## 2. Componentes y subsistemas críticos
- Insight principal: la familia con peor perfil de riesgo es pantograph.
- Evidencia cuantitativa: riesgo medio 0.466 y salud media 42.97.
- Lectura operativa: concentración de fallas potenciales en pocas familias sugiere plan de acción específico.
- Lectura estratégica: invertir en sensórica y reglas por familia mejora retorno de CBM.
- Caveats: la segmentación depende de reglas de mapeo de familia.
- Recomendación: calibrar thresholds por familia wheel/brake/bogie/pantograph.

## 3. Fallo, degradación y alerta temprana
- Insight principal: precisión práctica de alerta temprana 0.406, recall 0.285.
- Evidencia cuantitativa: la señal de degradación + backlog domina el driver de riesgo en top componentes.
- Lectura operativa: las alertas son útiles para priorización, no para reemplazar inspección manual en casos de baja confianza.
- Lectura estratégica: mantener la lógica interpretable facilita adopción por mantenimiento y operaciones.
- Caveats: la precisión varía con umbral elegido y densidad de fallas.
- Recomendación: revisión trimestral de reglas y performance por familia.

## 4. Taller, backlog y priorización
- Insight principal: principal cuello de botella en DEP01.
- Evidencia cuantitativa: pendientes no ejecutables 565 (capacidad=518, repuesto=47, conflicto=0, escalar=0).
- Lectura operativa: secuencia recomendada reduce riesgo de diferimiento en componentes de mayor impacto.
- Lectura estratégica: balancear especialización y saturación de depósitos eleva throughput útil.
- Caveats: la heurística no optimiza globalmente, prioriza robustez y trazabilidad.
- Recomendación: usar la cola sugerida como base diaria y ajustar por restricciones reales.

## 5. Impacto operativo
- Insight principal: intervención temprana evita escalada no lineal de indisponibilidad al diferir acciones críticas.
- Evidencia cuantitativa: coste esperado y downtime crecen monotónicamente en escenarios de diferimiento.
- Lectura operativa: actuar en ventana de 2-7 días para top riesgo reduce sustituciones y cancelaciones.
- Lectura estratégica: el control de diferimiento es palanca directa de calidad de servicio.
- Caveats: proxies económicos dependen de supuestos de coste unitario.
- Recomendación: establecer política de diferimiento máximo por tier de riesgo.

## 6. Implicaciones estratégicas
- Insight principal: CBM aporta mayor valor en familias críticas y depósitos con backlog persistente.
- Evidencia cuantitativa: ahorro operativo proxy CBM vs reactiva = 20764476 EUR.
- Lectura operativa: priorizar digitalización de inspección y reglas en familias con mayor detección pre-falla y menor tasa de falsa alerta.
- Lectura estratégica: transición gradual a CBM con governance de datos y validación periódica.
- Caveats: el business case debe recalibrarse con costos y SLA reales.
- Recomendación: roadmap de 3 olas: pilotos, escalado por familia, despliegue multi-depósito.