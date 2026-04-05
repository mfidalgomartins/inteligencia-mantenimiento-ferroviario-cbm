# Recommendation Decision Logic

## Objetivo
Evitar colapso en una acción dominante y forzar una lógica jerárquica interpretable para operación ferroviaria.

## Categorías de acción (componente)
- intervencion_inmediata
- intervencion_proxima_ventana
- inspeccion_prioritaria
- monitorizacion_intensiva
- mantener_bajo_observacion
- no_accion_por_ahora
- escalado_tecnico_manual_review

## Categorías de decisión (operación/taller)
- intervención inmediata
- intervención en próxima ventana
- inspección prioritaria
- monitorización intensiva
- mantener bajo observación
- no acción por ahora
- escalado técnico/manual review

## Tabla de reglas | Capa componente
| rule_id                | accion                         | condicion                                                                  |
|:-----------------------|:-------------------------------|:---------------------------------------------------------------------------|
| R01_inmediata          | intervencion_inmediata         | riesgo muy alto + impacto alto + confianza suficiente                      |
| R02_proxima_ventana    | intervencion_proxima_ventana   | riesgo alto o RUL operativo estrecho sin condición de emergencia inmediata |
| R03_inspeccion         | inspeccion_prioritaria         | señal degradada con confianza baja o conflicto moderado                    |
| R04_monitorizacion     | monitorizacion_intensiva       | riesgo/interacción moderada sin gatillos de intervención                   |
| R05_observacion        | mantener_bajo_observacion      | riesgo bajo con salud aceptable                                            |
| R06_no_accion          | no_accion_por_ahora            | riesgo muy bajo, salud alta, sin alertas ni backlog                        |
| R07_escalado_conflicto | escalado_tecnico_manual_review | conflicto señal-riesgo o riesgo alto con baja confianza                    |

## Tabla de reglas | Capa operativa
| rule_id                            | decision                        | condicion                                                                            |
|:-----------------------------------|:--------------------------------|:-------------------------------------------------------------------------------------|
| D01_inmediata                      | intervención inmediata          | urgencia alta + impacto operativo alto + capacidad disponible + confianza suficiente |
| D02_proxima_ventana                | intervención en próxima ventana | riesgo alto o RUL corto en escenario no emergente                                    |
| D03_inspeccion                     | inspección prioritaria          | incertidumbre alta o degradación sin impacto operacional crítico                     |
| D04_monitorizacion                 | monitorización intensiva        | riesgo medio con seguimiento reforzado                                               |
| D05_observacion                    | mantener bajo observación       | baja criticidad y ventana de seguridad amplia                                        |
| D06_no_accion                      | no acción por ahora             | riesgo mínimo y salud elevada                                                        |
| D07_escalado_conflicto             | escalado técnico/manual review  | riesgo alto con baja confianza o RUL crítico sin capacidad de taller                 |
| D02B_inmediata_bloqueada_capacidad | intervención en próxima ventana | urgencia alta pero bloqueo de capacidad/ventana                                      |

## Resolución de conflictos
- Riesgo alto + confianza baja: `escalado_tecnico_manual_review` / `escalado técnico/manual review`.
- Degradación alta + impacto operacional bajo: `inspeccion_prioritaria`.
- RUL bajo + ventana/capacidad inexistente: `intervención en próxima ventana` con marcador de bloqueo de capacidad.

## Ejemplos sintéticos paso a paso
Los casos siguientes muestran input clave -> regla disparada -> decisión final:
| unidad_id   | componente_id   | decision_type                   | decision_rule_id                   | recommended_action_initial   |   prob_fallo_30d |   health_score |   component_rul_estimate |   service_impact_score |   deferral_risk_score |   workshop_fit_score | confidence_flag   | decision_rationale                                                             |
|:------------|:----------------|:--------------------------------|:-----------------------------------|:-----------------------------|-----------------:|---------------:|-------------------------:|-----------------------:|----------------------:|---------------------:|:------------------|:-------------------------------------------------------------------------------|
| UNI0057     | COMP000454      | intervención en próxima ventana | D02B_inmediata_bloqueada_capacidad | inspeccion_prioritaria       |        0.277053  |        47.9718 |                       24 |                76.9347 |               64.3879 |              59.6173 | media             | D02B_inmediata_bloqueada_capacidad | risk=0.277, rul=24, impact=76.9, fit=59.6 |
| UNI0143     | COMP001143      | intervención inmediata          | D01_inmediata                      | intervencion_inmediata       |        0.88587   |        37.5395 |                       10 |                78.3377 |               80.825  |              59.6173 | baja              | D01_inmediata | risk=0.886, rul=10, impact=78.3, fit=59.6                      |
| UNI0021     | COMP000168      | monitorización intensiva        | D04_monitorizacion                 | monitorizacion_intensiva     |        0.379943  |        48.8525 |                       77 |                36.3306 |               48.7742 |              86.2859 | media             | D04_monitorizacion | risk=0.38, rul=77, impact=36.3, fit=86.3                  |
| UNI0051     | COMP000401      | inspección prioritaria          | D03_inspeccion                     | inspeccion_prioritaria       |        0.450042  |        48.12   |                       66 |                27.4633 |               52.2851 |              59.6173 | media             | D03_inspeccion | risk=0.45, rul=66, impact=27.5, fit=59.6                      |
| UNI0023     | COMP000180      | mantener bajo observación       | D05_observacion                    | mantener_bajo_observacion    |        0.15319   |        51.1078 |                       49 |                31.0319 |               41.305  |              81.2813 | alta              | D05_observacion | risk=0.153, rul=49, impact=31.0, fit=81.3                    |
| UNI0112     | COMP000892      | no acción por ahora             | D06_no_accion                      | no_accion_por_ahora          |        0.0670316 |        54.6585 |                       76 |                23.1954 |               34.0941 |              81.2813 | media             | D06_no_accion | risk=0.067, rul=76, impact=23.2, fit=81.3                      |