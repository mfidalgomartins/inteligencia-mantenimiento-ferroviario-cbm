# Lógica de Recomendación Operativa

## Objetivo
Evitar colapso en una acción dominante y forzar una lógica jerárquica interpretable para operación ferroviaria.

## Categorías de acción (componente)
- intervencion_inmediata
- intervencion_proxima_ventana
- inspeccion_prioritaria
- monitorizacion_intensiva
- mantener_bajo_observacion
- no_accion_por_ahora
- escalado_tecnico_revision_manual

## Categorías de decisión (operación/taller)
- intervención inmediata
- intervención en próxima ventana
- inspección prioritaria
- monitorización intensiva
- mantener bajo observación
- no acción por ahora
- escalado técnico/revisión manual

## Tabla de reglas | Capa componente
| rule_id                | accion                           | condicion                                                                  |
|:-----------------------|:---------------------------------|:---------------------------------------------------------------------------|
| R01_inmediata          | intervencion_inmediata           | riesgo muy alto + impacto alto + confianza suficiente                      |
| R02_proxima_ventana    | intervencion_proxima_ventana     | riesgo alto o RUL operativo estrecho sin condición de emergencia inmediata |
| R03_inspeccion         | inspeccion_prioritaria           | señal degradada con confianza baja o conflicto moderado                    |
| R04_monitorizacion     | monitorizacion_intensiva         | riesgo/interacción moderada sin gatillos de intervención                   |
| R05_observacion        | mantener_bajo_observacion        | riesgo bajo con salud aceptable                                            |
| R06_no_accion          | no_accion_por_ahora              | riesgo muy bajo, salud alta, sin alertas ni pendientes                     |
| R07_escalado_conflicto | escalado_tecnico_revision_manual | conflicto señal-riesgo o riesgo alto con baja confianza                    |

## Tabla de reglas | Capa operativa
| rule_id                            | decision                         | condicion                                                                            |
|:-----------------------------------|:---------------------------------|:-------------------------------------------------------------------------------------|
| D01_inmediata                      | intervención inmediata           | urgencia alta + impacto operativo alto + capacidad disponible + confianza suficiente |
| D02_proxima_ventana                | intervención en próxima ventana  | riesgo alto o RUL corto en escenario no emergente                                    |
| D03_inspeccion                     | inspección prioritaria           | incertidumbre alta o degradación sin impacto operacional crítico                     |
| D04_monitorizacion                 | monitorización intensiva         | riesgo medio con seguimiento reforzado                                               |
| D05_observacion                    | mantener bajo observación        | baja criticidad y ventana de seguridad amplia                                        |
| D06_no_accion                      | no acción por ahora              | riesgo mínimo y salud elevada                                                        |
| D07_escalado_conflicto             | escalado técnico/revisión manual | riesgo alto con baja confianza o RUL crítico sin capacidad de taller                 |
| D02B_inmediata_bloqueada_capacidad | intervención en próxima ventana  | urgencia alta pero bloqueo de capacidad/ventana                                      |

## Resolución de conflictos
- Riesgo alto + confianza baja: `escalado_tecnico_revision_manual` / `escalado técnico/revisión manual`.
- Degradación alta + impacto operacional bajo: `inspeccion_prioritaria`.
- RUL bajo + ventana/capacidad inexistente: `intervención en próxima ventana` con marcador de bloqueo de capacidad.

## Ejemplos sintéticos paso a paso
Los casos siguientes muestran entrada clave -> regla disparada -> decisión final:
| unidad_id   | componente_id   | decision_type                   | decision_rule_id                   | recommended_action_initial       |   prob_fallo_30d |   health_score |   component_rul_estimate |   service_impact_score |   deferral_risk_score |   workshop_fit_score | confidence_flag   | decision_rationale                                                                   |
|:------------|:----------------|:--------------------------------|:-----------------------------------|:---------------------------------|-----------------:|---------------:|-------------------------:|-----------------------:|----------------------:|---------------------:|:------------------|:-------------------------------------------------------------------------------------|
| UNI0055     | COMP000438      | intervención inmediata          | D01_inmediata                      | intervencion_inmediata           |        0.902906  |        35.7329 |                        3 |                78.8082 |               85.8073 |              63.5689 | media             | D01_inmediata | riesgo=0.903, rul=3, impacto=78.8, ajuste=63.6                       |
| UNI0055     | COMP000433      | intervención en próxima ventana | D02B_inmediata_bloqueada_capacidad | escalado_tecnico_revision_manual |        0.771251  |        43.738  |                       40 |                78.8082 |               78.3429 |              63.5689 | media             | D02B_inmediata_bloqueada_capacidad | riesgo=0.771, rul=40, impacto=78.8, ajuste=63.6 |
| UNI0074     | COMP000590      | inspección prioritaria          | D03_inspeccion                     | intervencion_proxima_ventana     |        0.403223  |        48.1958 |                       29 |                32.7984 |               54.9476 |              63.5689 | media             | D03_inspeccion | riesgo=0.403, rul=29, impacto=32.8, ajuste=63.6                     |
| UNI0089     | COMP000705      | monitorización intensiva        | D04_monitorizacion                 | monitorizacion_intensiva         |        0.227907  |        68.4159 |                      187 |                39.1679 |               39.9653 |              63.5689 | media             | D04_monitorizacion | riesgo=0.228, rul=187, impacto=39.2, ajuste=63.6                |
| UNI0090     | COMP000716      | mantener bajo observación       | D05_observacion                    | mantener_bajo_observacion        |        0.0545353 |        63.9305 |                       92 |                30.3916 |               36.5719 |              87.3592 | media             | D05_observacion | riesgo=0.055, rul=92, impacto=30.4, ajuste=87.4                    |
| UNI0028     | COMP000220      | no acción por ahora             | D06_no_accion                      | no_accion_por_ahora              |        0.0664179 |        66.8816 |                      111 |                27.8126 |               33.7384 |              87.3592 | media             | D06_no_accion | riesgo=0.066, rul=111, impacto=27.8, ajuste=87.4                     |
