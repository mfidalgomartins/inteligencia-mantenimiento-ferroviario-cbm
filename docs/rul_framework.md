# Marco de RUL

## 1) Diagnóstico de la lógica anterior
- La lógica anterior usaba extrapolación lineal y regla de saturación `slope >= -0.02 => RUL=365`.
- Resultado observado: `proporcion_rul_365_anterior = 0.723` (colapso en horizontes amplios).
- Impacto: baja discriminación para priorización, intervención y planificación.

## 2) Definición operativa del nuevo RUL
RUL (`component_rul_estimate`) = días estimados hasta cruzar umbral de condición crítica por familia técnica,
bajo condiciones operativas actuales y degradación efectiva diaria.

### Qué incorpora
- Perfil por familia (`wheel`, `brake`, `bogie`, `pantograph`) con umbral y horizonte máximo específicos.
- Degradación no lineal por deterioro, velocidad de degradación y aceleración de tendencia.
- Estrés operacional y ambiental.
- Restablecimientos parciales por restauración de mantenimiento (sin asumir restablecimiento perfecto).
- Penalización por repetitividad de falla y alertas críticas.
- Banda de confianza (`confidence_rul`, `confidence_flag`) por completitud y estabilidad de señal.

## 3) Cuándo usar y cuándo no usar RUL
### Usar RUL para:
- Dimensionar ventana de intervención (urgente/corta/media/larga).
- Desempatar prioridades entre activos con riesgo similar.
- Ajustar secuencia de taller junto con impacto de servicio y ajuste de depósito.

### No usar RUL como única señal cuando:
- `confidence_flag = baja` o hay conflicto fuerte entre señales (ej. alto riesgo con RUL amplio).
- Existen restricciones operativas duras (ventana/capacidad/repuesto) que dominan la decisión.
- Se requiere causalidad física de fallo por componente (este marco es una aproximación interpretable, no un modelo físico).

## 4) Convivencia con salud y riesgo
- `health_score`: estado actual (alto=mejor).
- `prob_fallo_30d`: probabilidad de fallo a corto plazo.
- `component_rul_estimate`: horizonte temporal de agotamiento bajo condiciones actuales.
- Regla práctica: decisión prioritaria cuando riesgo alto + RUL corto + impacto servicio alto.

## 5) Comparación con la lógica anterior
| metodo                     |   rul_medio |   rul_p10 |   rul_p50 |   rul_p90 |   proporcion_tope_rul |   proporcion_rul_<=30 |
|:---------------------------|------------:|----------:|----------:|----------:|----------------------:|----------------------:|
| lineal_anterior_365        |    318.322  |       162 |       365 |       365 |           0.72309     |           0.000868056 |
| nueva_aproximacion_familia |     73.3377 |        17 |        63 |       146 |           0.000868056 |           0.206597    |

### Discriminación por familia
| familia_componente   |   anterior_p50 |   anterior_p10 |   anterior_p90 |   nuevo_p50 |   nuevo_p10 |   nuevo_p90 |   nuevo_proporcion_<=30 |
|:---------------------|---------------:|---------------:|---------------:|------------:|------------:|------------:|------------------------:|
| pantógrafo           |            365 |          158.8 |            365 |        26   |           3 |        58.4 |               0.555556  |
| freno                |            365 |          170.1 |            365 |        41.5 |           9 |        86.7 |               0.354167  |
| rueda                |            365 |          141.6 |            365 |        54   |          18 |       119.4 |               0.256944  |
| bogie                |            365 |          162   |            365 |        82   |          31 |       162.2 |               0.0972222 |

### Relación con fallas posteriores (validación retrospectiva)
| metodo                     | grupo_rul   |   observaciones |   fallas_30d |   tasa_falla_30d |
|:---------------------------|:------------|----------------:|-------------:|-----------------:|
| lineal_anterior_365        | 01_15_30    |              25 |            3 |         0.12     |
| lineal_anterior_365        | 02_31_60    |             177 |           28 |         0.158192 |
| lineal_anterior_365        | 03_61_90    |             361 |           43 |         0.119114 |
| lineal_anterior_365        | 04_91_180   |            1451 |          208 |         0.143349 |
| lineal_anterior_365        | 05_>180     |           11207 |         1555 |         0.138753 |
| nueva_aproximacion_familia | 00_<=14     |            1261 |          194 |         0.153846 |
| nueva_aproximacion_familia | 01_15_30    |            1777 |          259 |         0.145751 |
| nueva_aproximacion_familia | 02_31_60    |            3599 |          516 |         0.143373 |
| nueva_aproximacion_familia | 03_61_90    |            2716 |          396 |         0.145803 |
| nueva_aproximacion_familia | 04_91_180   |            3377 |          418 |         0.123779 |
| nueva_aproximacion_familia | 05_>180     |             491 |           54 |         0.10998  |

## 6) Validaciones específicas de RUL
| check_id                        | severidad   | aprobado   |   valor_metrica | umbral                          | detalle                                                    |
|:--------------------------------|:------------|:-----------|----------------:|:--------------------------------|:-----------------------------------------------------------|
| rul_distribution_not_saturated  | alta        | sí         |     0.000868056 | <=0.60                          | proporción en techo de RUL                                 |
| rul_distribution_spread         | alta        | sí         |   129           | >=55 días                       | amplitud P90-P10                                           |
| rul_family_discrimination       | media       | sí         |    56           | >=12 días                       | diferencia medianas entre familias                         |
| rul_confidence_entropy          | media       | sí         |     0.917425    | >=0.65                          | entropía de confidence_flag                                |
| rul_failure_linkage_direction   | media       | sí         |    -0.0289239   | <=-0.02                         | control direccional; asociación esperada negativa          |
| rul_failure_quantile_separation | alta        | sí         |     0.0337573   | >=0.02; soporte >=500 por grupo | tasa_falla(Q1 RUL) - tasa_falla(Q4 RUL); soporte=3424/3309 |

## 7) Integración con recomendación y planificación
- `component_rul_estimate` y `confidence_rul` alimentan `assign_operational_decisions` y `workshop_priority_table`.
- RUL corto empuja decisiones de `intervención inmediata` / `próxima ventana` según conflicto de capacidad.
- RUL amplio con riesgo bajo permite `observación` / `no acción` con menor presión de taller.

## 8) Limitaciones
- Datos sintéticos: requiere recalibración con histórico real antes de despliegue operativo.
- El módulo no sustituye modelos físicos de desgaste por fabricante.
- La asociación con fallo a 30 días es direccional pero débil; usar RUL como ventana relativa, no como fecha de fallo calibrada.
- `days_since_last_maintenance` puede ser escaso en el sintético; se compensa con índices de restauración/frecuencia.
