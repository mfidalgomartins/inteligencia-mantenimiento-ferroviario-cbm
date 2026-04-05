# RUL Framework

## 1) Diagnóstico de la lógica anterior
- La lógica legacy usaba extrapolación lineal y regla de saturación `slope >= -0.02 => RUL=365`.
- Resultado observado: `share_rul_365_legacy = 0.993` (colapso en horizontes amplios).
- Impacto: baja discriminación para priorización, intervención y scheduling.

## 2) Definición operativa del nuevo RUL
RUL (`component_rul_estimate`) = días estimados hasta cruzar umbral de condición crítica por familia técnica,
bajo condiciones operativas actuales y degradación efectiva diaria.

### Qué incorpora
- Perfil por familia (`wheel`, `brake`, `bogie`, `pantograph`) con umbral y horizonte máximo específicos.
- Degradación no lineal por deterioro, velocidad de degradación y aceleración de tendencia.
- Estrés operacional y ambiental.
- Resets parciales por restauración de mantenimiento (sin asumir reset perfecto).
- Penalización por repetitividad de falla y alertas críticas.
- Banda de confianza (`confidence_rul`, `confidence_flag`) por completitud y estabilidad de señal.

## 3) Cuándo usar y cuándo no usar RUL
### Usar RUL para:
- Dimensionar ventana de intervención (urgente/corta/media/larga).
- Desempatar prioridades entre activos con riesgo similar.
- Ajustar secuencia de taller junto con impacto de servicio y fit de depósito.

### No usar RUL como única señal cuando:
- `confidence_flag = baja` o hay conflicto fuerte entre señales (ej. alto riesgo con RUL amplio).
- Existen restricciones operativas duras (ventana/capacidad/repuesto) que dominan la decisión.
- Se requiere causalidad física de fallo por componente (este marco es proxy interpretable, no modelo físico).

## 4) Convivencia con health y risk
- `health_score`: estado actual (alto=mejor).
- `prob_fallo_30d`: probabilidad de fallo a corto plazo.
- `component_rul_estimate`: horizonte temporal de agotamiento bajo condiciones actuales.
- Regla práctica: decisión prioritaria cuando riesgo alto + RUL corto + impacto servicio alto.

## 5) Before / After
| metodo              |   mean_rul |   p10_rul |   p50_rul |   p90_rul |   share_rul_cap |   share_rul_<=30 |
|:--------------------|-----------:|----------:|----------:|----------:|----------------:|-----------------:|
| legacy_lineal_365   |   364.231  |       365 |       365 |       365 |     0.993056    |          0       |
| nuevo_proxy_familia |    60.3255 |        13 |        54 |       117 |     0.000868056 |          0.27691 |

### Discriminación por familia
| component_family   |   legacy_p50 |   legacy_p10 |   legacy_p90 |   new_p50 |   new_p10 |   new_p90 |   new_share_le_30 |
|:-------------------|-------------:|-------------:|-------------:|----------:|----------:|----------:|------------------:|
| pantograph         |          365 |          365 |          365 |      17   |       3   |      44   |          0.722222 |
| brake              |          365 |          365 |          365 |      25.5 |       8.3 |      65.7 |          0.548611 |
| wheel              |          365 |          365 |          365 |      45   |      20   |      81.8 |          0.263889 |
| bogie              |          365 |          365 |          365 |      73   |      25   |     129.1 |          0.136111 |

### Relación con fallas posteriores (backtest)
| method              | rul_bucket   |   observations |   failures_30d |   failure_rate_30d |
|:--------------------|:-------------|---------------:|---------------:|-------------------:|
| legacy_lineal_365   | 00_<=14      |              1 |              1 |           1        |
| legacy_lineal_365   | 01_15_30     |              9 |              2 |           0.222222 |
| legacy_lineal_365   | 02_31_60     |             22 |             10 |           0.454545 |
| legacy_lineal_365   | 03_61_90     |             24 |              6 |           0.25     |
| legacy_lineal_365   | 04_91_180    |             91 |             23 |           0.252747 |
| legacy_lineal_365   | 05_>180      |          13074 |           5239 |           0.400719 |
| nuevo_proxy_familia | 00_<=14      |           1557 |            650 |           0.417469 |
| nuevo_proxy_familia | 01_15_30     |           2142 |            862 |           0.402428 |
| nuevo_proxy_familia | 02_31_60     |           4137 |           1710 |           0.413343 |
| nuevo_proxy_familia | 03_61_90     |           2944 |           1182 |           0.401495 |
| nuevo_proxy_familia | 04_91_180    |           2428 |            875 |           0.360379 |
| nuevo_proxy_familia | 05_>180      |             13 |              2 |           0.153846 |

## 6) Validaciones específicas de RUL
| check_id                       | severity   | passed   |   metric_value | threshold   | detail                                           |
|:-------------------------------|:-----------|:---------|---------------:|:------------|:-------------------------------------------------|
| rul_distribution_not_saturated | alta       | True     |    0.000868056 | <=0.60      | share en techo de RUL                            |
| rul_distribution_spread        | alta       | True     |  104           | >=55 días   | amplitud P90-P10                                 |
| rul_family_discrimination      | media      | True     |   56           | >=12 días   | diferencia medianas entre familias               |
| rul_confidence_entropy         | media      | True     |    0.970543    | >=0.65      | entropía de confidence_flag                      |
| rul_failure_linkage_direction  | alta       | False    |   -0.0328041   | <=-0.08     | correlación RUL vs falla_30d (esperada negativa) |
| rul_failure_bucket_separation  | alta       | True     |    0.254913    | >=0.02      | failure_rate(<=30d) - failure_rate(>180d)        |

## 7) Integración con recomendación y scheduling
- `component_rul_estimate` y `confidence_rul` alimentan `assign_operational_decisions` y `workshop_priority_table`.
- RUL corto empuja decisiones de `intervención inmediata` / `próxima ventana` según conflicto de capacidad.
- RUL amplio con riesgo bajo permite `observación` / `no acción` con menor presión de taller.

## 8) Limitaciones
- Datos sintéticos: requiere recalibración con histórico real antes de despliegue operativo.
- El módulo no sustituye modelos físicos de desgaste por fabricante.
- `days_since_last_maintenance` puede ser escaso en el sintético; se compensa con índices de restauración/frecuencia.