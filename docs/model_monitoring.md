# Validación temporal y monitorización del riesgo

- Fuente evaluada: `synthetic`.
- Uso autónomo permitido: `no`.
- Ventana de resultado: 30 días.
- Esquema: cortes mensuales; cada calibrador sólo usa cortes anteriores con resultado ya maduro.
- Discriminación: ROC AUC, average precision y lift del decil superior.
- Calibración: Brier y error de calibración fuera de muestra.
- Deriva: PSI entre los primeros y últimos 90 días disponibles.

## Puerta de despliegue

| check                           | status   | observed             | threshold   | blocks_autonomous_use   |
|:--------------------------------|:---------|:---------------------|:------------|:------------------------|
| external_historical_source      | failed   | synthetic            | external    | True                    |
| mature_temporal_folds           | passed   | 24                   | >=6         | False                   |
| observed_failures               | passed   | 2937                 | >=30        | False                   |
| discrimination_roc_auc          | failed   | 0.514171716850203    | >=0.65      | True                    |
| out_of_sample_calibration_error | passed   | 0.028556890316952632 | <=0.10      | False                   |
| feature_drift_psi               | failed   | 3.0923477540814828   | <0.25       | True                    |

La fuente sintética sirve para reproducibilidad y prueba del sistema, pero nunca habilita uso autónomo. La aprobación exige histórico externo, resultados maduros, discriminación, calibración y estabilidad dentro de umbral.
