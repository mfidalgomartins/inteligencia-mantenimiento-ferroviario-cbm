# Inteligencia de Mantenimiento Ferroviario - CBM

Plataforma de decisión para flotas ferroviarias: prioriza intervenciones de taller, cuantifica el riesgo de diferir cada decisión y mide el valor del mantenimiento basado en condición frente a una estrategia reactiva.

**[Dashboard en vivo](https://mfidalgomartins.github.io/inteligencia-mantenimiento-ferroviario-cbm/)** · Python · SQL · DuckDB · HTML offline

## Resultados - flota sintética de 144 unidades

| Métrica | Valor |
|---------|------:|
| Disponibilidad media de flota | **95,75 %** |
| Unidades de alto riesgo (≥ media + 1,5σ) | **9** |
| Backlog físico | **2.056 pendientes** |
| Backlog vencido | **2.011 pendientes** |
| Backlog crítico físico | **1.955 pendientes** |
| Casos de alto riesgo de diferimiento | **43** |
| Coste incremental proxy CBM vs reactiva | **€ 48.700.525** |
| Mejora de disponibilidad CBM vs reactiva | **+0,93 p.p.** |

**Decisión actual:** intervenir primero la unidad `UNI0055`, componente `COMP000438`.

## Qué resuelve

- Integra sensores, inspección automática, fallos y mantenimiento para puntuar 1.152 componentes.
- Ordena y secuencia la cola de taller según riesgo técnico, impacto de servicio, capacidad y ventana operativa.
- Compara CBM, preventiva rígida y reactiva con supuestos económicos explícitos y análisis de sensibilidad.
- Mantiene trazabilidad desde los datos hasta las métricas ejecutivas y bloquea la pipeline ante validaciones críticas.

## Análisis

<table>
<tr>
<td width="50%">

![Valor estratégico CBM vs reactiva](outputs/graphs/02_valor_estrategias.png)

</td>
<td width="50%">

![Distribución de riesgo de flota](outputs/graphs/06_distribucion_riesgo_unidades.png)

</td>
</tr>
<tr>
<td>

![Cola de taller por riesgo](outputs/graphs/04_ranking_intervenciones.png)

</td>
<td>

![Saturación de depósitos](outputs/graphs/05_saturacion_depositos.png)

</td>
</tr>
</table>

## Dashboard

HTML autocontenido sin dependencias externas. Funciona offline e incluye filtros por flota, depósito, familia, sistema, riesgo e intervención.

**[Abrir dashboard en vivo](https://mfidalgomartins.github.io/inteligencia-mantenimiento-ferroviario-cbm/)**

**[Descargar informe analítico (PDF)](outputs/reports/informe_analitico_cbm_ferroviario.pdf)**

## Arquitectura

```
datos sintéticos → staging SQL → marts → scoring → priorización → dashboard
```

1. Datos sintéticos deterministas de operación, sensores, fallos, inspección y mantenimiento.
2. Capa SQL DuckDB por etapas: staging, integración, marts y KPIs.
3. Feature engineering para salud de componente, RUL operativo y score de prioridad.
4. Priorización y scheduling heurístico con capacidad de taller.
5. Comparativa estratégica y análisis de diferimiento.
6. Dashboard offline alimentado por el registro oficial de métricas.

## Reproducir
Requiere Python 3.12 o superior.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-lock.txt
./scripts/run_pipeline.sh
./scripts/run_tests.sh
```
La pipeline usa semilla fija y regenera datos, métricas, documentación derivada y dashboard.

## Estructura

```
src/          lógica de datos, scoring y generador del dashboard
sql/          capa SQL por etapas (staging → integración → marts → KPIs)
notebooks/    análisis exploratorio por fase del pipeline
scripts/      ejecución del pipeline, tests y publicación
outputs/      dashboard, gráficos PNG e informe PDF
tests/        validaciones de QA, métricas y consistencia
docs/         reproducibilidad, supuestos y contratos de métricas
```

Documentación técnica: [`reproducibility`](docs/reproducibility.md) · [`repo_architecture`](docs/repo_architecture.md) · [`rul_framework`](docs/rul_framework.md) · [`gobierno_metricas`](docs/gobierno_metricas.md)

## Limitaciones
- Todos los datos son sintéticos; los umbrales requieren calibración antes de uso operacional.
- Los costes y ahorros son proxies de escenario, no estimaciones financieras contratuales.
- El RUL sirve como ventana relativa de intervención; su asociación con fallo a 30 días es débil y no representa una fecha de fallo calibrada.
- El scheduling es heurístico y no garantiza una solución global óptima.

## Stack
Python · SQL · DuckDB · pandas · matplotlib · pytest · HTML/CSS/JavaScript

## Licencia
MIT.
