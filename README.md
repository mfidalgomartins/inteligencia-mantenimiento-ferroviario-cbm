# Inteligencia de Mantenimiento Ferroviario - CBM

Sistema de decisión para flotas ferroviarias: prioriza intervenciones de taller, cuantifica el riesgo de diferir cada decisión y mide el valor del mantenimiento basado en condición frente a una estrategia reactiva.

**[Panel de control en vivo](https://mfidalgomartins.github.io/inteligencia-mantenimiento-ferroviario-cbm/)** · Python · SQL · DuckDB · HTML sin conexión

## Resultados - flota sintética de 144 unidades

| Métrica | Valor |
|---------|------:|
| Disponibilidad media de flota | **95,75 %** |
| Unidades de alto riesgo (≥ media + 1,5σ) | **9** |
| Pendientes físicos | **2.056 pendientes** |
| Pendientes vencidos | **2.011 pendientes** |
| Pendientes críticos físicos | **1.955 pendientes** |
| Casos de alto riesgo de diferimiento | **43** |
| Coste incremental aproximado CBM vs reactiva | **€ 48.700.525** |
| Mejora de disponibilidad CBM vs reactiva | **+0,93 p.p.** |

**Decisión actual:** intervenir primero la unidad `UNI0055`, componente `COMP000438`.

## Qué resuelve

- Integra sensores, inspección automática, fallos y mantenimiento para puntuar 1.152 componentes.
- Ordena y secuencia la cola de taller según riesgo técnico, impacto de servicio, capacidad y ventana operativa.
- Compara CBM, preventiva rígida y reactiva con supuestos económicos explícitos y análisis de sensibilidad.
- Mantiene trazabilidad desde los datos hasta las métricas ejecutivas y bloquea el flujo ante validaciones críticas.

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

## Panel de control

HTML autocontenido sin dependencias externas. Funciona sin conexión e incluye filtros por flota, depósito, familia, sistema, riesgo e intervención.

**[Abrir panel de control en vivo](https://mfidalgomartins.github.io/inteligencia-mantenimiento-ferroviario-cbm/)**

**[Descargar informe analítico (PDF)](outputs/reports/informe_analitico_cbm_ferroviario.pdf)**

## Arquitectura

```
datos sintéticos → preparación SQL → tablas analíticas → puntuación → priorización → panel de control
```

1. Datos sintéticos deterministas de operación, sensores, fallos, inspección y mantenimiento.
2. Capa SQL DuckDB por etapas: preparación, integración, tablas analíticas e indicadores.
3. Ingeniería de variables para salud de componente, RUL operativo y puntuación de prioridad.
4. Priorización y planificación heurística con capacidad de taller.
5. Comparativa estratégica y análisis de diferimiento.
6. Panel de control sin conexión alimentado por el registro oficial de métricas.

## Reproducir
Requiere Python 3.12 o superior.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-lock.txt
./scripts/run_pipeline.sh
./scripts/run_tests.sh
./scripts/run_coverage.sh
```
El flujo usa semilla fija y regenera datos, métricas, documentación derivada y panel de control.

## Estructura

```
src/          lógica de datos, puntuación y generador del panel de control
sql/          capa SQL por etapas (preparación → integración → tablas analíticas → indicadores)
notebooks/    análisis exploratorio por fase del flujo
scripts/      ejecución del flujo, pruebas y publicación
outputs/      panel de control, gráficos PNG e informe PDF
tests/        validaciones de calidad, métricas y consistencia
docs/         reproducibilidad, supuestos y contratos de métricas
```

Documentación técnica: [reproducibilidad](docs/reproducibility.md) · [arquitectura del repositorio](docs/repo_architecture.md) · [preparación productiva](docs/production_readiness.md) · [seguridad y dependencias](docs/security_dependency_hygiene.md) · [marco RUL](docs/rul_framework.md) · [gobierno de métricas](docs/gobierno_metricas.md)

## Limitaciones
- Todos los datos son sintéticos; los umbrales requieren calibración antes de uso operacional.
- Los costes y ahorros son aproximaciones de escenario, no estimaciones financieras contractuales.
- El RUL sirve como ventana relativa de intervención; su asociación con fallo a 30 días es débil y no representa una fecha de fallo calibrada.
- La planificación es heurística y no garantiza una solución global óptima.

## Tecnologías
Python · SQL · DuckDB · pandas · matplotlib · pytest · pytest-cov · HTML/CSS/JavaScript

## Licencia
MIT.
