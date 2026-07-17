# Inteligencia de Mantenimiento Ferroviario - CBM

[![CI](https://github.com/mfidalgomartins/inteligencia-mantenimiento-ferroviario-cbm/actions/workflows/ci.yml/badge.svg)](https://github.com/mfidalgomartins/inteligencia-mantenimiento-ferroviario-cbm/actions/workflows/ci.yml)
[![Licencia MIT](https://img.shields.io/badge/licencia-MIT-2c5fa8)](LICENSE)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-2c5fa8)](pyproject.toml)

Sistema de decisión para flotas ferroviarias: prioriza intervenciones de taller, cuantifica el riesgo de diferir cada decisión y mide el valor del mantenimiento basado en condición frente a una estrategia reactiva.

**[Abrir el panel de control](https://mfidalgomartins.github.io/inteligencia-mantenimiento-ferroviario-cbm/)** · **[Leer el informe analítico (PDF)](outputs/reports/informe_analitico_cbm_ferroviario.pdf)** · Python · SQL · DuckDB · HTML sin conexión

Los dos entregables ejecutivos del proyecto son el **panel de control** (la experiencia analítica interactiva)
y el **informe analítico** (el documento de apoyo a la decisión). Ambos se muestran a continuación.

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

![Panel de control CBM ferroviario](assets/preview/dashboard.png)

HTML autocontenido sin dependencias externas ni llamadas de red: funciona completamente sin conexión. Arquitectura en
pirámide invertida — abre con la orden de trabajo prioritaria y su evidencia, no con un muro de gráficos.

- Filtros cruzados por flota, depósito, familia de componente, sistema, nivel de riesgo, tipo de intervención y ventana temporal.
- Procedencia visible en cada indicador: `oficial` (registro gobernado de métricas) frente a `vista` (recorte por filtro activo).
- Ocho secciones de lectura: decisión, estado de flota, riesgo técnico, cola de intervención, capacidad y pendientes, factores, caso estratégico y detalle.
- Modo claro/oscuro y exportación a impresión, con tipografía embebida para paridad visual exacta.
- Verificado con validador de contraste y visión cromática: contraste de marca >3:1 y ΔE ≥12 bajo protanopia y deuteranopia.

**[Abrir panel de control en vivo](https://mfidalgomartins.github.io/inteligencia-mantenimiento-ferroviario-cbm/)**

## Informe analítico

<table>
<tr>
<td width="38%">

![Portada del informe analítico](assets/preview/report-cover.png)

</td>
<td width="62%">

Documento ejecutivo en PDF que acompaña al panel con el razonamiento completo: riesgo y priorización de taller,
vida remanente operativa, comparación de estrategias de mantenimiento y disciplina económica del caso CBM.

- Lectura ejecutiva en la portada con la decisión inmediata, antes de cualquier detalle técnico.
- Mismo registro oficial de métricas que el panel — cero divergencia entre lo narrado y lo mostrado.
- Diseño visual auditado a estándar de consultoría (tipografía, paleta y jerarquía consistentes con el panel).
- Generado de forma determinista con WeasyPrint a partir de HTML/CSS versionado, sin edición manual.

**[Descargar informe analítico (PDF)](outputs/reports/informe_analitico_cbm_ferroviario.pdf)**

</td>
</tr>
</table>

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
src/railway_cbm/  paquete instalable: datos, SQL, puntuación, planificación y panel de control
sql/               capa SQL por etapas (preparación → integración → tablas analíticas → indicadores)
notebooks/         análisis exploratorio por fase del flujo
scripts/           ejecución del flujo, pruebas y publicación
outputs/           panel de control, gráficos PNG e informe PDF
tests/             validaciones de calidad, métricas y consistencia
docs/              reproducibilidad, supuestos, gobierno de métricas y preparación productiva
assets/            tipografía embebida y capturas de referencia del panel/informe
```

Documentación técnica: [reproducibilidad](docs/reproducibility.md) · [arquitectura del repositorio](docs/repo_architecture.md) · [preparación productiva](docs/production_readiness.md) · [seguridad y dependencias](docs/security_dependency_hygiene.md) · [marco RUL](docs/rul_framework.md) · [gobierno de métricas](docs/gobierno_metricas.md)

## Hoja de ruta

El núcleo batch (datos → riesgo → priorización → panel) está implementado y gobernado por puertas de calidad. El
avance hacia operación real sigue tres fases, sin saltos: cada una exige responsable, umbral y evidencia de
aceptación antes de habilitar la siguiente ([detalle completo](docs/production_readiness.md)).

| Fase | Objetivo | Bloqueo actual |
|------|----------|----------------|
| **P0 — Evidencia real** | Cargar histórico autorizado vía `--source external` y recalibrar umbrales con Fiabilidad, Operaciones y Finanzas. | Ejecución publicada usa datos sintéticos; `model_deployment_gate.csv` exige fuente externa. |
| **P1 — Piloto en sombra** | Orquestar en producción, registrar aprobaciones humanas y comparar recomendaciones contra decisiones reales a 30/60/90 días. | Requiere un histórico con seis cortes maduros y ≥30 fallos observados. |
| **P2 — Operación controlada** | Integrar identidad y roles, conectar el registro aprobado con EAM/ERP, y ampliar el MILP con repuestos y habilidades. | Requiere piloto P1 validado con SLA y plan de reversión definidos. |

El sistema nunca ejecuta acciones de forma autónoma: la autoejecución permanece desactivada por diseño en las tres fases.

## Limitaciones
- Todos los datos son sintéticos; los umbrales requieren calibración antes de uso operacional.
- Los costes y ahorros son aproximaciones de escenario, no estimaciones financieras contractuales.
- El RUL sirve como ventana relativa de intervención; su asociación con fallo a 30 días es débil y no representa una fecha de fallo calibrada.
- La planificación es heurística y no garantiza una solución global óptima.

## Tecnologías
Python · SQL · DuckDB · pandas · matplotlib · pytest · pytest-cov · HTML/CSS/JavaScript

## Licencia
MIT.
