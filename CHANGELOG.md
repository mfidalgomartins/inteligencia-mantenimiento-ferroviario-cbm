# Registro de Cambios

El formato sigue [Keep a Changelog](https://keepachangelog.com/es-ES/1.1.0/) y el proyecto usa [Versionado Semántico](https://semver.org/lang/es/).

## [No publicado]

### Añadido

- Paquete instalable `railway_cbm`, metadatos PEP 621 y CLI con modos sintético y externo.
- Contratos de snapshot externo con preflight de esquema, PK, FK, fechas y revisiones humanas.
- Manifiestos de entrada y ejecución con hashes, cardinalidad, estado y duración por etapa.
- Validación temporal, calibración rodante, discriminación, Brier, error de calibración y deriva PSI.
- Puerta de despliegue de modelo que bloquea el uso autónomo con evidencia insuficiente.
- Diagnóstico de capacidad, pruebas de estrés y MILP depósito-semana activado por saturación persistente superior al 85 %.
- Registro estable de decisiones, revisiones humanas y controles bloqueantes de modo sombra.
- Umbral de cobertura end-to-end del 90 % aplicado en CI con alcance documentado.
- Lock completo de dependencias, manifiestos separados para ejecución y desarrollo, y controles automáticos de deriva, compatibilidad y vulnerabilidades.
- Actualizaciones agrupadas de dependencias y acciones de CI fijadas por SHA.
- Contratos de datos y métricas, prueba de determinismo SQL y metadatos `CITATION.cff`.
- Métrica gobernada de valor de equilibrio por hora de servicio para la estrategia CBM.
- Documentación concisa de los módulos y puntos de entrada públicos.
- Rediseño del panel de control como producto de decisión: arquitectura en pirámide invertida que
  abre con la orden de trabajo prioritaria y su evidencia, procedencia visible (`oficial` frente a
  `vista`) en cada indicador, serie semanal de disponibilidad, rango de sensibilidad del diferencial
  CBM y economía de la cola (coste de retraso y horas de taller) a partir de columnas canónicas.
- Lectura estratégica del caso base: coste incremental por hora de servicio preservada frente a la
  estrategia reactiva, comparando CBM y preventiva rígida sobre la métrica gobernada de equilibrio.
- Sistema visual verificado con el validador de contraste y visión cromática: contraste de marcas
  superior a 3:1 y ΔE ≥ 12 bajo protanopia y deuteranopia en modo claro y oscuro.

### Corregido

- Presentación de `saturation_ratio` como porcentaje acotado en el panel, cuando la carga
  equivalente puede superar la capacidad varias veces; ahora se muestra como índice de presión
  con la capacidad (`1,0x`) como referencia y el umbral de bloqueo del motor en `1,05x`.
- Vistas de depósito limitadas a los depósitos recomendados por el planificador, que ocultaban los
  depósitos con mayor cola física pero sin entrada sugerida; el estado de taller describe ahora la
  red completa y sólo el filtro explícito de depósito lo acota.
- Gráfico de diferimiento con doble eje, que sugería una correlación inexistente entre coste e
  indisponibilidad; sustituido por pequeños múltiplos con escala propia y eje temporal compartido.
- Comparación de salud y probabilidad de fallo sobre un mismo eje pese a ser unidades distintas;
  ahora ocupan paneles separados con eje categórico compartido.
- Glifos de la interfaz fuera del subconjunto tipográfico embebido, que caían en fuentes del
  sistema y rompían la coherencia métrica del texto.
- Multiplicación histórica del grano componente-día al unir varias órdenes de backlog; ahora se agrega severidad, edad y riesgo antes del join y se bloquean duplicados.
- Aceptación y etiquetado auditable de soluciones MILP dentro de tolerancia, sin presentarlas como óptimos probados.
- Determinismo byte a byte del panel mediante ejecución DuckDB monohebra.
- Respeto de `PYTHON_BIN` en todos los scripts operativos.
- Gráficos con categorías repetidas, asignación posicional de color y etiquetas de ejes duplicadas.
- Dependencias transitivas con avisos de seguridad conocidos (`msgpack`, `Pillow` y `Pygments`).
- Serialización del payload del panel endurecida contra cierre de bloques `script`.
- Eliminación automática de artefactos de datos obsoletos antes de cada ejecución completa.
- Terminología pública incoherente y comentarios redundantes o históricos en código activo.
- Parámetros sin uso en la generación sintética.

### Cambiado

- Código reorganizado de módulos sueltos a layout `src/railway_cbm/` con imports absolutos de paquete.
- La planificación formal se ejecuta de forma condicionada y su resultado permanece en modo sombra.
- CI ejecuta una única corrida del flujo bajo cobertura e incluye auditoría de dependencias.
- Informe migrado a WeasyPrint y retirado el generador PDF anterior.
- Salidas públicas consolidadas en `outputs/graphs`, `outputs/dashboard` y `outputs/reports`.
- Alias históricos y CSV de informe transitorios sustituidos por salidas canónicas en `data/processed`.
- Ingestión de CSV trasladada de pandas a lectura directa de DuckDB para reducir memoria intermedia.
- Tipado actualizado a Python 3.12 y lint ampliado con reglas de modernización.
- Alcance de producción actualizado para separar capacidades batch implementadas de infraestructura externa pendiente.

## [1.0.0] - 2026-06-23

### Añadido

- Flujo determinista: datos sintéticos, SQL DuckDB, variables, riesgo, RUL, priorización y planificación.
- Comparativa CBM, preventiva y reactiva con sensibilidad y análisis de diferimiento.
- Panel HTML autocontenido, gráficos ejecutivos e informe analítico PDF.
- Gobierno de métricas, documentación técnica y puertas de calidad automatizadas.
