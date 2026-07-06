# Registro de Cambios

Todos los cambios destacables de este proyecto se documentan en este fichero.

El formato sigue [Keep a Changelog](https://keepachangelog.com/es-ES/1.1.0/)
y el proyecto se adhiere a [Versionado Semántico](https://semver.org/lang/es/).

## [No publicado]

### Añadido
- Suelo de cobertura (`fail_under = 90`) aplicado en CI: la compilación falla si la cobertura cae.
- `scripts/check_lock_drift.py`: verificación reutilizable de la deriva del bloqueo de dependencias.
- `CITATION.cff` para que el trabajo sea citable.
- Métrica gobernada `cbm_breakeven_value_per_service_hour_eur`: valor sombra de equilibrio
  (€/hora) al que el coste incremental proxy de CBM queda compensado, con su propia
  anti-interpretación (umbral de decisión, no disposición a pagar validada).
- `tests/test_pipeline_determinism.py`: protege que la conexión SQL de producción sea
  monohebra.

### Corregido
- **Determinismo del flujo:** la capa SQL fija DuckDB a una sola hebra
  (`SET threads TO 1`). Antes, la sumación en coma flotante multihebra de `AVG`/`SUM`
  variaba entre corridas y cambiaba la `dashboard-signature` en cada ejecución; ahora
  dos corridas consecutivas producen un panel byte a byte idéntico. Protegido por
  `tests/test_pipeline_determinism.py`.
- `scripts/run_coverage.sh` ahora genera los gráficos de publicación antes de `pytest`,
  por lo que funciona en una copia limpia (antes dependía de artefactos ya presentes).
- **Gráfico de cola priorizada roto:** el ranking de las 15 intervenciones principales
  usaba "unidad · familia" como etiqueta categórica; varios componentes comparten unidad
  y familia (p. ej. dos bogies de la misma unidad), así que esas etiquetas duplicadas
  colapsaban en una sola barra — 15 casos se dibujaban como 10 barras, y las 5 etiquetas
  sobrantes flotaban fuera del área del gráfico, superpuestas al título. Corregido con
  posiciones numéricas explícitas y etiquetas únicas por componente.
- Gráfico de gobernanza: la paleta de color se asignaba por posición de columna tras
  `unstack()`; sin fallos (caso normal), la columna "fallidos" nunca se materializa y la
  única columna restante ("aprobados") heredaba el color de peligro — el gráfico mostraba
  barras rojas bajo un título que afirmaba cero bloqueos. Corregido con reindexado
  explícito de columnas.
- Etiquetas del eje Y duplicadas (98 %, 98 %, 96 %...) en el gráfico de tendencia de
  disponibilidad, por un paso de ticks más fino que la precisión del formateador.
- Un pie de figura fijaba "33 verificaciones" como texto literal mientras el párrafo
  siguiente calculaba la misma cifra dinámicamente; ahora ambos son dinámicos.
- Eliminado un generador de PDF completo basado en matplotlib/PdfPages (596 líneas) que
  había quedado muerto tras la migración a WeasyPrint, sin ninguna llamada activa.
- Códigos de subsistema en inglés sin traducir (`pantograph`, `door`, `brake`...) y modos
  de fallo sin acentuar (`perdida_contacto`, `contaminacion_lubricante`...) que aparecían
  en un gráfico del informe pese a que el resto del documento está en español correcto.
- Cada sección del informe forzaba un salto de página propio, desperdiciando espacio
  cuando una sección terminaba pronto (un caso dejó un recuadro de acción ejecutiva solo
  en una página 85 % en blanco). Ahora el contenido fluye de forma continua entre
  subsecciones; solo se preserva el salto donde compone mejor una sección de cierre.
- Revisión terminológica del repositorio: anglicismos innecesarios en prosa (`scoring`,
  `pipeline`, `dashboard`, `backlog`, `staging`, `checkout`, `offline`, `stack`...)
  sustituidos por su equivalente en español en documentación, notebooks y comentarios de
  código. Los identificadores reales (nombres de columnas, ficheros, funciones) se
  mantienen intactos.
- `notebooks/03_priorizacion_y_scheduling.ipynb` renombrado a
  `03_priorizacion_y_planificacion.ipynb` para coherencia con la revisión terminológica.
- `src/plotting.py` (una única función auxiliar) eliminado; su contenido se integró en
  su único consumidor (`scripts/build_publication_outputs.py`).
- Retirado `outputs/charts/`, directorio de salida heredado ya vacío y sin referencias,
  totalmente sustituido por `outputs/graphs/`.

### Cambiado
- El flujo de CI se ejecuta una sola vez bajo cobertura (lint → deriva → flujo+pruebas+cobertura),
  eliminando una ejecución redundante del flujo.

## [1.0.0] - 2026-06-23

Primera versión lista para publicación.

### Añadido
- Flujo determinista de extremo a extremo: datos sintéticos → SQL por etapas (DuckDB) →
  ingeniería de variables → puntuación de riesgo → priorización y planificación de taller → panel de control.
- Comparativa estratégica CBM vs. preventiva vs. reactiva con análisis de sensibilidad y diferimiento.
- Panel HTML autocontenido (sin conexión, claro/oscuro) alimentado por el registro oficial de métricas.
- Paquete de gráficos ejecutivos e informe analítico en PDF.
- Gobierno de métricas con contratos y validaciones que bloquean el flujo ante fallos críticos.
- Documentación técnica: reproducibilidad, arquitectura, supuestos del modelo y diccionarios de datos/métricas.
- Puertas de calidad: `ruff`, `pytest` con cobertura y verificación de deriva de dependencias.
