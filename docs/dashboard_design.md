# Diseño del Panel Ejecutivo

## Objetivo de producto
Panel orientado a decisión para dirección de mantenimiento y operaciones. Responde en orden:
- qué intervenir primero,
- por qué ese activo y no otro,
- qué exige la cola y en qué orden,
- qué puede ejecutarse con la capacidad real,
- qué vale la estrategia y qué cuesta esperar.

La arquitectura de la información sigue la pirámide invertida: la decisión abre el panel y la
evidencia la sostiene después. La tabla granular cierra, no encabeza.

## Decisiones de diseño implementadas

1. Motor de filtros globales único.
- Los filtros gobiernan indicadores de vista, gráficos, decisión y tabla final.
- Dimensiones agrupadas en cartera (flota, unidad, depósito), activo (familia, sistema) y
  decisión (riesgo, intervención, ventana, estrategia).
- El estado de taller es una excepción deliberada: describe la red física, así que sólo el filtro
  explícito de depósito lo acota. Ligarlo a los depósitos recomendados ocultaría los depósitos que
  acumulan pendientes pero nunca reciben una entrada sugerida.

2. Procedencia visible en cada indicador.
- `oficial` marca las cifras que consumen el registro gobernado (`narrative_metrics_official.csv`)
  y no cambian con los filtros.
- `vista` marca las cifras que responden a la selección activa.
- La orden de trabajo muestra la decisión filtrada; el pie de la tarjeta reproduce la decisión
  oficial sin filtros, de modo que la diferencia entre ambas capas nunca queda implícita.

3. Arquitectura 100% autocontenida sin conexión.
- Sin CDN. HTML único con CSS, JS y tipografía embebidos.
- Gráficos renderizados como SVG generado en el cliente; sin librerías de terceros.
- Versión determinista (`YYYYMMDD-firma`) derivada de la cobertura temporal y del contenido.
- El subconjunto tipográfico embebido cubre todos los glifos usados por la interfaz y por los datos,
  de modo que ningún carácter cae en una fuente del sistema.

4. Métricas gobernadas.
- Todas las cifras consumen salidas oficiales (`data/processed/` + `narrative_metrics_official.csv`).
- Los valores del payload se redondean en origen a la precisión que la interfaz muestra: se evita
  precisión falsa y se reduce el peso del artefacto.
- Se evita codificar conclusiones fijas en el HTML: los textos de lectura estratégica se calculan
  en tiempo de render a partir del payload.

5. Rigor de visualización.
- Ninguna gráfica usa doble eje. Dos medidas de escalas distintas viven en paneles separados con
  eje categórico compartido (salud frente a riesgo por familia) o en pequeños múltiplos con escala
  propia (coste e indisponibilidad al diferir).
- La identidad de color se asigna por rol, no por rango: acento para magnitud, estados reservados
  para severidad, rampas ordinales de un solo tono para magnitudes anidadas (pendientes físicos →
  vencidos → críticos) y por urgencia (ventana de intervención).
- Las rampas ordinales invierten su ancla en modo oscuro: el paso más intenso es siempre el de mayor
  magnitud, sea el más oscuro sobre papel o el más brillante sobre grafito.
- La paleta se verificó con el validador del método de visualización: las marcas superan 3:1 de
  contraste y ΔE ≥ 12 bajo protanopia y deuteranopia en ambos modos; las rampas son monótonas en
  luminosidad y su extremo más cercano a la superficie supera 2:1.
- Etiquetado selectivo: extremos de serie y valores de barra, nunca un número sobre cada punto.
- `saturation_ratio` se presenta como índice de presión (`1,0x` = capacidad), no como porcentaje
  acotado, porque la carga equivalente puede superar la capacidad varias veces.

6. Robustez de experiencia e interfaz.
- Diseño sin solapes (grid con `minmax(0,1fr)`, desbordamiento controlado, consultas de medios
  robustas). El contenido ancho desplaza dentro de su propio contenedor, nunca la página.
- Tabla final con paginación para evitar saturación de DOM.
- Redibujado con espera controlada en `resize` para asegurar legibilidad en cambios de ventana.
- Zonas de contacto de los gráficos ampliadas más allá de la marca; tema claro/oscuro persistente.
- QA automático de interfaz en `tests/test_dashboard_hardening.py`.

## Secciones funcionales
1. Decisión: orden de trabajo prioritaria, evidencia que la sostiene y estado de ejecución.
2. Estado de flota: indicadores oficiales y de vista, con serie semanal de disponibilidad y rango
   de sensibilidad del diferencial CBM.
3. Riesgo técnico: salud y riesgo por familia, vida útil restante estimada.
4. Cola de intervención: carga por ventana, unidades por prioridad, prioridad frente a
   diferimiento, mezcla de decisiones.
5. Capacidad y pendientes: presión de taller y pendientes por depósito.
6. Factores: factor dominante del riesgo y detección previa al fallo.
7. Caso estratégico: coste por hora de servicio preservada, coste de aplazar, valor capturado por
   la planificación.
8. Detalle: tabla interactiva con trazabilidad caso a caso.

## Validaciones aplicadas
- Consistencia de secciones obligatorias y controles de filtrado/paginación.
- Integridad de métricas de pendientes físicos vs riesgo de diferimiento.
- Coherencia del texto de decisión con la clasificación filtrada y con el registro oficial.
- Pruebas de integridad en `tests/test_dashboard_hardening.py`, `tests/test_dashboard_js_syntax.py`
  y `tests/test_reporting_consistency.py`.
