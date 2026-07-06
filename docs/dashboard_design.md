# Diseño del Panel Ejecutivo

## Objetivo de producto
Panel orientado a decisión para dirección de mantenimiento y operaciones, con foco en:
- qué intervenir primero,
- por qué,
- qué riesgo se evita,
- qué capacidad falta,
- qué valor aporta CBM frente a alternativas.

## Decisiones de diseño implementadas
1. Motor de filtros globales único.
- Los filtros gobiernan indicadores, gráficos, clasificación, resumen y tabla final.
- Dimensiones: flota, unidad, depósito, familia, sistema, nivel de riesgo, tipo de intervención, ventana y estrategia.

2. Resumen orientado a decisión.
- Se eliminan visuales sin impacto decisional.
- Se incorpora panel de decisión ejecutiva con:
  - unidad prioritaria,
  - componente prioritario,
  - impacto de intervenir ahora,
  - riesgo de diferir.

3. Arquitectura 100% autocontenida sin conexión.
- Sin CDN.
- HTML único con CSS/JS embebido.
- Renderizado de gráficos con SVG embebido.
- Versión determinista (`YYYYMMDD-firma`) derivada de la cobertura temporal y del contenido.

4. Métricas gobernadas.
- Todas las cifras consumen salidas oficiales (`data/processed/` + `narrative_metrics_official.csv`).
- Se evita codificar conclusiones fijas en el HTML.
- Indicadores estructurales (disponibilidad, MTBF/MTTR, diferencial CBM, diferimiento) usan el `metric_snapshot` oficial.

5. Robustez de experiencia e interfaz.
- Diseño seguro sin solapes (grid con `minmax(0,1fr)`, desbordamiento controlado, consultas de medios robustas).
- Tabla final con paginación para evitar saturación de DOM.
- Redibujado con espera controlada en `resize` para asegurar legibilidad en cambios de ventana.
- QA automático de interfaz en `tests/test_dashboard_hardening.py`.

## Secciones funcionales
1. Cabecera ejecutiva y cobertura.
2. Tarjetas de indicadores de disponibilidad, riesgo, pendientes y valor CBM.
3. Salud de activos (familias, RUL, deterioro).
4. Operación/servicio (impacto por unidad y disponibilidad).
5. Taller (saturación, pendientes físicos/vencidos/críticos).
6. Alertas e inspección automática.
7. Priorización y cola de intervención.
8. Vista estratégica (reactivo vs preventivo vs CBM).
9. Tabla interactiva final.

## Validaciones aplicadas
- Consistencia de secciones obligatorias.
- Integridad de métricas de pendientes físicos vs riesgo de diferimiento.
- Coherencia del texto de decisión con la clasificación filtrada.
- Pruebas de integridad en `tests/test_dashboard_hardening.py`, `tests/test_dashboard_js_syntax.py` y `tests/test_reporting_consistency.py`.
