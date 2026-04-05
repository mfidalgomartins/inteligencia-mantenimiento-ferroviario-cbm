# Diseño del Dashboard Ejecutivo

## Objetivo de producto
Dashboard orientado a decisión para dirección de mantenimiento y operaciones, con foco en:
- qué intervenir primero,
- por qué,
- qué riesgo se evita,
- qué capacidad falta,
- qué valor aporta CBM frente a alternativas.

## Decisiones de diseño implementadas
1. Motor de filtros globales único.
- Los filtros gobiernan KPIs, gráficos, ranking, narrativa e tabla final.
- Dimensiones: flota, unidad, depósito, familia, sistema, nivel de riesgo, tipo de intervención, ventana y estrategia.

2. Narrativa orientada a decisión.
- Se eliminan visuales sin impacto decisional.
- Se incorpora panel de decisión ejecutiva con:
  - unidad prioritaria,
  - componente prioritario,
  - impacto de intervenir ahora,
  - riesgo de diferir.

3. Arquitectura 100% autocontenida offline.
- Sin CDN.
- HTML único con CSS/JS embebido.
- Render de charts con SVG inline.
- Version stamping visible (`YYYYMMDD-HHMM` + firma corta de payload) para trazabilidad de release.

4. Métricas gobernadas.
- Todas las cifras consumen salidas oficiales (`data/processed/` + `narrative_metrics_official.csv`).
- Se evita hardcoding de insights.
- KPIs estructurales (disponibilidad, MTBF/MTTR, ahorro CBM, diferimiento) usan `metric_snapshot` SSOT.

5. Robustez UX/Frontend.
- Layout seguro sin solapes (grid con `minmax(0,1fr)`, overflow controlado, media queries robustas).
- Tabla final con paginación para evitar saturación de DOM.
- Re-render con debounce en `resize` para asegurar legibilidad en cambios de viewport.
- QA automático de frontend en `tests/test_dashboard_hardening.py`.

## Secciones funcionales
1. Header ejecutivo y cobertura.
2. KPI cards de disponibilidad, riesgo, backlog y valor CBM.
3. Salud de activos (familias, RUL, deterioro).
4. Operación/servicio (impacto por unidad y disponibilidad).
5. Taller (saturación, backlog físico/vencido/crítico).
6. Alertas e inspección automática.
7. Priorización y cola de intervención.
8. Vista estratégica (reactivo vs preventivo vs CBM).
9. Tabla interactiva final.

## Validaciones aplicadas
- Consistencia de secciones obligatorias.
- Integridad de métricas de backlog físico vs riesgo de diferimiento.
- Coherencia texto de decisión con ranking filtrado.
- Pruebas de integridad en `src/validation.py`.

## Before vs After (usabilidad decisional)
- Antes: parte de filtros no recalculaba todos los componentes; dependencia de CDN externa.
- Después: recalculo integral por filtro y ejecución offline.
- Antes: dashboards con mezcla de métricas legacy.
- Después: semántica alineada con contratos y taxonomía oficial.
- Antes: render de tabla sin paginación y riesgo de densidad visual.
- Después: paginación + guardas de legibilidad (densidad de labels adaptativa).
