# Validation Report

## Resumen Ejecutivo QA
- Estado de publicación: **READY**
- Confianza global: **ALTA**
- Controles ejecutados: **151**
- Controles aprobados: **151**
- Controles fallidos: **0**
- Publish blockers activos: **0**

## Release Readiness
- Estado primario: **committee-grade**
- technically_valid: **True**
- analytically_acceptable: **True**
- decision-support only: **False**
- screening-grade only: **False**
- not committee-grade: **False**
- publish-blocked: **False**

## Qué Se Comprobó (Matriz por Capa)
| layer                    |   total_controls |   passed_controls |   failed_controls |   failed_blockers |   pass_rate |
|:-------------------------|-----------------:|------------------:|------------------:|------------------:|------------:|
| dashboard_datasets       |               47 |                47 |                 0 |                 0 |           1 |
| features                 |               12 |                12 |                 0 |                 0 |           1 |
| marts                    |                6 |                 6 |                 0 |                 0 |           1 |
| raw_data                 |               30 |                30 |                 0 |                 0 |           1 |
| recommendations          |               15 |                15 |                 0 |                 0 |           1 |
| reports_docs_consistency |               12 |                12 |                 0 |                 0 |           1 |
| scores                   |               16 |                16 |                 0 |                 0 |           1 |
| staging                  |               13 |                13 |                 0 |                 0 |           1 |

## Qué Falló (detalle priorizado)
- No hay controles fallidos en esta corrida.

## Severidad de Fallos
- Sin fallos.

## Lista de Checks que Bloquean Publicación
- No hay blockers activos en esta corrida.

## Reglas de Interpretación
- `critica`: fallo estructural, publish-blocker por defecto.
- `alta`: impacto analítico serio; puede bloquear publicación según control.
- `media`: degrada calidad analítica, no bloquea por sí sola.
- `informativa`: control de auditoría/seguimiento.

## Disciplina de Release
- `technically_valid`: integridad estructural sin fallos críticos ni blockers.
- `analytically_acceptable`: además no hay fallos de severidad alta.
- `decision-support only`: técnicamente válido pero todavía no analíticamente aceptable.
- `screening-grade only`: aceptable para screening, no para comité.
- `committee-grade`: apto para revisión ejecutiva con riesgo controlado.
- `publish-blocked`: no publicar ni presentar como base de decisión.

## Caveats
- Los datos son sintéticos y no sustituyen calibración con histórico real.
- Las señales económicas son proxy y requieren ajuste por costes/SLA reales.
- Un estado READY no implica validez causal, solo coherencia analítica del repositorio.