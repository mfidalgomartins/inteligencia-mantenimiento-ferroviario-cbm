# Seguridad y dependencias

Política mínima para mantener reproducibilidad, dependencias auditables y salidas HTML seguras.

## Dependencias
- `requirements.txt` contiene solo dependencias de ejecución; `requirements-dev.txt` añade herramientas de calidad.
- Instalar `requirements-lock.txt` para reproducir el entorno validado, incluidas dependencias transitivas.
- Actualizar los manifiestos y el lock en el mismo cambio cuando se añade o elimina una librería.
- Ejecutar `python -m pip check` antes de publicar cambios de dependencias.
- Ejecutar `python -m pip_audit -r requirements-lock.txt` antes de una entrega externa; CI aplica esta revisión automáticamente.
- Mantener acciones de GitHub fijadas por SHA; Dependabot agrupa sus actualizaciones mensuales.

## Higiene HTML
- Evitar contenido HTML generado desde strings no confiables.
- Escapar texto de datos antes de insertarlo en el panel de control o informes; el payload JSON sustituye `<`, `>` y `&` por escapes Unicode.
- Mantener activos locales y rutas relativas dentro de `outputs/`.
- No incluir credenciales, tokens, rutas privadas o datos personales en salidas publicadas.

## Revisión antes de fusionar cambios
- Confirmar que `./scripts/run_tests.sh` pasa completo.
- Confirmar que los tests de endurecimiento del panel de control siguen activos.
- Revisar cambios en `requirements-lock.txt` para detectar actualizaciones no intencionales.
- Confirmar que no existen secretos, rutas locales ni datos personales en artefactos públicos.
- Documentar cualquier limitación de seguridad relevante en README o docs.
