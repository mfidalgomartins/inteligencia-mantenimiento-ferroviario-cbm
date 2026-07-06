# Seguridad y dependencias

Política mínima para mantener reproducibilidad, dependencias auditables y salidas HTML seguras.

## Dependencias
- Instalar siempre desde `requirements-lock.txt` para corridas reproducibles.
- Actualizar `requirements.txt` y `requirements-lock.txt` en el mismo cambio cuando se añade una librería.
- Ejecutar `python -m pip check` antes de publicar cambios de dependencias.
- Revisar vulnerabilidades con la herramienta disponible en el entorno antes de una entrega externa.

## Higiene HTML
- Evitar contenido HTML generado desde strings no confiables.
- Escapar texto de datos antes de insertarlo en el panel de control o informes.
- Mantener activos locales y rutas relativas dentro de `outputs/`.
- No incluir credenciales, tokens, rutas privadas o datos personales en salidas publicadas.

## Revisión antes de fusionar cambios
- Confirmar que `./scripts/run_tests.sh` pasa completo.
- Confirmar que los tests de endurecimiento del panel de control siguen activos.
- Revisar cambios en `requirements-lock.txt` para detectar actualizaciones no intencionales.
- Documentar cualquier limitación de seguridad relevante en README o docs.
