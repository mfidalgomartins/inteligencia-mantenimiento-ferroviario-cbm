# Seguridad y dependencias

Política mínima para mantener reproducibilidad, dependencias auditables y outputs HTML seguros.

## Dependencias
- Instalar siempre desde `requirements-lock.txt` para corridas reproducibles.
- Actualizar `requirements.txt` y `requirements-lock.txt` en el mismo cambio cuando se añade una librería.
- Ejecutar `python -m pip check` antes de publicar cambios de dependencias.
- Revisar vulnerabilidades con la herramienta disponible en el entorno antes de una entrega externa.

## Higiene HTML
- Evitar contenido HTML generado desde strings no confiables.
- Escapar texto de datos antes de insertarlo en dashboard o informes.
- Mantener assets locales y rutas relativas dentro de `outputs/`.
- No incluir credenciales, tokens, rutas privadas o datos personales en outputs publicados.

## Revisión antes de merge
- Confirmar que `./scripts/run_tests.sh` pasa completo.
- Confirmar que los tests de hardening del dashboard siguen activos.
- Revisar cambios en `requirements-lock.txt` para detectar upgrades no intencionales.
- Documentar cualquier limitación de seguridad relevante en README o docs.

