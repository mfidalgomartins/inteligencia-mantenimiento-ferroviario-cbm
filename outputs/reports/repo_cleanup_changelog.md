# Changelog de Cleanup Arquitectónico

## Cambios aplicados
- Consolidado SQL runner oficial en `src/run_sql_layer.py`.
- `src/run_sql_models.py` convertido a shim de compatibilidad (deprecado).
- Consolidada auditoría oficial en `src/explore_data_audit.py`.
- `src/data_profile_audit.py` convertido a shim de compatibilidad (deprecado).
- Documentación de arquitectura activa en `docs/repo_architecture.md`.

## Resultado
- Artefactos inventariados: 7
- Rutas muertas detectadas: 0