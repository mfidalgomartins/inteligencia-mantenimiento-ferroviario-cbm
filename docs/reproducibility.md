# Guía de Reproducibilidad

## Objetivo
Ejecutar el proyecto de forma determinista en una máquina limpia y con gates de calidad activos.

## Entorno
Requiere Python 3.12 o superior.
Reserve al menos 4 GB libres para los CSV y artefactos generados localmente.

1. Crear entorno virtual:
```bash
python3 -m venv .venv
```
2. Activar:
```bash
source .venv/bin/activate
```
3. Instalar las versiones validadas:
```bash
python -m pip install -r requirements-lock.txt
```

## Ejecución completa
```bash
./scripts/run_pipeline.sh
```

La pipeline genera datos sintéticos, marts, scoring, RUL, priorización, scheduling, documentación derivada y dashboard.

## Verificación
```bash
./scripts/run_tests.sh
```

Publicación recomendada solo si:
1. La pipeline termina sin validaciones de severidad alta fallidas.
2. Lint y suite de tests pasan completos.
3. `data/processed/narrative_metrics_official.csv` está alineado con README, memo y dashboard.
4. `git diff --check` no detecta errores de formato.

## Limitaciones conocidas
- Dataset sintético: no permite inferencia causal de producción.
- Capa económica basada en proxies de coste.
- RUL útil como ventana relativa, no como fecha de fallo calibrada.
- Scheduling heurístico, no optimización global matemática.
