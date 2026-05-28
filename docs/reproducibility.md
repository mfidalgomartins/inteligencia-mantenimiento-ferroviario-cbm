# Guía de Reproducibilidad

## Objetivo
Ejecutar el proyecto de forma determinista en una máquina limpia y con gates de calidad activos.

## Entorno
1. Crear entorno virtual:
```bash
python3 -m venv .venv
```
2. Activar:
```bash
source .venv/bin/activate
```
3. Instalar dependencias:
```bash
pip install -r requirements-lock.txt
```

## Ejecución completa
```bash
python -m src.run_pipeline
```

La pipeline genera datos sintéticos, marts procesados, scoring, RUL, priorización, scheduling y el dashboard final.

## Verificaciones mínimas
```bash
pytest -q
```

Publicación recomendada solo si:
1. `pytest -q` pasa completo.
2. `outputs/dashboard/centro-control-mantenimiento-ferroviario.html` es el único HTML final.
3. `data/processed/narrative_metrics_official.csv` está alineado con README, memo y dashboard.

## Limitaciones conocidas
- Dataset sintético: no permite inferencia causal de producción.
- Capa económica basada en proxies de coste.
- Scheduling heurístico, no optimización global matemática.
