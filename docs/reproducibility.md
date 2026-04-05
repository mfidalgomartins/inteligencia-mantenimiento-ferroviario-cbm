# Reproducibility Guide

## Objective
Provide deterministic, clean-machine execution with explicit dependencies and hard governance gates.

## Environment
1. Create virtual environment:
`python3 -m venv .venv`
2. Activate:
`source .venv/bin/activate`
3. Install locked dependencies:
`pip install -r requirements-lock.txt`

If a lock refresh is needed, update `requirements-lock.txt` from a validated run.

## Full Pipeline
`python -m src.run_pipeline`

Pipeline includes:
- synthetic generation
- SQL layer
- features/scoring/RUL/recommendations
- strategy and inspection modules
- governance contracts (fail on blockers)
- reporting/dashboard
- validation

## Quality Gates
- Governance contracts with blocker policy: `outputs/reports/governance_contract_blockers.csv`
- Validation blockers: `outputs/reports/publish_blockers.csv`
- Tests: `pytest -q`

## Publish Criteria
Minimum required:
1. `governance_contract_blockers.csv` empty.
2. `publish_blockers.csv` empty.
3. `pytest -q` all green.

## Remaining Caveats
- Synthetic data: no direct causal claims to real fleets.
- Economic values are proxy-level and scenario-sensitive.
- Scheduling remains heuristic, not optimizer-grade.
