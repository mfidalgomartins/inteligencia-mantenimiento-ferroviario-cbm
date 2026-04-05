from __future__ import annotations

from src.reporting_governance import sync_narrative_artifacts


def generate_executive_outputs() -> None:
    sync_narrative_artifacts(force_recompute=True)


if __name__ == "__main__":
    generate_executive_outputs()
