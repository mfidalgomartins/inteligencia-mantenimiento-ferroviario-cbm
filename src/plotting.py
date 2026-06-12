from __future__ import annotations

import os
import tempfile
from pathlib import Path


def configure_matplotlib_cache() -> None:
    """Define um cache Matplotlib gravável antes do primeiro import de pyplot."""
    cache_dir = Path(tempfile.gettempdir()) / "railway_cbm_matplotlib"
    cache_dir.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("MPLCONFIGDIR", str(cache_dir))
