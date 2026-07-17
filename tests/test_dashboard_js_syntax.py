from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DASHBOARD = ROOT / "outputs" / "dashboard" / "centro-control-mantenimiento-ferroviario.html"


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js no disponible para verificación sintáctica")
def test_dashboard_embedded_js_has_valid_syntax():
    html = DASHBOARD.read_text(encoding="utf-8")
    match = re.search(r"<script>\n(.*)\n</script>", html, flags=re.S)
    assert match, "Script embebido no encontrado en el panel de control"

    script = match.group(1)
    result = subprocess.run(["node", "--check", "-"], input=script, capture_output=True, text=True)
    assert result.returncode == 0, f"Error de sintaxis JS en el panel de control: {result.stderr}"
