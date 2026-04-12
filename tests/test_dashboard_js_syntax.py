from __future__ import annotations

import re
import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
DASHBOARD = ROOT / "outputs" / "dashboard" / "centro-control-mantenimiento-ferroviario.html"


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js não disponível para check sintático")
def test_dashboard_embedded_js_has_valid_syntax():
    html = DASHBOARD.read_text(encoding="utf-8")
    match = re.search(r"<script>\n(.*)\n</script>", html, flags=re.S)
    assert match, "Script embebido não encontrado no dashboard"

    script = match.group(1)
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as tmp:
        tmp.write(script)
        tmp_path = tmp.name

    result = subprocess.run(["node", "--check", tmp_path], capture_output=True, text=True)
    assert result.returncode == 0, f"Erro de sintaxe JS no dashboard: {result.stderr}"
