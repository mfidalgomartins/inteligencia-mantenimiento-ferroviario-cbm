from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DASHBOARD = ROOT / "outputs" / "dashboard" / "index.html"


def _html() -> str:
    assert DASHBOARD.exists(), f"Dashboard oficial no existe: {DASHBOARD}"
    return DASHBOARD.read_text(encoding="utf-8")


def test_dashboard_single_official_artifact():
    dashboard_dir = ROOT / "outputs" / "dashboard"
    html_files = sorted(p.name for p in dashboard_dir.glob("*.html"))
    assert html_files == ["index.html"], f"Se esperava solo index.html oficial, encontrado: {html_files}"


def test_dashboard_offline_no_cdn_refs():
    html = _html()
    assert re.search(r"<script[^>]+src=['\"]https?://", html, flags=re.IGNORECASE) is None
    assert re.search(r"<link[^>]+href=['\"]https?://", html, flags=re.IGNORECASE) is None


def test_dashboard_meta_stamp_present():
    html = _html()
    assert 'name="dashboard-version"' in html
    assert 'name="dashboard-signature"' in html


def test_dashboard_layout_safety_guards_present():
    html = _html()
    required = [
        "overflow-x:hidden",
        "grid-template-columns:minmax(260px,var(--sidebar-width)) minmax(0,1fr)",
        ".section{",
        "min-width:0",
        ".pager{display:flex",
    ]
    for token in required:
        assert token in html, f"Falta guarda de layout/UX: {token}"
    assert "position:absolute" not in html, "No debe haber posicionamiento absoluto frágil en layout principal"


def test_dashboard_filter_and_pagination_controls_exist():
    html = _html()
    for element_id in [
        "f_flota",
        "f_unidad",
        "f_deposito",
        "f_familia",
        "f_sistema",
        "f_riesgo",
        "f_intervencion",
        "f_ventana",
        "f_estrategia",
        "btnPrevPage",
        "btnNextPage",
        "pageSize",
        "pageInfo",
    ]:
        assert f'id="{element_id}"' in html, f"Control ausente: {element_id}"


def test_dashboard_performance_payload_ceiling():
    size_bytes = DASHBOARD.stat().st_size
    assert size_bytes < 2_000_000, f"Dashboard demasiado pesado ({size_bytes} bytes)"


def test_dashboard_responsive_redraw_debounce():
    html = _html()
    assert "window.addEventListener(\"resize\"" in html
    assert "setTimeout(() => renderAll(), 180)" in html
