from __future__ import annotations

import re
from pathlib import Path

from railway_cbm.build_dashboard import _json_for_script

ROOT = Path(__file__).resolve().parents[1]
PANEL = ROOT / "outputs" / "dashboard" / "centro-control-mantenimiento-ferroviario.html"
ROOT_INDEX = ROOT / "index.html"


def _html() -> str:
    assert PANEL.exists(), f"Panel de control oficial no existe: {PANEL}"
    return PANEL.read_text(encoding="utf-8")


def _root_index_html() -> str:
    assert ROOT_INDEX.exists(), f"Index público no existe: {ROOT_INDEX}"
    return ROOT_INDEX.read_text(encoding="utf-8")


def test_dashboard_single_official_artifact():
    dashboard_dir = ROOT / "outputs" / "dashboard"
    html_files = sorted(p.name for p in dashboard_dir.glob("*.html"))
    assert html_files == ["centro-control-mantenimiento-ferroviario.html"], (
        f"Se esperaba un único HTML final, encontrado: {html_files}"
    )


def test_dashboard_offline_no_cdn_refs():
    html = _html()
    assert re.search(r"<script[^>]+src=['\"]https?://", html, flags=re.IGNORECASE) is None
    assert re.search(r"<link[^>]+href=['\"]https?://", html, flags=re.IGNORECASE) is None


def test_static_html_blocks_high_risk_embedding_and_navigation_patterns():
    for html in [_html(), _root_index_html()]:
        for pattern in [
            r"\s+on[a-z]+\s*=",
            r"javascript:",
            r"<iframe\b",
            r"<object\b",
            r"<embed\b",
            r"<form\b",
        ]:
            assert re.search(pattern, html, flags=re.IGNORECASE) is None, f"Patrón HTML inseguro: {pattern}"
        assert re.search(r'target=["\']_blank["\']', html, flags=re.IGNORECASE) is None


def test_root_index_redirect_stays_relative_to_official_dashboard():
    html = _root_index_html()
    expected = "outputs/dashboard/centro-control-mantenimiento-ferroviario.html"
    assert f'content="0; url={expected}"' in html
    assert f'href="{expected}"' in html
    assert "http://" not in html.lower()
    assert "https://" not in html.lower()


def test_dashboard_meta_stamp_present():
    html = _html()
    assert re.search(r'name="dashboard-version" content="\d{8}-[0-9a-f]{4}"', html)
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
    size_bytes = PANEL.stat().st_size
    assert size_bytes < 2_000_000, f"Panel de control demasiado pesado ({size_bytes} bytes)"


def test_dashboard_responsive_redraw_debounce():
    html = _html()
    assert 'window.addEventListener("resize"' in html
    assert "setTimeout(() => renderAll(), 180)" in html


def test_dashboard_filter_options_are_dom_built_not_template_injected():
    html = _html()
    assert "function setSelectOptions(sel, values)" in html
    assert 'document.createElement("option")' in html
    assert "sel.innerHTML = uniq(baseRows.map" not in html


def test_dashboard_payload_serialization_blocks_script_breakout():
    serialized = _json_for_script({"value": "</script><script>alert('xss')</script>&"})
    assert "</script>" not in serialized.lower()
    assert "<" not in serialized
    assert ">" not in serialized
    assert "&" not in serialized
