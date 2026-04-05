from __future__ import annotations

import csv
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.config import OUTPUTS_DASHBOARD_DIR, OUTPUTS_REPORTS_DIR, ROOT_DIR


@dataclass
class ReleaseCheck:
    check_id: str
    passed: bool
    severity: str
    detail: str
    recommendation: str


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _build_manifest(paths: list[Path]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for p in paths:
        exists = p.exists()
        stat = p.stat() if exists else None
        rows.append(
            {
                "artifact_path": str(p.relative_to(ROOT_DIR)),
                "exists": exists,
                "size_bytes": int(stat.st_size) if stat else 0,
                "sha256": _sha256(p) if exists else "",
                "modified_utc": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat() if stat else "",
            }
        )
    return pd.DataFrame(rows)


def _extract_token(text: str, pattern: str) -> str | None:
    m = re.search(pattern, text, flags=re.IGNORECASE)
    if not m:
        return None
    return m.group(1)


def _run_checks() -> pd.DataFrame:
    dashboard = OUTPUTS_DASHBOARD_DIR / "index.html"
    readiness = OUTPUTS_REPORTS_DIR / "release_readiness.csv"
    narrative = OUTPUTS_REPORTS_DIR / "narrative_metrics_official.csv"
    validation = OUTPUTS_REPORTS_DIR / "validation_checks_detailed.csv"

    html = dashboard.read_text(encoding="utf-8") if dashboard.exists() else ""
    checks: list[ReleaseCheck] = []

    dashboard_files = list(OUTPUTS_DASHBOARD_DIR.glob("*.html"))
    checks.append(
        ReleaseCheck(
            check_id="release_single_dashboard_artifact",
            passed=(len(dashboard_files) == 1 and dashboard_files[0].name == "index.html"),
            severity="alta",
            detail=f"dashboard_html_files={[x.name for x in dashboard_files]}",
            recommendation="Mantener un único dashboard oficial: outputs/dashboard/index.html.",
        )
    )

    sig_header = _extract_token(html, r"Dashboard:\s*\d{8}-\d{4}\s*·\s*([a-f0-9]{10})")
    sig_payload = _extract_token(html, r'"payload_signature"\s*:\s*"([a-f0-9]{10})"')
    checks.append(
        ReleaseCheck(
            check_id="release_dashboard_signature_consistency",
            passed=(sig_header is not None and sig_payload is not None and sig_header == sig_payload),
            severity="alta",
            detail=f"header_sig={sig_header}, payload_sig={sig_payload}",
            recommendation="Sincronizar stamp visual y meta payload_signature.",
        )
    )

    checks.append(
        ReleaseCheck(
            check_id="release_dashboard_offline_mode",
            passed=(
                re.search(r"<script[^>]+src=['\"]https?://", html, flags=re.IGNORECASE) is None
                and re.search(r"<link[^>]+href=['\"]https?://", html, flags=re.IGNORECASE) is None
            ),
            severity="critica",
            detail="external_refs_found=False" if html else "dashboard_missing",
            recommendation="Eliminar dependencias externas para ejecución offline.",
        )
    )

    if narrative.exists():
        metrics = pd.read_csv(narrative)
        lookup = dict(zip(metrics["metric_id"].astype(str), metrics["metric_value"].astype(str), strict=False))
        top_unit_expected = lookup.get("top_unit_by_priority")
        top_comp_expected = lookup.get("top_component_by_priority")
    else:
        top_unit_expected = None
        top_comp_expected = None

    top_unit_html = _extract_token(html, r"Unidad que debe entrar primero:</strong>\s*([A-Z0-9]+)")
    top_comp_html = _extract_token(html, r"Componente que debe sustituirse primero:</strong>\s*([A-Z0-9]+)")
    checks.append(
        ReleaseCheck(
            check_id="release_dashboard_decision_alignment_ssot",
            passed=(
                top_unit_expected is not None
                and top_comp_expected is not None
                and top_unit_html == top_unit_expected
                and top_comp_html == top_comp_expected
            ),
            severity="alta",
            detail=(
                f"top_unit_html={top_unit_html}, top_unit_expected={top_unit_expected}, "
                f"top_comp_html={top_comp_html}, top_comp_expected={top_comp_expected}"
            ),
            recommendation="Regenerar dashboard desde métricas SSOT tras actualizar scoring/priorización.",
        )
    )

    publish_blocked = None
    primary_state = None
    if readiness.exists():
        rr = pd.read_csv(readiness)
        blocked_row = rr[rr["dimension"] == "publish-blocked"]
        state_row = rr[rr["dimension"] == "primary_release_state"]
        if not blocked_row.empty:
            publish_blocked = str(blocked_row.iloc[0]["value"]).strip().lower() == "true"
        if not state_row.empty:
            primary_state = str(state_row.iloc[0]["value"]).strip()
    checks.append(
        ReleaseCheck(
            check_id="release_readiness_not_blocked",
            passed=(publish_blocked is False and primary_state is not None),
            severity="critica",
            detail=f"publish_blocked={publish_blocked}, primary_state={primary_state}",
            recommendation="Resolver publish-blockers antes de declarar release candidate.",
        )
    )

    validation_failed = None
    if validation.exists():
        vd = pd.read_csv(validation)
        validation_failed = int((~vd["passed"]).sum()) if "passed" in vd.columns else None
    checks.append(
        ReleaseCheck(
            check_id="release_validation_has_results",
            passed=(validation_failed is not None),
            severity="media",
            detail=f"failed_checks={validation_failed}",
            recommendation="Ejecutar validación completa y generar validation_checks_detailed.csv.",
        )
    )

    return pd.DataFrame(
        [
            {
                "check_id": c.check_id,
                "passed": c.passed,
                "severity": c.severity,
                "detail": c.detail,
                "recommendation": c.recommendation,
            }
            for c in checks
        ]
    )


def run_release_hardening() -> None:
    OUTPUTS_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    tracked = [
        ROOT_DIR / "README.md",
        ROOT_DIR / "docs" / "memo_ejecutivo_es.md",
        ROOT_DIR / "docs" / "dashboard_design.md",
        ROOT_DIR / "outputs" / "dashboard" / "index.html",
        ROOT_DIR / "outputs" / "reports" / "validation_report.md",
        ROOT_DIR / "outputs" / "reports" / "validation_checks_detailed.csv",
        ROOT_DIR / "outputs" / "reports" / "release_readiness.csv",
        ROOT_DIR / "outputs" / "reports" / "narrative_metrics_official.csv",
        ROOT_DIR / "data" / "processed" / "scoring_componentes.csv",
        ROOT_DIR / "data" / "processed" / "workshop_priority_table.csv",
        ROOT_DIR / "data" / "processed" / "component_rul_estimate.csv",
        ROOT_DIR / "data" / "processed" / "comparativo_estrategias.csv",
        ROOT_DIR / "data" / "processed" / "impacto_diferimiento_resumen.csv",
    ]
    manifest = _build_manifest(tracked)
    manifest.to_csv(OUTPUTS_REPORTS_DIR / "release_artifact_manifest.csv", index=False, quoting=csv.QUOTE_MINIMAL)

    checks = _run_checks()
    checks.to_csv(OUTPUTS_REPORTS_DIR / "release_hardening_checks.csv", index=False, quoting=csv.QUOTE_MINIMAL)

    failed = checks[~checks["passed"]]
    critical_failed = int(((checks["severity"] == "critica") & (~checks["passed"])).sum())
    high_failed = int(((checks["severity"] == "alta") & (~checks["passed"])).sum())
    report = [
        "# Release Hardening Report",
        "",
        f"- Fecha UTC: {datetime.now(timezone.utc).isoformat()}",
        f"- Checks ejecutados: {len(checks)}",
        f"- Fallos críticos: {critical_failed}",
        f"- Fallos altos: {high_failed}",
        f"- Estado release hardening: {'FAIL' if critical_failed > 0 else ('WARN' if high_failed > 0 else 'PASS')}",
        "",
        "## Failed Checks",
    ]
    if failed.empty:
        report.append("- Ninguno.")
    else:
        for _, row in failed.iterrows():
            report.append(
                f"- `{row['check_id']}` [{row['severity']}]: {row['detail']} | Acción: {row['recommendation']}"
            )

    report.extend(
        [
            "",
            "## Notes",
            "- Este reporte no sustituye `validation_report.md`; actúa como gate final de release package.",
            "- Manifesto usa hash SHA-256 para trazabilidad de artefactos críticos.",
        ]
    )
    (OUTPUTS_REPORTS_DIR / "release_hardening_report.md").write_text("\n".join(report), encoding="utf-8")

    payload = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "checks_total": int(len(checks)),
        "checks_failed": int((~checks["passed"]).sum()),
        "critical_failed": critical_failed,
        "high_failed": high_failed,
        "status": "FAIL" if critical_failed > 0 else ("WARN" if high_failed > 0 else "PASS"),
    }
    (OUTPUTS_REPORTS_DIR / "release_hardening_status.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


if __name__ == "__main__":
    run_release_hardening()

