import base64
import re
import subprocess
import xml.etree.ElementTree as ET
from io import BytesIO
from pathlib import Path

import pandas as pd
from PIL import Image

ROOT = Path(__file__).resolve().parents[1]


def test_archivos_clave_existen():
    expected = [
        ROOT / "data" / "processed" / "scoring_componentes.csv",
        ROOT / "data" / "processed" / "component_rul_estimate.csv",
        ROOT / "data" / "processed" / "workshop_priority_table.csv",
        ROOT / "data" / "processed" / "workshop_scheduling_recommendation.csv",
        ROOT / "data" / "processed" / "input_data_manifest.csv",
        ROOT / "data" / "processed" / "pipeline_execution_manifest.csv",
        ROOT / "data" / "processed" / "model_deployment_gate.csv",
        ROOT / "data" / "processed" / "capacity_optimization_gate.csv",
        ROOT / "data" / "processed" / "formal_capacity_allocation.csv",
        ROOT / "data" / "processed" / "decision_register.csv",
        ROOT / "outputs" / "dashboard" / "centro-control-mantenimiento-ferroviario.html",
    ]
    for file in expected:
        assert file.exists(), f"No existe: {file}"


def test_graficos_publicados_existen_y_no_estan_vacios():
    graph_dir = ROOT / "outputs" / "graphs"
    expected = {
        "01_tendencia_disponibilidad.png",
        "02_valor_estrategias.png",
        "03_coste_diferimiento.png",
        "04_ranking_intervenciones.png",
        "05_saturacion_depositos.png",
        "06_distribucion_riesgo_unidades.png",
        "07_scheduling_antes_despues.png",
        "08_determinantes_riesgo.png",
        "09_salud_vs_riesgo.png",
        "10_riesgo_por_familia.png",
        "11_concentracion_backlog.png",
        "12_pareto_fallos_repetitivos.png",
        "13_cohortes_rul_familia.png",
        "14_rul_antes_despues.png",
        "15_inspeccion_por_familia.png",
        "16_utilizacion_capacidad.png",
        "17_ranking_indisponibilidad.png",
        "18_variancia_escenarios.png",
        "19_gobernanza_validaciones.png",
    }
    found = {path.name for path in graph_dir.glob("*.png")}
    assert found == expected
    assert all((graph_dir / name).stat().st_size > 20_000 for name in expected)


def test_publication_directories_contain_only_final_artifacts():
    reports = sorted(path.name for path in (ROOT / "outputs" / "reports").iterdir())
    dashboards = sorted(path.name for path in (ROOT / "outputs" / "dashboard").iterdir())
    assert reports == ["informe_analitico_cbm_ferroviario.pdf"]
    assert dashboards == ["centro-control-mantenimiento-ferroviario.html"]


def test_final_report_uses_widescreen_consulting_layout():
    report = ROOT / "outputs" / "reports" / "informe_analitico_cbm_ferroviario.pdf"
    result = subprocess.run(
        ["pdfinfo", str(report)],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    match = re.search(r"Page size:\s+([\d.]+) x ([\d.]+) pts", result.stdout)
    assert match, "pdfinfo no devolvió las dimensiones de página"
    width, height = (float(value) for value in match.groups())
    assert width > height
    assert abs((width / height) - (16 / 9)) < 0.02


def test_report_section_headings_start_at_the_top_of_a_page():
    report = ROOT / "outputs" / "reports" / "informe_analitico_cbm_ferroviario.pdf"
    result = subprocess.run(
        ["pdftotext", "-bbox-layout", str(report), "-"],
        check=True,
        capture_output=True,
    )
    root = ET.fromstring(result.stdout)
    headings = {
        "1. Contexto y objetivos",
        "2. Datos y metodología",
        "3. Marco analítico",
        "4. Diagnóstico operativo",
        "5. Fiabilidad, inspección y vida remanente",
        "6. Priorización y capacidad de taller",
        "7. Economía y decisión estratégica",
        "8. Riesgos, limitaciones y cautelas",
        "9. Recomendaciones y prioridades de acción",
        "Apéndice. Métricas, fuentes e interpretación",
    }
    found: dict[str, float] = {}
    pages = [element for element in root.iter() if element.tag.endswith("page")]
    for page in pages[2:]:
        words = [
            (element.text or "", float(element.attrib["yMin"]))
            for element in page.iter()
            if element.tag.endswith("word")
        ]
        tokens = [text for text, _ in words]
        for heading in headings - found.keys():
            target = heading.split()
            for index in range(len(tokens) - len(target) + 1):
                if tokens[index : index + len(target)] == target:
                    found[heading] = words[index][1]
                    break

    assert found.keys() == headings
    assert all(y_position < 100 for y_position in found.values()), found


def test_report_embeds_charts_with_the_editorial_blue_palette():
    from runpy import run_path

    b64_img = run_path(str(ROOT / "scripts" / "build_report_pdf.py"))["b64_img"]
    encoded = b64_img(ROOT / "outputs" / "graphs" / "01_tendencia_disponibilidad.png")
    image = Image.open(BytesIO(base64.b64decode(encoded))).convert("RGB")
    color_counts = image.getcolors(maxcolors=image.width * image.height)
    assert color_counts is not None
    colors = {color for _, color in color_counts}
    assert (30, 73, 226) in colors
    assert (36, 99, 212) not in colors


def test_final_report_preserves_all_nineteen_figures_and_inspection_analysis():
    report = ROOT / "outputs" / "reports" / "informe_analitico_cbm_ferroviario.pdf"
    image_list = subprocess.run(
        ["pdfimages", "-list", str(report)],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    ).stdout
    primary_images = [line for line in image_list.splitlines() if len(line.split()) >= 3 and line.split()[2] == "image"]

    extracted_text = subprocess.run(
        ["pdftotext", "-layout", str(report), "-"],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    ).stdout

    assert len(primary_images) == 19
    assert extracted_text.count("Figura.") == 19
    assert "La inspección automática no rinde igual en todas las familias" in extracted_text


def test_executive_action_callouts_are_not_orphaned_on_sparse_pages():
    report = ROOT / "outputs" / "reports" / "informe_analitico_cbm_ferroviario.pdf"
    result = subprocess.run(
        ["pdftotext", "-layout", str(report), "-"],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    callout_pages = [page for page in result.stdout.split("\f") if "ACCIÓN EJECUTIVA" in page]
    assert callout_pages
    assert all(len(page.split()) >= 80 for page in callout_pages)


def test_rangos_scoring():
    df = pd.read_csv(ROOT / "data" / "processed" / "scoring_componentes.csv")
    assert df["health_score"].between(0, 100).all()
    assert df["prob_fallo_30d"].between(0, 1).all()
    assert df["riesgo_ajustado_negocio"].between(0, 100).all()


def test_rul_y_priorizacion():
    rul = pd.read_csv(ROOT / "data" / "processed" / "component_rul_estimate.csv")
    prio = pd.read_csv(ROOT / "data" / "processed" / "workshop_priority_table.csv")
    assert (rul["component_rul_estimate"] > 0).all()
    assert prio["intervention_priority_score"].between(0, 100).all()


def test_legacy_alias_outputs_are_absent():
    legacy = {
        "rul_instancia.csv",
        "priorizacion_intervenciones.csv",
        "plan_taller_14d.csv",
    }
    found = {path.name for path in (ROOT / "data" / "processed").iterdir()}
    assert legacy.isdisjoint(found)


def test_synthetic_model_is_blocked_from_autonomous_use():
    gate = pd.read_csv(ROOT / "data" / "processed" / "model_deployment_gate.csv").iloc[0]
    assert gate["source_mode"] == "synthetic"
    assert not bool(gate["autonomous_use_allowed"])
    assert gate["operating_mode"] == "shadow"


def test_formal_capacity_and_decisions_respect_operational_guards():
    utilization = pd.read_csv(ROOT / "data" / "processed" / "formal_capacity_utilization.csv")
    decisions = pd.read_csv(ROOT / "data" / "processed" / "decision_register.csv")
    assert utilization["utilization"].between(0, 1).all()
    assert decisions["operating_mode"].eq("shadow").all()
    assert not decisions["auto_execution_allowed"].any()
    assert not decisions["execution_authorized"].any()


def test_backlog_snapshot_has_unique_order_identity():
    backlog = pd.read_csv(ROOT / "data" / "raw" / "backlog_mantenimiento.csv")
    assert backlog["backlog_id"].notna().all()
    assert not backlog.duplicated(["fecha", "backlog_id"]).any()


def test_synthetic_plausibility_checks_pass():
    checks = pd.read_csv(ROOT / "data" / "raw" / "validaciones_plausibilidad.csv")
    assert checks["aprobado"].astype(bool).all(), checks.loc[~checks["aprobado"].astype(bool)].to_dict(orient="records")


def test_sql_blocking_validations_pass():
    zero_outputs = [
        "val_null_rates_critical.csv",
        "val_sensor_ranges.csv",
        "val_temporal_coherence.csv",
        "val_backlog_semantic_consistency.csv",
        "val_primary_key_uniqueness.csv",
        "val_referential_integrity.csv",
        "val_join_cardinality.csv",
        "val_metric_ranges.csv",
        "val_business_metric_coherence.csv",
    ]
    for name in zero_outputs:
        df = pd.read_csv(ROOT / "data" / "processed" / name)
        assert (df.select_dtypes(include="number") == 0).all().all(), f"Validación SQL fallida: {name}"


def test_sql_validation_exports_are_blocking_gates():
    from railway_cbm.run_sql_layer import EXPORT_OBJECTS

    required = {
        "val_primary_key_uniqueness",
        "val_referential_integrity",
        "val_join_cardinality",
        "val_metric_ranges",
        "val_business_metric_coherence",
    }
    assert required.issubset(set(EXPORT_OBJECTS))


def test_mtbf_proxy_reconciles_to_available_hours_per_failure():
    unit_day = pd.read_csv(ROOT / "data" / "processed" / "mart_unit_day.csv")
    fleet_week = pd.read_csv(ROOT / "data" / "processed" / "mart_fleet_week.csv")
    unit_day["week_start"] = pd.to_datetime(unit_day["fecha"]).dt.to_period("W-SUN").dt.start_time
    expected = unit_day.groupby(["week_start", "flota_id"], as_index=False).agg(
        available_hours=("horas_disponibles", "sum"),
        failures_count=("failures_count", "sum"),
    )
    expected["expected_mtbf"] = expected["available_hours"] / expected["failures_count"].replace(0, pd.NA)
    fleet_week["week_start"] = pd.to_datetime(fleet_week["week_start"])
    merged = fleet_week.merge(expected, on=["week_start", "flota_id"], how="inner")
    assert (merged["failures_count_x"] == merged["failures_count_y"]).all()
    with_failures = merged["failures_count_x"] > 0
    error = (merged.loc[with_failures, "mtbf_proxy"] - merged.loc[with_failures, "expected_mtbf"]).abs().max()
    assert float(error) <= 1e-6
