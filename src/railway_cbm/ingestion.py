"""Valida y carga snapshots externos con el mismo contrato que la fuente sintética."""

from __future__ import annotations

import csv
import hashlib
import shutil
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from railway_cbm.config import DATA_PROCESSED_DIR, DATA_RAW_DIR


@dataclass(frozen=True)
class RawTableContract:
    required_columns: tuple[str, ...]
    primary_key: tuple[str, ...]
    foreign_keys: tuple[tuple[str, str, str], ...] = ()


RAW_TABLE_CONTRACTS: dict[str, RawTableContract] = {
    "flotas": RawTableContract(
        (
            "flota_id",
            "nombre_flota",
            "tipo_material",
            "operador",
            "region",
            "ano_fabricacion_base",
            "uso_intensidad",
            "criticidad_operativa",
            "estrategia_mantenimiento_actual",
        ),
        ("flota_id",),
    ),
    "depositos": RawTableContract(
        (
            "deposito_id",
            "nombre_deposito",
            "region",
            "capacidad_taller",
            "capacidad_inspeccion",
            "especializacion_tecnica",
            "carga_operativa_media",
        ),
        ("deposito_id",),
    ),
    "unidades": RawTableContract(
        (
            "unidad_id",
            "flota_id",
            "deposito_id",
            "linea_servicio",
            "fecha_entrada_servicio",
            "kilometraje_acumulado_km",
            "horas_operacion_acumuladas",
            "configuracion_unidad",
            "criticidad_servicio",
            "disponibilidad_objetivo",
        ),
        ("unidad_id",),
        (("flota_id", "flotas", "flota_id"), ("deposito_id", "depositos", "deposito_id")),
    ),
    "componentes_criticos": RawTableContract(
        (
            "componente_id",
            "unidad_id",
            "sistema_principal",
            "subsistema",
            "tipo_componente",
            "fabricante_proxy",
            "fecha_instalacion",
            "edad_componente_dias",
            "ciclos_acumulados",
            "criticidad_componente",
            "vida_util_teorica_dias",
            "vida_util_teorica_ciclos",
        ),
        ("componente_id",),
        (("unidad_id", "unidades", "unidad_id"),),
    ),
    "sensores_componentes": RawTableContract(
        (
            "timestamp",
            "unidad_id",
            "componente_id",
            "sensor_tipo",
            "valor_sensor",
            "temperatura_operacion",
            "vibracion_proxy",
            "presion_proxy",
            "desgaste_proxy",
            "corriente_proxy",
            "ruido_proxy",
            "velocidad_operativa",
            "carga_operativa",
            "ambiente_externo_proxy",
        ),
        ("timestamp", "unidad_id", "componente_id", "sensor_tipo"),
        (("unidad_id", "unidades", "unidad_id"), ("componente_id", "componentes_criticos", "componente_id")),
    ),
    "inspecciones_automaticas": RawTableContract(
        (
            "inspeccion_id",
            "timestamp",
            "unidad_id",
            "componente_id",
            "familia_inspeccion",
            "detector_origen",
            "severidad_hallazgo",
            "score_defecto",
            "defecto_detectado",
            "confianza_deteccion",
            "recomendacion_inicial",
        ),
        ("inspeccion_id",),
        (("unidad_id", "unidades", "unidad_id"), ("componente_id", "componentes_criticos", "componente_id")),
    ),
    "eventos_mantenimiento": RawTableContract(
        (
            "mantenimiento_id",
            "unidad_id",
            "componente_id",
            "deposito_id",
            "fecha_inicio",
            "fecha_fin",
            "tipo_mantenimiento",
            "motivo_intervencion",
            "correctiva_flag",
            "basada_en_condicion_flag",
            "programada_flag",
            "horas_taller",
            "coste_mano_obra_proxy",
            "coste_material_proxy",
            "resultado_intervencion",
        ),
        ("mantenimiento_id",),
        (
            ("unidad_id", "unidades", "unidad_id"),
            ("componente_id", "componentes_criticos", "componente_id"),
            ("deposito_id", "depositos", "deposito_id"),
        ),
    ),
    "fallas_historicas": RawTableContract(
        (
            "falla_id",
            "unidad_id",
            "componente_id",
            "fecha_falla",
            "modo_falla",
            "severidad_falla",
            "impacto_en_servicio",
            "tiempo_fuera_servicio_horas",
            "causa_raiz_proxy",
            "repetitiva_flag",
        ),
        ("falla_id",),
        (("unidad_id", "unidades", "unidad_id"), ("componente_id", "componentes_criticos", "componente_id")),
    ),
    "alertas_operativas": RawTableContract(
        (
            "alerta_id",
            "timestamp",
            "unidad_id",
            "componente_id",
            "tipo_alerta",
            "severidad",
            "trigger_origen",
            "alerta_temprana_flag",
            "accion_sugerida",
            "atendida_flag",
        ),
        ("alerta_id",),
        (("unidad_id", "unidades", "unidad_id"), ("componente_id", "componentes_criticos", "componente_id")),
    ),
    "intervenciones_programadas": RawTableContract(
        (
            "intervencion_id",
            "unidad_id",
            "componente_id",
            "deposito_id",
            "fecha_programada",
            "prioridad_planificada",
            "estado_intervencion",
            "ventana_operativa_disponible",
            "impacto_si_no_se_ejecuta",
        ),
        ("intervencion_id",),
        (
            ("unidad_id", "unidades", "unidad_id"),
            ("componente_id", "componentes_criticos", "componente_id"),
            ("deposito_id", "depositos", "deposito_id"),
        ),
    ),
    "disponibilidad_servicio": RawTableContract(
        (
            "fecha",
            "unidad_id",
            "flota_id",
            "linea_servicio",
            "horas_planificadas",
            "horas_disponibles",
            "horas_no_disponibles",
            "motivo_no_disponibilidad",
            "cancelaciones_proxy",
            "puntualidad_impactada_proxy",
        ),
        ("fecha", "unidad_id"),
        (("unidad_id", "unidades", "unidad_id"), ("flota_id", "flotas", "flota_id")),
    ),
    "asignacion_servicio": RawTableContract(
        (
            "fecha",
            "unidad_id",
            "linea_servicio",
            "servicio_planificado",
            "servicio_realizado",
            "reserva_flag",
            "sustitucion_requerida_flag",
        ),
        ("fecha", "unidad_id"),
        (("unidad_id", "unidades", "unidad_id"),),
    ),
    "backlog_mantenimiento": RawTableContract(
        (
            "fecha",
            "backlog_id",
            "deposito_id",
            "unidad_id",
            "componente_id",
            "tipo_pendencia",
            "antiguedad_backlog_dias",
            "severidad_pendiente",
            "riesgo_acumulado",
        ),
        ("fecha", "backlog_id"),
        (
            ("unidad_id", "unidades", "unidad_id"),
            ("componente_id", "componentes_criticos", "componente_id"),
            ("deposito_id", "depositos", "deposito_id"),
        ),
    ),
    "parametros_operativos_contexto": RawTableContract(
        (
            "fecha",
            "linea_servicio",
            "region",
            "temperatura_ambiente",
            "humedad",
            "tipo_explotacion",
            "intensidad_servicio",
            "nivel_congestion_operativa_proxy",
        ),
        ("fecha", "linea_servicio"),
    ),
    "escenarios_mantenimiento": RawTableContract(
        (
            "fecha",
            "escenario",
            "intensidad_operacion_indice",
            "tension_backlog_indice",
            "disponibilidad_recursos_indice",
            "presion_coste_indice",
        ),
        ("fecha", "escenario"),
    ),
}

APPROVAL_COLUMNS = (
    "decision_id",
    "approval_status",
    "reviewer_id",
    "reviewed_at",
    "comment",
)
VALID_APPROVAL_STATUSES = {"approved", "rejected", "escalated"}


def validate_approval_events(approvals: pd.DataFrame) -> pd.DataFrame:
    """Valida y normaliza eventos de revisión humana."""
    missing = sorted(set(APPROVAL_COLUMNS) - set(approvals.columns))
    if missing:
        raise ValueError(f"decision_approvals.csv no cumple el esquema: {missing}")
    invalid_statuses = sorted(set(approvals["approval_status"].dropna()) - VALID_APPROVAL_STATUSES)
    if invalid_statuses:
        raise ValueError(f"Estados de aprobación no permitidos: {invalid_statuses}")
    reviewed_at = pd.to_datetime(approvals["reviewed_at"], errors="coerce", utc=True)
    invalid_rows = (
        approvals["decision_id"].isna()
        | approvals["reviewer_id"].fillna("").astype(str).str.strip().eq("")
        | reviewed_at.isna()
    )
    if invalid_rows.any():
        raise ValueError(f"decision_approvals.csv contiene {int(invalid_rows.sum())} revisiones incompletas")
    normalized = approvals.copy()
    normalized["decision_id"] = normalized["decision_id"].astype(str)
    normalized["reviewer_id"] = normalized["reviewer_id"].astype(str)
    normalized["reviewed_at"] = reviewed_at.dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    if normalized.duplicated(["decision_id", "reviewed_at"]).any():
        raise ValueError("decision_approvals.csv contiene eventos de revisión duplicados")
    return normalized


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_validated_csv(path: Path, contract: RawTableContract) -> pd.DataFrame:
    try:
        with path.open("r", encoding="utf-8", newline="") as stream:
            header = next(csv.reader(stream))
    except (OSError, StopIteration, UnicodeDecodeError) as exc:
        raise ValueError(f"No se puede leer {path.name} como CSV UTF-8: {exc}") from exc

    duplicates = sorted({column for column in header if header.count(column) > 1})
    if duplicates:
        raise ValueError(f"{path.name} contiene columnas duplicadas: {duplicates}")

    missing = sorted(set(contract.required_columns) - set(header))
    if missing:
        raise ValueError(f"{path.name} no cumple el esquema; faltan columnas: {missing}")

    temporal_columns = [
        column for column in contract.required_columns if column == "timestamp" or column.startswith("fecha")
    ]
    integrity_columns = (
        set(contract.primary_key) | set(temporal_columns) | {column for column, _, _ in contract.foreign_keys}
    )
    usecols = [column for column in header if column in integrity_columns]
    frame = pd.read_csv(path, usecols=usecols, low_memory=False)
    if frame.empty:
        raise ValueError(f"{path.name} no contiene filas")
    if frame[list(contract.primary_key)].isna().any().any():
        raise ValueError(f"{path.name} contiene claves primarias nulas: {contract.primary_key}")
    if frame.duplicated(list(contract.primary_key)).any():
        raise ValueError(f"{path.name} contiene claves primarias duplicadas: {contract.primary_key}")

    for column in temporal_columns:
        parsed = pd.to_datetime(frame[column], errors="coerce")
        invalid = int(frame[column].notna().sum() - parsed.notna().sum())
        if invalid:
            raise ValueError(f"{path.name}.{column} contiene {invalid} valores temporales inválidos")
    return frame


def validate_external_snapshot(source_dir: str | Path) -> dict[str, pd.DataFrame]:
    """Valida estructura, claves y referencias sin modificar el área gestionada."""
    source = Path(source_dir).expanduser().resolve()
    if not source.is_dir():
        raise NotADirectoryError(f"El directorio de entrada externo no existe: {source}")

    raw = DATA_RAW_DIR.resolve()
    if source == raw or source.is_relative_to(raw) or raw.is_relative_to(source):
        raise ValueError("La fuente externa debe estar fuera de data/raw para evitar sobrescribir la entrada")

    missing_files = [f"{name}.csv" for name in RAW_TABLE_CONTRACTS if not (source / f"{name}.csv").is_file()]
    if missing_files:
        raise FileNotFoundError(f"Faltan tablas obligatorias en el snapshot externo: {missing_files}")

    tables = {
        name: _read_validated_csv(source / f"{name}.csv", contract) for name, contract in RAW_TABLE_CONTRACTS.items()
    }
    for table_name, contract in RAW_TABLE_CONTRACTS.items():
        frame = tables[table_name]
        for column, parent_table, parent_column in contract.foreign_keys:
            child_values = set(frame[column].dropna().astype(str))
            parent_values = set(tables[parent_table][parent_column].dropna().astype(str))
            orphan_count = len(child_values - parent_values)
            if orphan_count:
                raise ValueError(
                    f"{table_name}.{column} contiene {orphan_count} valores sin referencia en "
                    f"{parent_table}.{parent_column}"
                )

    approvals_path = source / "decision_approvals.csv"
    if approvals_path.exists():
        validate_approval_events(pd.read_csv(approvals_path, dtype={"decision_id": str, "reviewer_id": str}))
    return tables


def _copy_external_snapshot(source_dir: str | Path) -> None:
    """Copia un snapshot ya validado al área gestionada."""
    source = Path(source_dir).expanduser().resolve()
    DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)
    for table_name in RAW_TABLE_CONTRACTS:
        shutil.copy2(source / f"{table_name}.csv", DATA_RAW_DIR / f"{table_name}.csv")
    approvals_path = source / "decision_approvals.csv"
    if approvals_path.exists():
        shutil.copy2(approvals_path, DATA_RAW_DIR / approvals_path.name)


def ingest_external_snapshot(source_dir: str | Path) -> None:
    """Valida y copia un snapshot externo al área gestionada."""
    validate_external_snapshot(source_dir)
    _copy_external_snapshot(source_dir)


def write_input_manifest(*, source_mode: str, source_name: str) -> pd.DataFrame:
    """Registra hashes y cardinalidad del snapshot efectivo usado por el flujo."""
    rows: list[dict[str, object]] = []
    for path in sorted(DATA_RAW_DIR.glob("*.csv")):
        with path.open("r", encoding="utf-8", newline="") as stream:
            reader = csv.reader(stream)
            header = next(reader)
            row_count = sum(1 for _ in reader)
        rows.append(
            {
                "source_mode": source_mode,
                "source_name": source_name,
                "file_name": path.name,
                "row_count": row_count,
                "column_count": len(header),
                "bytes": path.stat().st_size,
                "sha256": _sha256(path),
            }
        )
    manifest = pd.DataFrame(rows)
    if manifest.empty:
        raise RuntimeError("No hay archivos de entrada para registrar")
    manifest.to_csv(DATA_PROCESSED_DIR / "input_data_manifest.csv", index=False)
    return manifest
