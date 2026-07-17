"""Interfaz de línea de comandos del flujo CBM ferroviario."""

from __future__ import annotations

import argparse
from collections.abc import Sequence

from railway_cbm.config import RANDOM_SEED
from railway_cbm.ingestion import validate_external_snapshot
from railway_cbm.run_pipeline import run_pipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="railway-cbm", description="Ejecuta y valida el sistema CBM ferroviario.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Ejecuta el flujo completo")
    run_parser.add_argument("--source", choices=("synthetic", "external"), default="synthetic")
    run_parser.add_argument("--input-dir", help="Directorio del snapshot externo")
    run_parser.add_argument("--seed", type=int, default=RANDOM_SEED, help="Semilla de la fuente sintética")

    validate_parser = subparsers.add_parser("validate-input", help="Valida un snapshot externo sin copiarlo")
    validate_parser.add_argument("--input-dir", required=True, help="Directorio del snapshot externo")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "validate-input":
        tables = validate_external_snapshot(args.input_dir)
        row_count = sum(len(frame) for frame in tables.values())
        print(f"Snapshot válido: {len(tables)} tablas, {row_count:,} filas")
        return 0

    if args.source == "external" and not args.input_dir:
        parser.error("--input-dir es obligatorio cuando --source=external")
    if args.source == "synthetic" and args.input_dir:
        parser.error("--input-dir sólo se admite cuando --source=external")
    run_pipeline(source_mode=args.source, input_dir=args.input_dir, seed=args.seed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
