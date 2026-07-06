"""Guarda la propiedad central del repositorio: el flujo es reproducible.

Las agregaciones en coma flotante de DuckDB (AVG/SUM) suman en orden no
determinista cuando hay varias hebras, lo que filtra ruido en los últimos
dígitos hacia la puntuación, las métricas y la firma del panel de control. Fijar la
conexión a una sola hebra elimina esa fuente de no determinismo. Este test
asegura que la conexión de producción se configura así.
"""

from __future__ import annotations

import duckdb

from src.run_sql_layer import _configure_connection


def test_sql_connection_is_single_threaded_for_determinism():
    con = duckdb.connect()
    try:
        _configure_connection(con)
        threads = con.execute("SELECT current_setting('threads')").fetchone()[0]
    finally:
        con.close()
    assert int(threads) == 1
