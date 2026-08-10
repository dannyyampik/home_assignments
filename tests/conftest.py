"""Shared test fixtures.

The source database is attached under the name ``src``, and DuckDB resolves
``src.raw_trades`` identically whether ``src`` is an attached database or a plain
schema. That means the tests can create an in-memory ``src`` schema and run the
*production* SQL against it unmodified — the tests exercise the real
transformation logic rather than a Python reimplementation of it.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

SOURCE_SCHEMA = "src"

_RAW_TABLES = {
    "raw_clients": """
        client_id   VARCHAR,
        client_name VARCHAR,
        segment     VARCHAR
    """,
    "raw_client_segment_changes": """
        client_id      VARCHAR,
        from_segment   VARCHAR,
        to_segment     VARCHAR,
        effective_date DATE
    """,
    "raw_fx_rates": """
        rate_id        INTEGER,
        base_currency  VARCHAR,
        quote_currency VARCHAR,
        mid_rate       DOUBLE,
        rate_date      DATE,
        source         VARCHAR
    """,
    "raw_trades": """
        trade_id            VARCHAR,
        client_id           VARCHAR,
        trade_date          DATE,
        status              VARCHAR,
        base_currency       VARCHAR,
        quote_currency      VARCHAR,
        amount              DOUBLE,
        amount_in_thousands BOOLEAN,
        agreed_rate         DOUBLE,
        created_at          TIMESTAMP
    """,
}


@pytest.fixture()
def con() -> duckdb.DuckDBPyConnection:
    """An in-memory database with an empty ``src`` schema mirroring the source."""
    connection = duckdb.connect(":memory:")
    connection.execute(f"CREATE SCHEMA {SOURCE_SCHEMA}")
    for table, columns in _RAW_TABLES.items():
        connection.execute(f"CREATE TABLE {SOURCE_SCHEMA}.{table} ({columns})")
    yield connection
    connection.close()


def insert(con: duckdb.DuckDBPyConnection, table: str, rows: list[tuple]) -> None:
    """Insert fixture rows into a raw source table."""
    if not rows:
        return
    placeholders = ", ".join(["?"] * len(rows[0]))
    con.executemany(f"INSERT INTO {SOURCE_SCHEMA}.{table} VALUES ({placeholders})", rows)


def trade(
    trade_id: str,
    client_id: str = "C001",
    trade_date: str = "2026-01-15",
    status: str = "ACTIVE",
    base_currency: str = "GBP",
    quote_currency: str = "USD",
    amount: float = 1000.0,
    amount_in_thousands: bool = False,
    agreed_rate: float = 1.25,
    created_at: str = "2025-10-01 09:00:00",
) -> tuple:
    """Build a raw trade row, overriding only what a test cares about."""
    return (
        trade_id,
        client_id,
        trade_date,
        status,
        base_currency,
        quote_currency,
        amount,
        amount_in_thousands,
        agreed_rate,
        created_at,
    )


def fx_rate(
    rate_id: int,
    base_currency: str,
    quote_currency: str,
    mid_rate: float | None,
    rate_date: str | None = "2026-01-15",
    source: str = "feed_a",
) -> tuple:
    """Build a raw FX rate row."""
    return (rate_id, base_currency, quote_currency, mid_rate, rate_date, source)
