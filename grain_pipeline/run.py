"""Pipeline orchestration.

Idempotency rests on four properties, all visible here:

* Target tables are written with CREATE OR REPLACE, never appended to.
* Deduplication and rate selection are deterministic, with explicit tiebreaks.
* No wall-clock or random value participates in any output column — the
  2026-06-01 cutoff is a literal, not a relative date.
* The target directory is created if absent, so a clean checkout runs.
"""

from __future__ import annotations

import time
from pathlib import Path

import duckdb

from .cleaning import apply_filters, deduplicate_trades
from .config import SOURCE_DB, SOURCE_SCHEMA, TARGET_DB
from .dimensions import build_dim_clients
from .facts import build_fact_daily_exposure, build_trades_enriched
from .logging_setup import configure_logging
from .quality import run_all_checks
from .rates import build_fx_to_usd
from .staging import (
    build_stg_clients,
    build_stg_fx_rates,
    build_stg_segment_changes,
    build_stg_trades,
)


def build_analytics(con: duckdb.DuckDBPyConnection, source_schema: str = SOURCE_SCHEMA) -> None:
    """Run every transformation against an open connection.

    Separated from connection management so the tests can drive the full
    pipeline against an in-memory database.
    """
    build_stg_clients(con, source_schema)
    build_stg_segment_changes(con, source_schema)
    build_stg_trades(con, source_schema)
    build_stg_fx_rates(con, source_schema)

    deduplicate_trades(con)
    apply_filters(con)

    build_dim_clients(con)
    build_fx_to_usd(con)

    build_trades_enriched(con)
    build_fact_daily_exposure(con)

    run_all_checks(con)


def run_pipeline(source_db: Path = SOURCE_DB, target_db: Path = TARGET_DB) -> None:
    """Read the raw database and write the analytical layer."""
    logger = configure_logging()
    started = time.perf_counter()

    if not source_db.exists():
        raise FileNotFoundError(
            f"Source database not found at {source_db}. "
            "Expected source_data/grain_raw.duckdb relative to the project root."
        )

    target_db.parent.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 78)
    logger.info("Grain analytics pipeline")
    logger.info("  source: %s", source_db)
    logger.info("  target: %s", target_db)
    logger.info("=" * 78)

    con = duckdb.connect(str(target_db))
    try:
        con.execute(f"ATTACH '{source_db}' AS {SOURCE_SCHEMA} (READ_ONLY)")
        try:
            build_analytics(con, SOURCE_SCHEMA)
        finally:
            con.execute(f"DETACH {SOURCE_SCHEMA}")
    finally:
        con.close()

    elapsed = time.perf_counter() - started
    logger.info("=" * 78)
    logger.info("Pipeline completed successfully in %.2fs.", elapsed)
    logger.info("=" * 78)
