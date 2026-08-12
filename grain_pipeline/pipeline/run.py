"""Pipeline orchestration.

One call per model, in dependency order. Each model owns its own staging, its
own build and its own quality checks, so this function reads as a dependency
graph rather than as a procedure:

    dim_clients      (no upstream model)
    fx_to_usd        (no upstream model)
    fact_daily_exposure  (reads stg_clients, dim_clients, fx_to_usd)

Adding a model means adding a module and one line here. Nothing else in the
project needs to know about it.

Idempotency rests on four properties:

* Target tables are written with CREATE OR REPLACE, never appended to.
* Deduplication and rate selection are deterministic, with explicit tiebreaks.
* No wall-clock or random value participates in any output column — the
  2026-06-01 cutoff is a literal, not a relative date.
* The target directory is created if absent, so a clean checkout runs.

The whole build runs inside a single transaction, which is what makes the
quality checks protective rather than merely informative. DDL in DuckDB is
transactional, and ``CREATE OR REPLACE TABLE`` would otherwise auto-commit each
output table *before* the checks that guard it run — so a failing assertion
would abort the process only after the target had already been overwritten with
data it had just rejected. The previous, good tables would be gone and only a
non-zero exit code would say so. Building and checking inside one transaction
means a rejected load leaves the last known-good target intact (DECISIONS.md,
section 11.1).
"""

from __future__ import annotations

import time
from pathlib import Path

import duckdb

from ..utils.config import SOURCE_DB, SOURCE_SCHEMA, TARGET_DB
from ..utils.logging_setup import configure_logging, get_logger
from . import dim_clients, fact_daily_exposure, fx_to_usd


def build_analytics(con: duckdb.DuckDBPyConnection, source_schema: str = SOURCE_SCHEMA) -> None:
    """Run every model against an open connection, in dependency order.

    Separated from connection management so the tests can drive the full
    pipeline against an in-memory database.
    """
    dim_clients.build(con, source_schema)
    fx_to_usd.build(con, source_schema)
    fact_daily_exposure.build(con, source_schema)


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
            # Build and verify atomically. A quality check that fires must leave
            # the previous target untouched, not merely report that the target
            # it already replaced is wrong.
            con.execute("BEGIN TRANSACTION")
            try:
                build_analytics(con, SOURCE_SCHEMA)
            except Exception:
                con.execute("ROLLBACK")
                get_logger().error(
                    "Build rolled back. The target database is unchanged and still holds "
                    "the last successful load."
                )
                raise
            con.execute("COMMIT")
        finally:
            con.execute(f"DETACH {SOURCE_SCHEMA}")
    finally:
        con.close()

    elapsed = time.perf_counter() - started
    logger.info("=" * 78)
    logger.info("Pipeline completed successfully in %.2fs.", elapsed)
    logger.info("=" * 78)
