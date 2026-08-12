#!/usr/bin/env python3
"""Grain analytics pipeline — entry point.

Usage, from the repository root:

    python pipeline.py

Reads   source_data/grain_raw.duckdb
Writes  target/grain_analytics.duckdb  (dim_clients, fact_daily_exposure)

The run is idempotent: executing it twice produces identical output.
Design decisions are documented in DECISIONS.md.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from grain_pipeline.pipeline.run import run_pipeline  # noqa: E402
from grain_pipeline.utils.logging_setup import configure_logging  # noqa: E402


def main() -> int:
    try:
        run_pipeline()
    except Exception as exc:  # noqa: BLE001 - top-level handler
        logger = configure_logging()
        logger.error("Pipeline failed: %s", exc, exc_info=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
