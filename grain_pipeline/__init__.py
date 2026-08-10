"""Grain analytics pipeline.

Transforms the raw operational DuckDB database into an analytical layer holding
``dim_clients`` (a Type 2 slowly changing dimension) and ``fact_daily_exposure``
(a daily aggregate of FX exposure).

Design decisions and the data observations behind them are recorded in
DECISIONS.md at the project root.
"""

from __future__ import annotations

__all__ = ["run_pipeline"]

from .run import run_pipeline
