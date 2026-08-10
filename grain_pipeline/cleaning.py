"""Trade deduplication and the exclusion filter chain.

Filters run sequentially, each producing a counted step. A row that violates
several rules is attributed to the first one it fails, so the counts are
sequential rather than independent (DECISIONS.md, section 7). The order is fixed
so the reported numbers are reproducible.
"""

from __future__ import annotations

from dataclasses import dataclass

import duckdb

from .config import ACTIVE_STATUS, ISO_4217_PATTERN, TRADE_DATE_CUTOFF
from .logging_setup import get_logger


@dataclass(frozen=True)
class FilterStep:
    """One stage of the exclusion chain."""

    name: str
    predicate: str
    description: str


#: The chain, in the order it is applied.
FILTER_STEPS: tuple[FilterStep, ...] = (
    FilterStep(
        name="non_active_status",
        predicate=f"status = '{ACTIVE_STATUS}'",
        description="status is not ACTIVE",
    ),
    FilterStep(
        name="after_cutoff_date",
        predicate=f"trade_date IS NOT NULL AND trade_date <= DATE '{TRADE_DATE_CUTOFF.isoformat()}'",
        description=f"trade_date is missing or later than {TRADE_DATE_CUTOFF.isoformat()}",
    ),
    FilterStep(
        name="missing_client",
        predicate="client_id IS NOT NULL AND client_id IN (SELECT client_id FROM stg_clients)",
        description="client_id is missing or does not resolve to a known client",
    ),
    FilterStep(
        name="missing_or_invalid_currency",
        predicate=(
            f"base_currency IS NOT NULL AND regexp_matches(base_currency, '{ISO_4217_PATTERN}') "
            f"AND quote_currency IS NOT NULL AND regexp_matches(quote_currency, '{ISO_4217_PATTERN}')"
        ),
        description="base or quote currency is missing or not a valid ISO 4217 code",
    ),
    FilterStep(
        name="non_positive_amount",
        predicate="amount IS NOT NULL AND amount > 0",
        description="amount is missing, zero or negative",
    ),
)


def deduplicate_trades(con: duckdb.DuckDBPyConnection) -> None:
    """Reduce ``stg_trades`` to one row per ``trade_id`` in ``trades_deduplicated``.

    The earliest ``created_at`` wins. Deduplication runs *before* the status
    filter: "keep the earliest version" reads as a property of the raw record
    set, not of the filtered subset. In this dataset duplicate ``trade_id``
    values always share a status, so the two orderings are equivalent here — the
    choice is documented rather than incidental (DECISIONS.md, section 4).

    Idempotency requires a total ordering, and ``created_at`` alone is not
    guaranteed unique. The tiebreak is therefore content-based rather than
    positional: physical row order is not a guarantee the database owes us across
    runs, whereas ordering on the business columns is stable by construction.
    """
    logger = get_logger()

    before = _count(con, "stg_trades")
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE trades_identified AS
        SELECT * FROM stg_trades WHERE trade_id IS NOT NULL
        """
    )
    identified = _count(con, "trades_identified")
    if before != identified:
        logger.info(
            "Excluded %s rows with a missing trade_id (%s remaining).",
            f"{before - identified:,}",
            f"{identified:,}",
        )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE trades_deduplicated AS
        SELECT * EXCLUDE (_version_rank)
        FROM (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY trade_id
                    ORDER BY
                        created_at    NULLS LAST,
                        amount        NULLS LAST,
                        agreed_rate   NULLS LAST,
                        status        NULLS LAST,
                        base_currency NULLS LAST
                ) AS _version_rank
            FROM trades_identified
        )
        WHERE _version_rank = 1
        """
    )

    after = _count(con, "trades_deduplicated")
    logger.info(
        "Deduplicated trades: %s rows in, %s superseded versions removed, %s distinct trades out.",
        f"{identified:,}",
        f"{identified - after:,}",
        f"{after:,}",
    )


def apply_filters(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Run the exclusion chain, producing ``trades_clean``.

    Returns a mapping of step name to the number of rows it removed, so the
    caller can assert on the counts as well as log them.
    """
    logger = get_logger()
    exclusions: dict[str, int] = {}

    con.execute("CREATE OR REPLACE TEMP TABLE trades_filtered AS SELECT * FROM trades_deduplicated")
    remaining = _count(con, "trades_filtered")
    logger.info("Filter chain starting with %s trades.", f"{remaining:,}")

    for step in FILTER_STEPS:
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE trades_filtered_next AS
            SELECT * FROM trades_filtered WHERE {step.predicate}
            """
        )
        kept = _count(con, "trades_filtered_next")
        removed = remaining - kept
        exclusions[step.name] = removed

        logger.info(
            "  [%-28s] excluded %6s rows (%s) | %s remaining",
            step.name,
            f"{removed:,}",
            step.description,
            f"{kept:,}",
        )

        con.execute("DROP TABLE trades_filtered")
        con.execute("ALTER TABLE trades_filtered_next RENAME TO trades_filtered")
        remaining = kept

    con.execute("CREATE OR REPLACE TEMP TABLE trades_clean AS SELECT * FROM trades_filtered")
    logger.info(
        "Filter chain complete: %s trades excluded in total, %s retained.",
        f"{sum(exclusions.values()):,}",
        f"{remaining:,}",
    )
    return exclusions


def _count(con: duckdb.DuckDBPyConnection, table: str) -> int:
    result = con.execute(f"SELECT count(*) FROM {table}").fetchone()
    return int(result[0]) if result else 0
