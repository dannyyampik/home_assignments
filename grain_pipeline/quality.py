"""Data quality assertions.

These guard the failure modes that do *not* raise an error on their own. A
dimension fan-out or a duplicate rate row produces a plausible-looking table with
the wrong row count — no exception, nothing obviously odd on inspection, and a
number that is quietly wrong for as long as nobody checks. Asserting is the
difference between owning what happens when a check fails and merely running one.

Every check raises rather than warns. A partially-correct analytical table is
worse than an absent one, because it will be trusted.
"""

from __future__ import annotations

import duckdb

from .logging_setup import get_logger


class DataQualityError(RuntimeError):
    """Raised when a data quality assertion fails."""


def assert_rate_feed_unique(con: duckdb.DuckDBPyConnection) -> None:
    """After invalid rows are filtered, one rate per (base, quote, date).

    No source precedence rule is defined because, post-filter, none should be
    needed. If this fires, the right response is to define one deliberately
    rather than to have guessed at one silently.
    """
    duplicates = _scalar(
        con,
        """
        SELECT count(*) FROM (
            SELECT 1 FROM stg_fx_rates
            GROUP BY base_currency, quote_currency, rate_date
            HAVING count(*) > 1
        )
        """,
    )
    if duplicates:
        raise DataQualityError(
            f"{duplicates} (base_currency, quote_currency, rate_date) keys have more than one "
            "valid rate after cleaning. A source precedence rule is now required."
        )
    _ok("FX rate feed is unique on (base_currency, quote_currency, rate_date).")


def assert_dimension_intervals_valid(con: duckdb.DuckDBPyConnection) -> None:
    """No overlapping or gapped intervals within a client's timeline."""
    overlaps = _scalar(
        con,
        """
        SELECT count(*) FROM (
            SELECT
                effective_end_date,
                lead(effective_start_date) OVER (
                    PARTITION BY client_id ORDER BY effective_start_date
                ) AS next_start
            FROM dim_clients
        )
        WHERE next_start IS NOT NULL AND next_start <> effective_end_date
        """,
    )
    if overlaps:
        raise DataQualityError(
            f"{overlaps} client interval boundaries in dim_clients overlap or leave a gap. "
            "Point-in-time lookups would return the wrong segment or none at all."
        )

    inverted = _scalar(
        con,
        "SELECT count(*) FROM dim_clients WHERE effective_start_date >= effective_end_date",
    )
    if inverted:
        raise DataQualityError(f"{inverted} dim_clients rows have a non-positive interval length.")

    multiple_current = _scalar(
        con,
        """
        SELECT count(*) FROM (
            SELECT 1 FROM dim_clients WHERE is_current
            GROUP BY client_id HAVING count(*) > 1
        )
        """,
    )
    if multiple_current:
        raise DataQualityError(
            f"{multiple_current} clients have more than one current row in dim_clients."
        )

    _ok("dim_clients intervals are contiguous, non-overlapping and single-current per client.")


def reconcile_segment_chain(con: duckdb.DuckDBPyConnection) -> int:
    """Report, without failing, inconsistencies in the segment change log.

    Two things are checked: whether ``raw_clients.segment`` agrees with the
    earliest ``from_segment`` for each reclassified client, and whether the chain
    is continuous (each row's ``from_segment`` matching the previous row's
    ``to_segment``).

    These warn rather than raise, because neither can corrupt the output. The
    dimension is built from the change log alone and never reads
    ``raw_clients.segment`` for a reclassified client, so a mismatch is a
    statement about the reference table rather than about the dimension. Failing
    the load would block a correct result over an upstream inconsistency the
    pipeline has already routed around — but leaving it invisible would hide a
    real defect from whoever owns the source system.

    Returns the number of inconsistencies found.
    """
    logger = get_logger()

    origin_mismatches = _scalar(
        con,
        """
        WITH first_change AS (
            SELECT client_id, from_segment
            FROM stg_segment_changes
            QUALIFY row_number() OVER (PARTITION BY client_id ORDER BY effective_date) = 1
        )
        SELECT count(*)
        FROM first_change f
        JOIN stg_clients c USING (client_id)
        WHERE c.segment IS DISTINCT FROM f.from_segment
        """,
    )

    chain_breaks = _scalar(
        con,
        """
        SELECT count(*) FROM (
            SELECT
                from_segment,
                lag(to_segment) OVER (PARTITION BY client_id ORDER BY effective_date) AS prev_to
            FROM stg_segment_changes
        )
        WHERE prev_to IS NOT NULL AND prev_to IS DISTINCT FROM from_segment
        """,
    )

    total = origin_mismatches + chain_breaks
    if origin_mismatches:
        logger.warning(
            "  WARN  %s clients whose raw_clients.segment disagrees with their earliest "
            "from_segment. The dimension is unaffected; the reference table is inconsistent.",
            f"{origin_mismatches:,}",
        )
    if chain_breaks:
        logger.warning(
            "  WARN  %s discontinuities in the segment change chain (from_segment does not "
            "match the preceding to_segment).",
            f"{chain_breaks:,}",
        )
    if not total:
        _ok("Segment change chain is continuous and agrees with the client reference table.")

    return total


def assert_fact_grain_unique(con: duckdb.DuckDBPyConnection) -> None:
    """Exactly one fact row per (date, client, base_currency, quote_currency).

    This is the assertion that catches a dimension join fan-out, which is the
    highest-risk silent failure in this pipeline.
    """
    duplicates = _scalar(
        con,
        """
        SELECT count(*) FROM (
            SELECT 1 FROM fact_daily_exposure
            GROUP BY trade_date, client_id, base_currency, quote_currency
            HAVING count(*) > 1
        )
        """,
    )
    if duplicates:
        raise DataQualityError(
            f"{duplicates} (trade_date, client_id, base_currency, quote_currency) combinations "
            "appear more than once in fact_daily_exposure. The declared grain is violated — "
            "most likely a fan-out on the dim_clients join."
        )
    _ok("fact_daily_exposure is unique on its declared grain.")


def assert_no_trades_lost(con: duckdb.DuckDBPyConnection) -> None:
    """Fact trade counts reconcile exactly with the cleaned trade set.

    Catches both directions at once: rows dropped by the inner dimension join,
    and rows duplicated by a fan-out. Either would leave totals wrong without
    raising anything.
    """
    clean_trades = _scalar(con, "SELECT count(*) FROM trades_clean")
    fact_trades = _scalar(con, "SELECT coalesce(sum(trade_count), 0) FROM fact_daily_exposure")

    if clean_trades != fact_trades:
        raise DataQualityError(
            f"Trade count reconciliation failed: {clean_trades} cleaned trades but "
            f"{fact_trades} counted in fact_daily_exposure. Rows were lost or duplicated "
            "in the dimension join."
        )
    _ok(f"Trade counts reconcile: {clean_trades:,} cleaned trades accounted for in the fact table.")


def assert_conversions_consistent(con: duckdb.DuckDBPyConnection) -> None:
    """A resolved rate implies a converted amount, and vice versa."""
    inconsistent = _scalar(
        con,
        """
        SELECT count(*) FROM fact_daily_exposure
        WHERE (fx_rate_source = 'not_found' AND total_amount_usd IS NOT NULL)
           OR (fx_rate_source <> 'not_found' AND total_amount_usd IS NULL)
           OR (fx_rate_used IS NOT NULL AND fx_rate_used <= 0)
        """,
    )
    if inconsistent:
        raise DataQualityError(
            f"{inconsistent} fact rows have an inconsistent rate source, converted amount "
            "or a non-positive rate."
        )
    _ok("USD conversions are consistent with their recorded rate source.")


def run_all_checks(con: duckdb.DuckDBPyConnection) -> None:
    """Run every assertion in order. Raises on the first failure."""
    get_logger().info("Running data quality checks.")
    assert_rate_feed_unique(con)
    assert_dimension_intervals_valid(con)
    reconcile_segment_chain(con)
    assert_fact_grain_unique(con)
    assert_no_trades_lost(con)
    assert_conversions_consistent(con)
    get_logger().info("All data quality checks passed.")


def _scalar(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    row = con.execute(sql).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _ok(message: str) -> None:
    get_logger().info("  PASS  %s", message)
