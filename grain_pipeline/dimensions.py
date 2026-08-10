"""``dim_clients`` — a Type 2 slowly changing dimension.

``raw_clients.segment`` matches ``from_segment`` in the change log, which
establishes that the base table holds the client's *original* classification, not
their current one. History is therefore built **forward**: ``from_segment`` holds
until the change date, and ``to_segment`` from the change date onward.

This has a consequence worth stating plainly. ``raw_clients`` is not a
current-state reference table, so joining ``raw_clients.segment`` straight onto
trades would stamp every trade with the client's *original* segment regardless of
when it happened — the mirror image of the naive-current-segment bug the
requirement warns about, and just as wrong. The dimension is therefore built from
the change log, which is self-describing: every interval's segment comes from
``from_segment`` or ``to_segment``, never from the reference table, except for
clients that were never reclassified and for whom the two agree by definition.

Intervals are half-open — ``effective_start_date <= trade_date <
effective_end_date`` — the only convention under which a trade falling exactly on
a change date cannot match two dimension rows (DECISIONS.md, section 3.3).

The current dataset holds at most one change per client, but the closing logic is
written generically with ``lead()`` so that a client reclassified twice produces a
correct interval chain without modification. This costs nothing today and avoids a
rebuild the first time the assumption breaks.
"""

from __future__ import annotations

import duckdb

from .config import DATE_CEILING, DATE_FLOOR
from .logging_setup import get_logger


def build_dim_clients(con: duckdb.DuckDBPyConnection) -> None:
    """Build ``dim_clients`` from ``stg_clients`` and ``stg_segment_changes``."""
    floor_literal = f"DATE '{DATE_FLOOR.isoformat()}'"
    ceiling_literal = f"DATE '{DATE_CEILING.isoformat()}'"

    con.execute(
        f"""
        CREATE OR REPLACE TABLE dim_clients AS
        WITH ordered_changes AS (
            SELECT
                client_id,
                from_segment,
                to_segment,
                effective_date,
                row_number() OVER (
                    PARTITION BY client_id ORDER BY effective_date
                ) AS change_seq,
                lead(effective_date) OVER (
                    PARTITION BY client_id ORDER BY effective_date
                ) AS next_effective_date
            FROM stg_segment_changes
        ),

        -- The interval before a client's first recorded change, carrying the
        -- segment they held at the time.
        pre_change_intervals AS (
            SELECT
                client_id,
                from_segment        AS segment,
                {floor_literal}     AS effective_start_date,
                effective_date      AS effective_end_date
            FROM ordered_changes
            WHERE change_seq = 1
        ),

        -- One interval per change, running until the next change or, for the
        -- most recent change, until the ceiling sentinel.
        post_change_intervals AS (
            SELECT
                client_id,
                to_segment                                      AS segment,
                effective_date                                  AS effective_start_date,
                coalesce(next_effective_date, {ceiling_literal}) AS effective_end_date
            FROM ordered_changes
        ),

        -- Clients that were never reclassified get a single all-time interval.
        unchanged_intervals AS (
            SELECT
                c.client_id,
                c.segment,
                {floor_literal}   AS effective_start_date,
                {ceiling_literal} AS effective_end_date
            FROM stg_clients c
            WHERE c.client_id NOT IN (SELECT client_id FROM stg_segment_changes)
        ),

        all_intervals AS (
            SELECT * FROM pre_change_intervals
            UNION ALL
            SELECT * FROM post_change_intervals
            UNION ALL
            SELECT * FROM unchanged_intervals
        )

        SELECT
            i.client_id,
            c.client_name,
            i.segment,
            i.effective_start_date,
            i.effective_end_date,
            (i.effective_end_date = {ceiling_literal}) AS is_current
        FROM all_intervals i
        LEFT JOIN stg_clients c USING (client_id)
        -- A change recorded on the floor date would yield a zero-length
        -- pre-change interval, which can never match a trade. Dropping it keeps
        -- the dimension free of unreachable rows.
        WHERE i.effective_start_date < i.effective_end_date
        ORDER BY i.client_id, i.effective_start_date
        """
    )

    logger = get_logger()
    summary = con.execute(
        """
        SELECT
            count(*)                                          AS interval_rows,
            count(DISTINCT client_id)                         AS clients,
            count(*) FILTER (WHERE is_current)                AS current_rows,
            count(*) FILTER (WHERE segment IS NULL)           AS null_segment_rows
        FROM dim_clients
        """
    ).fetchone()

    if summary:
        logger.info(
            "Built dim_clients: %s interval rows across %s clients (%s current). %s rows with an unknown segment.",
            f"{summary[0]:,}",
            f"{summary[1]:,}",
            f"{summary[2]:,}",
            f"{summary[3]:,}",
        )
