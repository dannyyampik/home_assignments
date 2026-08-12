"""``dim_clients`` — a Type 2 slowly changing dimension.

**Reads**  ``src.raw_clients``, ``src.raw_client_segment_changes``
**Writes** ``stg_clients``, ``stg_segment_changes`` (temp), ``dim_clients``
**Checks** change-log orderability and interval integrity (raise), segment chain
           reconciliation (warns)

``stg_clients`` is also read by ``fact_daily_exposure`` for its
"client resolves to a known client" exclusion. That is a genuine cross-model
dependency and the reason ``run.py`` builds this model first — the same way a
fact depends on its conformed dimension in any warehouse.

----

``raw_clients.segment`` matches ``from_segment`` in the change log, which
establishes that the base table holds the client's *original* classification,
not their current one. History is therefore built **forward**: ``from_segment``
holds until the change date, and ``to_segment`` from the change date onward.

This has a consequence worth stating plainly. ``raw_clients`` is not a
current-state reference table, so joining ``raw_clients.segment`` straight onto
trades would stamp every trade with the client's *original* segment regardless of
when it happened — the mirror image of the naive-current-segment bug the
requirement warns about, and just as wrong.

Both sources are therefore needed, for different things:

* ``raw_clients`` defines the **client universe** and supplies names.
* ``raw_client_segment_changes`` supplies the **history** for clients that have
  one; each interval's segment comes from ``from_segment`` or ``to_segment``, so
  the history is self-describing and does not depend on how
  ``raw_clients.segment`` is interpreted.
* A client with **no** change row has no history to reconstruct, and the change
  log says nothing about them. Their single all-time interval necessarily takes
  its segment from ``raw_clients`` — which is unambiguous precisely because,
  never having been reclassified, their original and current segment are the
  same value.

Intervals are half-open — ``effective_start_date <= trade_date <
effective_end_date`` — the only convention under which a trade falling exactly on
a change date cannot match two dimension rows (DECISIONS.md, section 3.4).

The current dataset holds at most one change per client, but the closing logic is
written generically with ``lead()`` so that a client reclassified twice produces a
correct interval chain without modification. This costs nothing today and avoids a
rebuild the first time the assumption breaks.

That generality has one boundary, and it is enforced rather than assumed: two
changes for one client on the *same* date cannot be ordered, because the change
log carries no sequence column. ``assert_change_log_orderable`` fails the load
instead of resolving the tie arbitrarily.
"""

from __future__ import annotations

import duckdb

from ..utils.config import DATE_CEILING, DATE_FLOOR, SOURCE_SCHEMA
from ..utils.logging_setup import get_logger
from ..utils.quality import passed, require, warn
from ..utils.sql import canonical_text, row_count, scalar


# --- staging ---------------------------------------------------------------


def stage_clients(con: duckdb.DuckDBPyConnection, source_schema: str = SOURCE_SCHEMA) -> None:
    """Normalise and deduplicate client reference data into ``stg_clients``.

    The source contains the same logical client under case and whitespace
    variants of one identifier ('c007'/'C007', 'C003'/'C003 '). Canonicalising
    collapses them. The duplicate pairs agree on name and segment, so there is no
    survivorship conflict; ``any_value`` is safe and its choice is immaterial.

    Collapsing here is what prevents the dimension join from fanning out and
    silently duplicating fact rows (DECISIONS.md, section 2).
    """
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE stg_clients AS
        SELECT
            {canonical_text('client_id')}     AS client_id,
            any_value(nullif(trim(client_name), ''))  AS client_name,
            any_value(nullif(trim(segment), ''))      AS segment
        FROM {source_schema}.raw_clients
        WHERE {canonical_text('client_id')} IS NOT NULL
        GROUP BY 1
        """
    )
    raw = scalar(con, f"SELECT count(*) FROM {source_schema}.raw_clients")
    staged = row_count(con, "stg_clients")
    get_logger().info(
        "Staged clients: %s raw rows collapsed to %s canonical clients "
        "(%s duplicate identifier variants merged).",
        f"{raw:,}",
        f"{staged:,}",
        f"{raw - staged:,}",
    )


def stage_segment_changes(
    con: duckdb.DuckDBPyConnection, source_schema: str = SOURCE_SCHEMA
) -> None:
    """Normalise the segment change log into ``stg_segment_changes``.

    Rows with no effective date cannot be placed on a timeline and are dropped.
    """
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE stg_segment_changes AS
        SELECT
            {canonical_text('client_id')}       AS client_id,
            nullif(trim(from_segment), '')      AS from_segment,
            nullif(trim(to_segment), '')        AS to_segment,
            effective_date
        FROM {source_schema}.raw_client_segment_changes
        WHERE {canonical_text('client_id')} IS NOT NULL
          AND effective_date IS NOT NULL
        """
    )
    get_logger().info(
        "Staged segment changes: %s rows.", f"{row_count(con, 'stg_segment_changes'):,}"
    )


# --- build -----------------------------------------------------------------


def build_dimension(con: duckdb.DuckDBPyConnection) -> None:
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
        get_logger().info(
            "Built dim_clients: %s interval rows across %s clients (%s current). "
            "%s rows with an unknown segment.",
            f"{summary[0]:,}",
            f"{summary[1]:,}",
            f"{summary[2]:,}",
            f"{summary[3]:,}",
        )


# --- checks ----------------------------------------------------------------


def assert_change_log_orderable(con: duckdb.DuckDBPyConnection) -> None:
    """One change per client per ``effective_date``.

    The interval chain is built with ``lead(effective_date) OVER (PARTITION BY
    client_id ORDER BY effective_date)``. Two changes for one client on the *same*
    date give that ordering nothing to work with: the tie resolves arbitrarily,
    one interval collapses to zero length and is dropped by the positive-length
    filter, and a segment silently disappears from the client's history.

    Nothing downstream would notice. The resulting chain is still contiguous,
    non-overlapping and single-current, so ``assert_intervals_valid`` passes — the
    output is structurally valid and semantically wrong, which is the failure mode
    this whole pipeline is built to refuse.

    The change log carries no sequence column, so there is no correct answer to
    guess at; the honest response is to declare the ordering assumption as a
    contract and fail when it is violated (DECISIONS.md, section 3.3).
    """
    require(
        scalar(
            con,
            """
            SELECT count(*) FROM (
                SELECT 1 FROM stg_segment_changes
                GROUP BY client_id, effective_date HAVING count(*) > 1
            )
            """,
        ),
        "{count} (client_id, effective_date) pairs have more than one segment change. "
        "The change log carries no sequence column, so their order — and therefore the "
        "resulting segment history — is undefined. A tiebreaking column is required.",
    )
    passed("Segment change log has at most one change per client per date.")


def assert_intervals_valid(con: duckdb.DuckDBPyConnection) -> None:
    """No overlapping or gapped intervals within a client's timeline."""
    require(
        scalar(
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
        ),
        "{count} client interval boundaries in dim_clients overlap or leave a gap. "
        "Point-in-time lookups would return the wrong segment or none at all.",
    )
    require(
        scalar(con, "SELECT count(*) FROM dim_clients WHERE effective_start_date >= effective_end_date"),
        "{count} dim_clients rows have a non-positive interval length.",
    )
    require(
        scalar(
            con,
            """
            SELECT count(*) FROM (
                SELECT 1 FROM dim_clients WHERE is_current
                GROUP BY client_id HAVING count(*) > 1
            )
            """,
        ),
        "{count} clients have more than one current row in dim_clients.",
    )
    passed("dim_clients intervals are contiguous, non-overlapping and single-current per client.")


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
    origin_mismatches = scalar(
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
    chain_breaks = scalar(
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

    warn(
        origin_mismatches,
        f"{origin_mismatches:,} clients whose raw_clients.segment disagrees with their "
        "earliest from_segment. The dimension is unaffected; the reference table is "
        "inconsistent.",
    )
    warn(
        chain_breaks,
        f"{chain_breaks:,} discontinuities in the segment change chain (from_segment does "
        "not match the preceding to_segment).",
    )

    total = origin_mismatches + chain_breaks
    if not total:
        passed("Segment change chain is continuous and agrees with the client reference table.")
    return total


# --- process ---------------------------------------------------------------


def build(con: duckdb.DuckDBPyConnection, source_schema: str = SOURCE_SCHEMA) -> None:
    """Run the whole ``dim_clients`` process: stage, build, validate."""
    stage_clients(con, source_schema)
    stage_segment_changes(con, source_schema)
    assert_change_log_orderable(con)
    build_dimension(con)
    assert_intervals_valid(con)
    reconcile_segment_chain(con)
