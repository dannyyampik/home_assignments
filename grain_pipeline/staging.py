"""Staging layer.

Every normalisation happens here, before any filtering or joining. Ordering
matters and is deliberate (DECISIONS.md, sections 5 and 6):

* Amount units are normalised before deduplication, so that two versions of one
  trade recorded under different conventions (5000/false and 5/true) are
  recognised as carrying the same value rather than as a real discrepancy.
* Currency codes are normalised before the "missing currency" exclusion, so that
  'usd' is not discarded as malformed. Normalisation includes resolving non-ISO
  aliases such as 'NIS' -> 'ILS'; case folding alone leaves those codes shaped
  like valid ISO ones, so they pass every filter and then fail to match the rate
  feed, which is the quietest way for a trade to lose its USD exposure.
* Client identifiers are canonicalised in all three tables that carry them, so
  the joins between them cannot miss.
"""

from __future__ import annotations

import duckdb

from .config import CURRENCY_ALIASES, SOURCE_SCHEMA
from .logging_setup import get_logger


def _canonical_text(column: str) -> str:
    """SQL fragment: trim, uppercase, and treat the empty string as NULL.

    An identifier that is blank or whitespace-only carries no information, so it
    is folded to NULL and handled by the same exclusion as a true NULL.
    """
    return f"upper(nullif(trim({column}), ''))"


def _canonical_currency(column: str) -> str:
    """SQL fragment: canonicalise a currency code and resolve non-ISO aliases.

    'Normalised to ISO 4217' is not satisfied by case folding alone. A code such
    as 'NIS' is already uppercase and three letters, so it passes the ISO shape
    test and is never excluded — but it matches no row in the rate feed, which
    publishes the same currency as 'ILS'. The result is a trade that survives
    every filter and then silently resolves to not_found with a NULL USD
    exposure.

    Aliases are resolved here, alongside the other normalisations and before any
    filtering or joining, so both the exclusion chain and the FX join see one
    spelling per currency (DECISIONS.md, section 6.1).
    """
    canonical = _canonical_text(column)
    if not CURRENCY_ALIASES:
        return canonical
    branches = " ".join(
        f"WHEN '{alias}' THEN '{iso}'" for alias, iso in sorted(CURRENCY_ALIASES.items())
    )
    return f"CASE {canonical} {branches} ELSE {canonical} END"


def build_stg_trades(con: duckdb.DuckDBPyConnection, source_schema: str = SOURCE_SCHEMA) -> None:
    """Normalise raw trades into ``stg_trades``.

    Applies unit normalisation to ``amount`` and canonicalisation to the
    identifier, status and currency columns. Performs no filtering.
    """
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE stg_trades AS
        SELECT
            nullif(trim(trade_id), '')            AS trade_id,
            {_canonical_text('client_id')}        AS client_id,
            trade_date,
            {_canonical_text('status')}             AS status,
            {_canonical_currency('base_currency')}  AS base_currency,
            {_canonical_currency('quote_currency')} AS quote_currency,
            CASE
                WHEN coalesce(amount_in_thousands, FALSE) THEN amount * 1000
                ELSE amount
            END                                   AS amount,
            agreed_rate,
            created_at
        FROM {source_schema}.raw_trades
        """
    )
    get_logger().info(
        "Staged trades: %s rows (amount units, identifiers, currencies and status normalised).",
        f"{_row_count(con, 'stg_trades'):,}",
    )
    _log_currency_aliases(con, source_schema)


def _log_currency_aliases(con: duckdb.DuckDBPyConnection, source_schema: str) -> None:
    """Report how many trade rows had a non-ISO currency code rewritten.

    Surfaced rather than applied silently: a pipeline that rewrites the source's
    currency codes owes the reader a count of how often it did so, and the number
    is what a source-system owner needs in order to fix the feed upstream.
    """
    if not CURRENCY_ALIASES:
        return

    logger = get_logger()
    for alias, iso in sorted(CURRENCY_ALIASES.items()):
        affected = _row_count_expr(
            con,
            f"""
            SELECT
                count(*) FILTER (WHERE {_canonical_text('base_currency')} = '{alias}')
              + count(*) FILTER (WHERE {_canonical_text('quote_currency')} = '{alias}')
            FROM {source_schema}.raw_trades
            """,
        )
        if affected:
            logger.info(
                "  Currency alias applied: %s -> %s on %s trade currency values "
                "(%s is not an ISO 4217 code; the rate feed publishes %s).",
                alias,
                iso,
                f"{affected:,}",
                alias,
                iso,
            )


def build_stg_clients(con: duckdb.DuckDBPyConnection, source_schema: str = SOURCE_SCHEMA) -> None:
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
            {_canonical_text('client_id')}     AS client_id,
            any_value(nullif(trim(client_name), ''))  AS client_name,
            any_value(nullif(trim(segment), ''))      AS segment
        FROM {source_schema}.raw_clients
        WHERE {_canonical_text('client_id')} IS NOT NULL
        GROUP BY 1
        """
    )
    logger = get_logger()
    raw_count = _row_count_expr(con, f"SELECT count(*) FROM {source_schema}.raw_clients")
    staged = _row_count(con, "stg_clients")
    logger.info(
        "Staged clients: %s raw rows collapsed to %s canonical clients (%s duplicate identifier variants merged).",
        f"{raw_count:,}",
        f"{staged:,}",
        f"{raw_count - staged:,}",
    )


def build_stg_segment_changes(
    con: duckdb.DuckDBPyConnection, source_schema: str = SOURCE_SCHEMA
) -> None:
    """Normalise the segment change log into ``stg_segment_changes``.

    Rows with no effective date cannot be placed on a timeline and are dropped.
    """
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE stg_segment_changes AS
        SELECT
            {_canonical_text('client_id')}       AS client_id,
            nullif(trim(from_segment), '')       AS from_segment,
            nullif(trim(to_segment), '')         AS to_segment,
            effective_date
        FROM {source_schema}.raw_client_segment_changes
        WHERE {_canonical_text('client_id')} IS NOT NULL
          AND effective_date IS NOT NULL
        """
    )
    get_logger().info(
        "Staged segment changes: %s rows.", f"{_row_count(con, 'stg_segment_changes'):,}"
    )


def build_stg_fx_rates(con: duckdb.DuckDBPyConnection, source_schema: str = SOURCE_SCHEMA) -> None:
    """Normalise and clean the FX rate feed into ``stg_fx_rates``.

    Invalid rates are removed *before* any deduplication. The feed contains
    duplicate (base, quote, date) keys from the same source where one row holds a
    NULL rate and the other a zero. Filtering first removes that key entirely and
    lets the lookup fall through correctly; deduplicating first could retain the
    invalid row and discard a valid one elsewhere (DECISIONS.md, section 8.1).

    Negative rates are excluded alongside zeros. The requirement names only zero,
    but a negative FX rate is not a meaningful quantity and inverting one would
    silently produce a negative converted amount.
    """
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE stg_fx_rates AS
        SELECT
            {_canonical_currency('base_currency')}   AS base_currency,
            {_canonical_currency('quote_currency')}  AS quote_currency,
            mid_rate,
            rate_date
        FROM {source_schema}.raw_fx_rates
        WHERE rate_date IS NOT NULL
          AND mid_rate IS NOT NULL
          AND mid_rate > 0
          AND {_canonical_text('base_currency')} IS NOT NULL
          AND {_canonical_text('quote_currency')} IS NOT NULL
        """
    )
    logger = get_logger()
    raw_count = _row_count_expr(con, f"SELECT count(*) FROM {source_schema}.raw_fx_rates")
    staged = _row_count(con, "stg_fx_rates")
    logger.info(
        "Staged FX rates: %s raw rows, %s excluded as invalid (NULL/zero/negative rate or missing date), %s retained.",
        f"{raw_count:,}",
        f"{raw_count - staged:,}",
        f"{staged:,}",
    )


# --- helpers --------------------------------------------------------------


def _row_count(con: duckdb.DuckDBPyConnection, table: str) -> int:
    return _row_count_expr(con, f"SELECT count(*) FROM {table}")


def _row_count_expr(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    result = con.execute(sql).fetchone()
    return int(result[0]) if result else 0
