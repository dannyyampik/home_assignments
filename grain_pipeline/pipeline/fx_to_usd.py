"""``fx_to_usd`` — one resolved base-to-USD rate per (currency, date).

**Reads**  ``src.raw_fx_rates``
**Writes** ``stg_fx_rates``, ``fx_to_usd`` (both temp)
**Checks** rate feed key uniqueness (raises)

This is an *intermediate* model rather than a dimension or a fact: it is not
published to the target, it is a conformed lookup that any model needing a USD
conversion joins to. It gets its own module for the same reason a dimension
does — it has its own source, its own cleaning rules, its own quality check, and
exactly one reason to change. Folding it into ``fact_daily_exposure`` would make
that fact the only model in the project that owns two unrelated sources, and
would bury a reusable lookup inside a single consumer.

----

The fact table converts a *base-currency* amount into USD, so for a trade with
base currency X the pipeline needs an X -> USD rate.

Two sources produce one:

``direct``
    A feed row with base X and quote USD. Used as-is.

``inverse``
    No X -> USD row exists, but the feed carries USD -> X. Then
    ``X -> USD = 1 / (USD -> X)``.

    Note the direction: inverting X -> USD would yield USD -> X, which converts
    the wrong way for this pipeline.

Direct always wins where both exist, per the requirement.

``quote_currency`` plays no part in resolution — the lookup is keyed on
``(base_currency, rate_date)`` because the measure being produced is the base
amount expressed in USD, not the base amount expressed in the quote currency
(DECISIONS.md, section 8.2).

On this dataset the only feed row with USD as base is USD -> CAD, and CAD *is* a
trade base currency with no CAD -> USD row, so the ``inverse`` branch is
exercised in production rather than merely implemented.
"""

from __future__ import annotations

import duckdb

from ..utils.config import (
    RATE_SOURCE_DIRECT,
    RATE_SOURCE_INVERSE,
    SOURCE_SCHEMA,
    USD,
)
from ..utils.logging_setup import get_logger
from ..utils.quality import passed, require
from ..utils.sql import canonical_currency, row_count, scalar


# --- staging ---------------------------------------------------------------


def stage_fx_rates(con: duckdb.DuckDBPyConnection, source_schema: str = SOURCE_SCHEMA) -> None:
    """Normalise and clean the FX rate feed into ``stg_fx_rates``.

    Invalid rates are removed *before* any deduplication. The feed contains two
    duplicate (base, quote, date) keys, and in each case the duplicate pairs a
    *valid* rate with an invalid one — ILS/USD on 2026-02-15 carries 0.275537 and
    0.0, EUR/USD on 2026-02-20 carries 1.085145 and NULL. Filtering first keeps
    the good rate; deduplicating first with an arbitrary pick could retain the
    zero and convert real trades at a rate of nothing (DECISIONS.md, section 8.1).

    Negative rates are excluded alongside zeros. The requirement names only zero,
    but a negative FX rate is not a meaningful quantity and inverting one would
    silently produce a negative converted amount.
    """
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE stg_fx_rates AS
        SELECT
            {canonical_currency('base_currency')}   AS base_currency,
            {canonical_currency('quote_currency')}  AS quote_currency,
            mid_rate,
            rate_date
        FROM {source_schema}.raw_fx_rates
        WHERE rate_date IS NOT NULL
          AND mid_rate IS NOT NULL
          AND mid_rate > 0
          AND {canonical_currency('base_currency')} IS NOT NULL
          AND {canonical_currency('quote_currency')} IS NOT NULL
        """
    )
    raw = scalar(con, f"SELECT count(*) FROM {source_schema}.raw_fx_rates")
    staged = row_count(con, "stg_fx_rates")
    get_logger().info(
        "Staged FX rates: %s raw rows, %s excluded as invalid "
        "(NULL/zero/negative rate or missing date), %s retained.",
        f"{raw:,}",
        f"{raw - staged:,}",
        f"{staged:,}",
    )


# --- checks ----------------------------------------------------------------


def assert_feed_unique(con: duckdb.DuckDBPyConnection) -> None:
    """After invalid rows are filtered, one rate per (base, quote, date).

    No source precedence rule is defined because, post-filter, none should be
    needed. If this fires, the right response is to define one deliberately
    rather than to have guessed at one silently.
    """
    require(
        scalar(
            con,
            """
            SELECT count(*) FROM (
                SELECT 1 FROM stg_fx_rates
                GROUP BY base_currency, quote_currency, rate_date
                HAVING count(*) > 1
            )
            """,
        ),
        "{count} (base_currency, quote_currency, rate_date) keys have more than one valid rate "
        "after cleaning. A source precedence rule is now required.",
    )
    passed("FX rate feed is unique on (base_currency, quote_currency, rate_date).")


# --- build -----------------------------------------------------------------


def build_lookup(con: duckdb.DuckDBPyConnection) -> None:
    """Build ``fx_to_usd`` from the cleaned feed.

    Reads the already-cleaned ``stg_fx_rates``, so nothing here needs to guard
    against zero or NULL rates — which matters, because the inverse branch
    divides by the rate.
    """
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE fx_to_usd AS
        WITH candidates AS (
            -- Direct: base -> USD, taken as-is.
            SELECT
                base_currency              AS currency,
                rate_date,
                mid_rate                   AS rate_to_usd,
                '{RATE_SOURCE_DIRECT}'     AS rate_source,
                0                          AS preference
            FROM stg_fx_rates
            WHERE quote_currency = '{USD}'
              AND base_currency <> '{USD}'

            UNION ALL

            -- Inverse: USD -> quote, reciprocated to give quote -> USD.
            SELECT
                quote_currency             AS currency,
                rate_date,
                1.0 / mid_rate             AS rate_to_usd,
                '{RATE_SOURCE_INVERSE}'    AS rate_source,
                1                          AS preference
            FROM stg_fx_rates
            WHERE base_currency = '{USD}'
              AND quote_currency <> '{USD}'
        )
        SELECT currency, rate_date, rate_to_usd, rate_source
        FROM candidates
        QUALIFY row_number() OVER (
            PARTITION BY currency, rate_date
            ORDER BY preference, rate_to_usd
        ) = 1
        """
    )

    summary = con.execute(
        f"""
        SELECT
            count(*),
            count(*) FILTER (WHERE rate_source = '{RATE_SOURCE_DIRECT}'),
            count(*) FILTER (WHERE rate_source = '{RATE_SOURCE_INVERSE}'),
            count(DISTINCT currency)
        FROM fx_to_usd
        """
    ).fetchone()

    if summary:
        get_logger().info(
            "Resolved FX rates to USD: %s (currency, date) pairs across %s currencies "
            "— %s direct, %s inverse.",
            f"{summary[0]:,}",
            f"{summary[3]:,}",
            f"{summary[1]:,}",
            f"{summary[2]:,}",
        )


# --- process ---------------------------------------------------------------


def build(con: duckdb.DuckDBPyConnection, source_schema: str = SOURCE_SCHEMA) -> None:
    """Run the whole ``fx_to_usd`` process: stage, validate, resolve.

    The uniqueness check runs *between* staging and resolution rather than after
    it: once the lookup is built, a duplicate key has already been resolved by
    the ``QUALIFY`` tiebreak, and the check would be reporting on a decision
    silently already taken.
    """
    stage_fx_rates(con, source_schema)
    assert_feed_unique(con)
    build_lookup(con)
