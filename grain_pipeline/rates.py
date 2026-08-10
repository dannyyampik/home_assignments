"""FX rate resolution to USD.

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

USD-denominated trades are a special case: no USD -> USD row exists in the feed,
so a literal reading of the chain would send every USD trade to ``not_found``
with a NULL converted amount. They are assigned a rate of 1.0 labelled
``direct`` (DECISIONS.md, section 8.4).

On this dataset the only feed row with USD as base is USD -> CAD, and CAD never
appears as a trade base currency, so the ``inverse`` branch is unreachable in
production and is covered by a synthetic test fixture instead.
"""

from __future__ import annotations

import duckdb

from .config import RATE_SOURCE_DIRECT, RATE_SOURCE_INVERSE, USD
from .logging_setup import get_logger


def build_fx_to_usd(con: duckdb.DuckDBPyConnection) -> None:
    """Build ``fx_to_usd``: one resolved rate per (currency, date).

    Reads the already-cleaned ``stg_fx_rates``. Invalid rows were removed during
    staging, so nothing here needs to guard against zero or NULL rates — which
    matters, because the inverse branch divides by the rate.
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

    logger = get_logger()
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
        logger.info(
            "Resolved FX rates to USD: %s (currency, date) pairs across %s currencies — %s direct, %s inverse.",
            f"{summary[0]:,}",
            f"{summary[3]:,}",
            f"{summary[1]:,}",
            f"{summary[2]:,}",
        )
