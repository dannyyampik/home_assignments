"""``fact_daily_exposure`` — daily aggregate at
(trade_date, client_id, base_currency, quote_currency).

Built in two stages so each is legible and separately testable:

1. ``trades_enriched`` attaches, per trade, the point-in-time segment and the
   resolved USD rate.
2. The aggregate groups to the declared grain.

The segment join is an inner join against ``dim_clients`` over a half-open
interval. Because trades whose client does not resolve were already excluded, the
inner join should drop nothing — and the quality check asserting that trade
counts reconcile will fail loudly if it ever does.
"""

from __future__ import annotations

import duckdb

from .config import RATE_SOURCE_DIRECT, RATE_SOURCE_NOT_FOUND, USD
from .logging_setup import get_logger


def build_trades_enriched(con: duckdb.DuckDBPyConnection) -> None:
    """Attach point-in-time segment and USD rate to each cleaned trade."""
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE trades_enriched AS
        SELECT
            t.trade_id,
            t.trade_date,
            t.client_id,
            d.client_name,
            d.segment,
            t.base_currency,
            t.quote_currency,
            t.amount,
            t.agreed_rate,
            -- USD converts to itself at parity. Applied ahead of the lookup so
            -- it cannot be masked by a spurious feed row.
            CASE
                WHEN t.base_currency = '{USD}' THEN 1.0
                ELSE r.rate_to_usd
            END AS fx_rate_used,
            CASE
                WHEN t.base_currency = '{USD}' THEN '{RATE_SOURCE_DIRECT}'
                ELSE coalesce(r.rate_source, '{RATE_SOURCE_NOT_FOUND}')
            END AS fx_rate_source
        FROM trades_clean t
        JOIN dim_clients d
          ON  d.client_id = t.client_id
          -- Half-open interval: a trade on a change date belongs to the new
          -- segment only, so it cannot match two dimension rows.
          AND t.trade_date >= d.effective_start_date
          AND t.trade_date <  d.effective_end_date
        LEFT JOIN fx_to_usd r
          ON  r.currency  = t.base_currency
          AND r.rate_date = t.trade_date
        """
    )


def build_fact_daily_exposure(con: duckdb.DuckDBPyConnection) -> None:
    """Aggregate ``trades_enriched`` to the declared daily grain."""
    con.execute(
        """
        CREATE OR REPLACE TABLE fact_daily_exposure AS
        SELECT
            trade_date,
            client_id,
            client_name,
            segment,
            base_currency,
            quote_currency,

            count(*)      AS trade_count,
            sum(amount)   AS total_amount_base,

            -- Amount-weighted mean of the agreed rate. Trades with an unknown
            -- agreed rate leave both the numerator and the denominator, so their
            -- amount cannot drag the average toward zero — but they remain in
            -- trade_count and total_amount_base, because the trade is real and
            -- its amount is known even where its rate is not.
            sum(amount * agreed_rate) FILTER (WHERE agreed_rate IS NOT NULL)
                / nullif(sum(amount) FILTER (WHERE agreed_rate IS NOT NULL), 0)
                          AS weighted_avg_agreed_rate,

            -- Constant within the group: the rate is keyed by currency and date,
            -- both of which are grain columns.
            max(fx_rate_used)     AS fx_rate_used,

            -- NULL rather than 0 where no rate resolved: a zero would be
            -- indistinguishable from genuine zero exposure and would silently
            -- understate any downstream sum.
            sum(amount * fx_rate_used) AS total_amount_usd,

            max(fx_rate_source)   AS fx_rate_source
        FROM trades_enriched
        GROUP BY 1, 2, 3, 4, 5, 6
        ORDER BY trade_date, client_id, base_currency, quote_currency
        """
    )

    logger = get_logger()
    summary = con.execute(
        """
        SELECT
            count(*),
            sum(trade_count),
            count(*) FILTER (WHERE fx_rate_source = 'direct'),
            count(*) FILTER (WHERE fx_rate_source = 'inverse'),
            count(*) FILTER (WHERE fx_rate_source = 'not_found'),
            count(*) FILTER (WHERE weighted_avg_agreed_rate IS NULL)
        FROM fact_daily_exposure
        """
    ).fetchone()

    if summary:
        logger.info(
            "Built fact_daily_exposure: %s rows covering %s trades.",
            f"{summary[0]:,}",
            f"{summary[1]:,}",
        )
        logger.info(
            "  FX rate resolution: %s direct, %s inverse, %s not_found.",
            f"{summary[2]:,}",
            f"{summary[3]:,}",
            f"{summary[4]:,}",
        )
        if summary[5]:
            logger.warning(
                "  %s rows have no weighted average agreed rate (every trade in the group had a NULL agreed_rate).",
                f"{summary[5]:,}",
            )
