"""``fact_daily_exposure`` — daily FX exposure per client and currency pair.

**Grain**  ``(trade_date, client_id, base_currency, quote_currency)``
**Reads**  ``src.raw_trades``; ``stg_clients`` and ``dim_clients`` (from
           ``dim_clients``); ``fx_to_usd`` (from ``fx_to_usd``)
**Writes** ``stg_trades``, ``trades_deduplicated``, ``trades_clean``,
           ``trades_enriched`` (temp), ``fact_daily_exposure``
**Checks** grain uniqueness, trade reconciliation, conversion consistency
           (all raise)

The upstream dependencies are why ``run.py`` builds this model last. They are
ordinary warehouse dependencies — a fact reads its conformed dimension and its
rate lookup — and are declared here rather than inferred from execution order.

The build runs in five stages, each separately testable:

1. **Stage** — normalise amounts, identifiers, currencies and status.
2. **Deduplicate** — one row per ``trade_id``, earliest version wins.
3. **Filter** — the counted exclusion chain.
4. **Enrich** — attach the point-in-time segment and the resolved USD rate.
5. **Aggregate** — group to the declared grain.

Stage before deduplicate, and deduplicate before filter, are both deliberate and
both load-bearing; see DECISIONS.md sections 5, 6 and 4 respectively.
"""

from __future__ import annotations

import duckdb

from ..utils.config import (
    ACTIVE_STATUS,
    CURRENCY_ALIASES,
    ISO_4217_PATTERN,
    RATE_SOURCE_DIRECT,
    RATE_SOURCE_NOT_FOUND,
    SOURCE_SCHEMA,
    TRADE_DATE_CUTOFF,
    USD,
)
from ..utils.filters import FilterStep, apply_filter_chain
from ..utils.logging_setup import get_logger
from ..utils.quality import passed, require
from ..utils.sql import canonical_currency, canonical_text, row_count, scalar


#: The exclusion chain, in the order it is applied. Order is fixed so the logged
#: counts reproduce; a row violating several rules is attributed to the first.
FILTER_STEPS: tuple[FilterStep, ...] = (
    FilterStep(
        name="non_active_status",
        predicate=f"status = '{ACTIVE_STATUS}'",
        description="status is not ACTIVE",
    ),
    FilterStep(
        name="after_cutoff_date",
        predicate=(
            f"trade_date IS NOT NULL AND trade_date <= DATE '{TRADE_DATE_CUTOFF.isoformat()}'"
        ),
        description=f"trade_date is missing or later than {TRADE_DATE_CUTOFF.isoformat()}",
    ),
    FilterStep(
        # Reads stg_clients, owned by the dim_clients model. "Missing client
        # information" is read as covering both a NULL identifier and one with no
        # match in the reference data — an unresolvable reference is as much a
        # gap as an absent one (DECISIONS.md, section 7).
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


# --- staging ---------------------------------------------------------------


def stage_trades(con: duckdb.DuckDBPyConnection, source_schema: str = SOURCE_SCHEMA) -> None:
    """Normalise raw trades into ``stg_trades``. Performs no filtering.

    Amount units are normalised *before* deduplication, so two versions of one
    trade recorded under different conventions (5000/false and 5/true) are
    recognised as carrying the same value rather than as a real discrepancy — and
    before the positive-amount filter, so that test applies to real values.

    Currency codes are normalised *before* the missing-currency exclusion, so
    'usd' is not discarded as malformed, and non-ISO aliases are resolved here
    too — see ``canonical_currency``.
    """
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE stg_trades AS
        SELECT
            nullif(trim(trade_id), '')              AS trade_id,
            {canonical_text('client_id')}           AS client_id,
            trade_date,
            {canonical_text('status')}              AS status,
            {canonical_currency('base_currency')}   AS base_currency,
            {canonical_currency('quote_currency')}  AS quote_currency,
            CASE
                WHEN coalesce(amount_in_thousands, FALSE) THEN amount * 1000
                ELSE amount
            END                                     AS amount,
            agreed_rate,
            created_at
        FROM {source_schema}.raw_trades
        """
    )
    get_logger().info(
        "Staged trades: %s rows (amount units, identifiers, currencies and status normalised).",
        f"{row_count(con, 'stg_trades'):,}",
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
        affected = scalar(
            con,
            f"""
            SELECT
                count(*) FILTER (WHERE {canonical_text('base_currency')} = '{alias}')
              + count(*) FILTER (WHERE {canonical_text('quote_currency')} = '{alias}')
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


# --- deduplication and filtering -------------------------------------------


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

    before = row_count(con, "stg_trades")
    con.execute(
        "CREATE OR REPLACE TEMP TABLE trades_identified AS "
        "SELECT * FROM stg_trades WHERE trade_id IS NOT NULL"
    )
    identified = row_count(con, "trades_identified")
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

    after = row_count(con, "trades_deduplicated")
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
    return apply_filter_chain(
        con, "trades_deduplicated", "trades_clean", FILTER_STEPS, unit="trades"
    )


# --- enrichment and aggregation --------------------------------------------


def build_trades_enriched(con: duckdb.DuckDBPyConnection) -> None:
    """Attach point-in-time segment and USD rate to each cleaned trade.

    The segment join is an inner join against ``dim_clients`` over a half-open
    interval. Because trades whose client does not resolve were already excluded,
    it should drop nothing — and ``assert_no_trades_lost`` fails loudly if it
    ever does.
    """
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


def build_fact(con: duckdb.DuckDBPyConnection) -> None:
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
            -- indistinguishable from a genuine zero-exposure row and would
            -- silently understate any downstream sum.
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
                "  %s rows have no weighted average agreed rate "
                "(every trade in the group had a NULL agreed_rate).",
                f"{summary[5]:,}",
            )


# --- checks ----------------------------------------------------------------


def assert_grain_unique(con: duckdb.DuckDBPyConnection) -> None:
    """Exactly one fact row per (date, client, base_currency, quote_currency).

    This is the assertion that catches a dimension join fan-out, which is the
    highest-risk silent failure in this pipeline.
    """
    require(
        scalar(
            con,
            """
            SELECT count(*) FROM (
                SELECT 1 FROM fact_daily_exposure
                GROUP BY trade_date, client_id, base_currency, quote_currency
                HAVING count(*) > 1
            )
            """,
        ),
        "{count} (trade_date, client_id, base_currency, quote_currency) combinations appear more "
        "than once in fact_daily_exposure. The declared grain is violated — most likely "
        "a fan-out on the dim_clients join.",
    )
    passed("fact_daily_exposure is unique on its declared grain.")


def assert_no_trades_lost(con: duckdb.DuckDBPyConnection) -> None:
    """Fact trade counts reconcile exactly with the cleaned trade set.

    Catches both directions at once: rows dropped by the inner dimension join,
    and rows duplicated by a fan-out. Either would leave totals wrong without
    raising anything.
    """
    clean_trades = row_count(con, "trades_clean")
    fact_trades = scalar(con, "SELECT coalesce(sum(trade_count), 0) FROM fact_daily_exposure")

    require(
        int(clean_trades != fact_trades),
        f"Trade count reconciliation failed: {clean_trades} cleaned trades but "
        f"{fact_trades} counted in fact_daily_exposure. Rows were lost or duplicated "
        "in the dimension join.",
    )
    passed(f"Trade counts reconcile: {clean_trades:,} cleaned trades accounted for in the fact table.")


def assert_conversions_consistent(con: duckdb.DuckDBPyConnection) -> None:
    """A resolved rate implies a converted amount, and vice versa."""
    require(
        scalar(
            con,
            """
            SELECT count(*) FROM fact_daily_exposure
            WHERE (fx_rate_source = 'not_found' AND total_amount_usd IS NOT NULL)
               OR (fx_rate_source <> 'not_found' AND total_amount_usd IS NULL)
               OR (fx_rate_used IS NOT NULL AND fx_rate_used <= 0)
            """,
        ),
        "{count} fact rows have an inconsistent rate source, converted amount or a non-positive rate.",
    )
    passed("USD conversions are consistent with their recorded rate source.")


# --- process ---------------------------------------------------------------


def build(con: duckdb.DuckDBPyConnection, source_schema: str = SOURCE_SCHEMA) -> dict[str, int]:
    """Run the whole ``fact_daily_exposure`` process, returning exclusion counts."""
    stage_trades(con, source_schema)
    deduplicate_trades(con)
    exclusions = apply_filters(con)
    build_trades_enriched(con)
    build_fact(con)
    assert_grain_unique(con)
    assert_no_trades_lost(con)
    assert_conversions_consistent(con)
    return exclusions
