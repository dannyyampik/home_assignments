"""Unit tests for the Grain analytics pipeline.

Each test targets a rule that was ambiguous in the specification or a defect
found while profiling the source data, and asserts the behaviour recorded in
DECISIONS.md.

Most of what is covered here is exercised by the real data — the ``inverse``
branch by CAD, ``not_found`` by SGD. A few rules are not, and those are the ones
that most need a fixture: the USD parity branch (no trade has USD as its base
currency), a client reclassified twice, a NULL ``agreed_rate``, and duplicate
trade versions that disagree on status. A rule the supplied data cannot reach is
a rule nothing would notice breaking.
"""

from __future__ import annotations

import duckdb
import pytest

from conftest import fx_rate, insert, trade
from grain_pipeline.pipeline import dim_clients, fact_daily_exposure, fx_to_usd
from grain_pipeline.pipeline.run import build_analytics, run_pipeline
from grain_pipeline.utils.quality import DataQualityError


def run_all_checks(con):
    """Every model's checks, for tests that assemble a build by hand.

    In a normal run each model validates itself inside its own ``build()``, so
    this exists only for tests that call the stages individually and still want
    the full tripwire.
    """
    dim_clients.assert_intervals_valid(con)
    dim_clients.reconcile_segment_chain(con)
    fx_to_usd.assert_feed_unique(con)
    fact_daily_exposure.assert_grain_unique(con)
    fact_daily_exposure.assert_no_trades_lost(con)
    fact_daily_exposure.assert_conversions_consistent(con)


# --------------------------------------------------------------------------
# 1. Amount unit normalisation
# --------------------------------------------------------------------------


def test_amount_in_thousands_is_scaled_and_survives_deduplication(con):
    """The thousands flag scales the amount, and scaling happens before dedup.

    Two versions of one trade recorded under different conventions — 5000/false
    and 5/true — describe the same value. Normalising first means they are not
    mistaken for a genuine amount discrepancy.
    """
    insert(
        con,
        "raw_trades",
        [
            trade("T1", amount=5.0, amount_in_thousands=True, created_at="2025-10-01 09:00:00"),
            trade("T1", amount=5000.0, amount_in_thousands=False, created_at="2025-10-02 09:00:00"),
            trade("T2", amount=250.0, amount_in_thousands=False),
        ],
    )
    fact_daily_exposure.stage_trades(con)

    amounts = dict(
        con.execute("SELECT trade_id, amount FROM stg_trades ORDER BY trade_id, amount").fetchall()
    )
    assert amounts["T2"] == 250.0

    scaled = con.execute("SELECT DISTINCT amount FROM stg_trades WHERE trade_id = 'T1'").fetchall()
    assert scaled == [(5000.0,)], "both versions of T1 should normalise to the same 5000.0"

    fact_daily_exposure.deduplicate_trades(con)
    kept = con.execute("SELECT amount FROM trades_deduplicated WHERE trade_id = 'T1'").fetchall()
    assert kept == [(5000.0,)]


# --------------------------------------------------------------------------
# 2. Deterministic deduplication
# --------------------------------------------------------------------------


def test_deduplication_keeps_earliest_created_at(con):
    """Version selection keeps the earliest ``created_at``, not the first row read."""
    insert(
        con,
        "raw_trades",
        [
            trade("T1", amount=900.0, created_at="2025-12-01 12:00:00"),
            trade("T1", amount=100.0, created_at="2025-09-01 08:30:00"),  # earliest
            trade("T1", amount=500.0, created_at="2025-11-15 17:45:00"),
        ],
    )
    fact_daily_exposure.stage_trades(con)
    fact_daily_exposure.deduplicate_trades(con)

    rows = con.execute("SELECT trade_id, amount FROM trades_deduplicated").fetchall()
    assert rows == [("T1", 100.0)]


def test_deduplication_keeps_the_earliest_version_even_when_statuses_disagree(con):
    """The case that actually distinguishes dedupe-before-filter from the reverse.

    Where two versions of one trade disagree on status, the two orderings give
    opposite answers: deduplicate first and an earliest-CANCELLED trade is
    dropped; filter first and its later ACTIVE version survives. The supplied
    data never exercises this — all 15 duplicate pairs share a status — so
    without this fixture the central decision of DECISIONS.md section 4 would be
    documented but unproven.
    """
    insert(con, "raw_clients", [("C001", "Acme", "SME")])
    insert(
        con,
        "raw_trades",
        [
            # T1: earliest version is CANCELLED, later version ACTIVE -> dropped.
            trade("T1", status="CANCELLED", created_at="2025-09-01 09:00:00"),
            trade("T1", status="ACTIVE", created_at="2025-10-01 09:00:00"),
            # T2: the mirror image -> kept, carrying the earliest version's amount.
            trade("T2", status="ACTIVE", amount=500.0, created_at="2025-09-01 09:00:00"),
            trade("T2", status="CANCELLED", amount=999.0, created_at="2025-10-01 09:00:00"),
        ],
    )
    dim_clients.stage_clients(con)
    fact_daily_exposure.stage_trades(con)
    fact_daily_exposure.deduplicate_trades(con)
    exclusions = fact_daily_exposure.apply_filters(con)

    kept = con.execute("SELECT trade_id, status, amount FROM trades_clean ORDER BY trade_id").fetchall()
    assert kept == [("T2", "ACTIVE", 500.0)]
    assert exclusions["non_active_status"] == 1, "the earliest-CANCELLED trade is excluded"


def test_deduplication_is_deterministic_across_insertion_orders(con, tmp_path):
    """The same rows in a different physical order produce the same survivor.

    Idempotency depends on a total ordering. ``created_at`` alone is not unique,
    so the tiebreak is content-based rather than positional — physical row order
    is not a guarantee the database owes us across runs.
    """
    import duckdb

    from conftest import _RAW_TABLES, SOURCE_SCHEMA  # noqa: PLC0415

    tied = [
        trade("T1", amount=300.0, agreed_rate=1.10, created_at="2025-10-01 09:00:00"),
        trade("T1", amount=200.0, agreed_rate=1.20, created_at="2025-10-01 09:00:00"),
    ]

    survivors = []
    for ordering in (tied, list(reversed(tied))):
        connection = duckdb.connect(":memory:")
        connection.execute(f"CREATE SCHEMA {SOURCE_SCHEMA}")
        for table, columns in _RAW_TABLES.items():
            connection.execute(f"CREATE TABLE {SOURCE_SCHEMA}.{table} ({columns})")
        insert(connection, "raw_trades", ordering)
        fact_daily_exposure.stage_trades(connection)
        fact_daily_exposure.deduplicate_trades(connection)
        survivors.append(
            connection.execute("SELECT amount FROM trades_deduplicated").fetchall()
        )
        connection.close()

    assert survivors[0] == survivors[1]


def test_dedup_ordering_covers_every_distinguishing_column(con):
    """Two versions differing *only* in a late-ordered column still resolve stably.

    The tiebreak is only a total order if it covers every column that can
    distinguish two versions. Rows tying on ``created_at``, ``amount``,
    ``agreed_rate``, ``status`` and ``base_currency`` but differing in
    ``trade_date`` would fall back to physical row order if the ordering stopped
    at the first five — which is precisely the run-to-run instability the tiebreak
    exists to remove.
    """
    from conftest import _RAW_TABLES, SOURCE_SCHEMA  # noqa: PLC0415

    tied = [
        trade("T1", trade_date="2026-01-15", created_at="2025-10-01 09:00:00"),
        trade("T1", trade_date="2026-02-20", created_at="2025-10-01 09:00:00"),
    ]

    survivors = []
    for ordering in (tied, list(reversed(tied))):
        connection = duckdb.connect(":memory:")
        connection.execute(f"CREATE SCHEMA {SOURCE_SCHEMA}")
        for table, columns in _RAW_TABLES.items():
            connection.execute(f"CREATE TABLE {SOURCE_SCHEMA}.{table} ({columns})")
        insert(connection, "raw_trades", ordering)
        fact_daily_exposure.stage_trades(connection)
        fact_daily_exposure.deduplicate_trades(connection)
        survivors.append(connection.execute("SELECT trade_date FROM trades_deduplicated").fetchall())
        connection.close()

    assert survivors[0] == survivors[1], "insertion order must not decide the survivor"


# --------------------------------------------------------------------------
# 3. Normalisation ordering and exclusions
# --------------------------------------------------------------------------


def test_lowercase_currency_is_normalised_not_excluded(con):
    """Currency normalisation runs before the missing-currency exclusion.

    Excluding first would discard 'usd' and ' Eur ' as malformed, which is the
    trap the requirement's ordering hides.
    """
    insert(con, "raw_clients", [("C001", "Acme", "SME")])
    insert(
        con,
        "raw_trades",
        [
            trade("T1", base_currency="gbp", quote_currency="usd"),
            trade("T2", base_currency=" Eur ", quote_currency="USD"),
            trade("T3", base_currency=None, quote_currency="USD"),  # genuinely missing
        ],
    )
    dim_clients.stage_clients(con)
    fact_daily_exposure.stage_trades(con)
    fact_daily_exposure.deduplicate_trades(con)
    exclusions = fact_daily_exposure.apply_filters(con)

    kept = con.execute(
        "SELECT trade_id, base_currency, quote_currency FROM trades_clean ORDER BY trade_id"
    ).fetchall()
    assert kept == [("T1", "GBP", "USD"), ("T2", "EUR", "USD")]
    assert exclusions["missing_or_invalid_currency"] == 1


def test_non_iso_currency_alias_is_resolved_to_its_iso_code(con):
    """'NIS' is rewritten to 'ILS' and converts against the ILS feed.

    This is the failure that case folding cannot catch. 'NIS' is already three
    uppercase letters, so it satisfies the ISO 4217 shape test and survives every
    exclusion — and then matches nothing in the rate feed, which publishes the
    same currency as 'ILS'. The trade reaches the fact table looking healthy,
    with a NULL USD exposure.

    The contrast row matters as much as the subject: 'SGD' has no feed coverage
    at all, so it must stay ``not_found``. Resolving a documented alias is a
    different act from inventing a rate for a currency nobody quoted.
    """
    insert(con, "raw_clients", [("C001", "Acme", "SME")])
    insert(
        con,
        "raw_trades",
        [
            trade("T1", base_currency="NIS", amount=1000.0, agreed_rate=0.27),
            trade("T2", base_currency="SGD", amount=2000.0, agreed_rate=0.74),
        ],
    )
    insert(con, "raw_fx_rates", [fx_rate(1, "ILS", "USD", 0.275)])

    dim_clients.stage_clients(con)
    dim_clients.stage_segment_changes(con)
    fact_daily_exposure.stage_trades(con)
    fx_to_usd.stage_fx_rates(con)
    fact_daily_exposure.deduplicate_trades(con)
    exclusions = fact_daily_exposure.apply_filters(con)
    dim_clients.build_dimension(con)
    fx_to_usd.build_lookup(con)
    fact_daily_exposure.build_trades_enriched(con)
    fact_daily_exposure.build_fact(con)

    assert exclusions["missing_or_invalid_currency"] == 0, "NIS is ISO-shaped; it is never excluded"

    rows = dict(
        con.execute(
            """
            SELECT base_currency, (fx_rate_used, total_amount_usd, fx_rate_source)
            FROM fact_daily_exposure
            """
        ).fetchall()
    )

    assert "NIS" not in rows, "the alias must not survive into the fact table"
    assert rows["ILS"] == (0.275, 275.0, "direct")
    assert rows["SGD"] == (None, None, "not_found"), "no feed coverage — genuinely unresolvable"

    run_all_checks(con)


def test_malformed_and_unresolvable_values_are_excluded(con):
    """The two exclusion halves that a NULL-only fixture never reaches.

    Both rules are documented as covering two conditions each, but a NULL-only
    fixture exercises just one of them: a currency that is present but malformed
    is caught by the ISO shape test rather than the NULL test, and a client_id
    that is present but unknown is caught by the resolution test rather than the
    NULL test. Without these rows the regex and the subquery could both be
    deleted with every test still green.
    """
    insert(con, "raw_clients", [("C001", "Acme", "SME")])
    insert(
        con,
        "raw_trades",
        [
            trade("T1"),  # control: survives
            trade("T2", client_id="C999"),  # present, but no such client
            trade("T3", base_currency="US"),  # present, but not ISO-shaped
            trade("T4", base_currency="GBPX"),  # present, but not ISO-shaped
        ],
    )
    dim_clients.stage_clients(con)
    fact_daily_exposure.stage_trades(con)
    fact_daily_exposure.deduplicate_trades(con)
    exclusions = fact_daily_exposure.apply_filters(con)

    kept = [r[0] for r in con.execute("SELECT trade_id FROM trades_clean").fetchall()]
    assert kept == ["T1"]
    assert exclusions["missing_client"] == 1, "an unresolvable client_id is as much a gap as a NULL"
    assert exclusions["missing_or_invalid_currency"] == 2


def test_cutoff_date_is_inclusive_of_the_boundary(con):
    """A trade *on* 2026-06-01 is kept; the day after is excluded.

    The rule is `trade_date <= 2026-06-01`, so the boundary date itself must
    survive. Testing only the day after would leave `<=` versus `<` unpinned.
    """
    insert(con, "raw_clients", [("C001", "Acme", "SME")])
    insert(
        con,
        "raw_trades",
        [
            trade("T1", trade_date="2026-05-31"),
            trade("T2", trade_date="2026-06-01"),  # the boundary itself
            trade("T3", trade_date="2026-06-02"),
        ],
    )
    dim_clients.stage_clients(con)
    fact_daily_exposure.stage_trades(con)
    fact_daily_exposure.deduplicate_trades(con)
    exclusions = fact_daily_exposure.apply_filters(con)

    kept = [r[0] for r in con.execute("SELECT trade_id FROM trades_clean ORDER BY trade_id").fetchall()]
    assert kept == ["T1", "T2"]
    assert exclusions["after_cutoff_date"] == 1


def test_client_identifier_variants_collapse_without_fanout(con):
    """Case and whitespace variants of one client_id resolve to a single client.

    This is the defect found in ``raw_clients`` ('c007'/'C007', 'C003'/'C003 ').
    Left uncorrected it fans out the dimension join and silently duplicates every
    affected fact row — no error raised, just wrong numbers.
    """
    insert(
        con,
        "raw_clients",
        [
            ("C007", "Globex", "Enterprise"),
            ("c007", "Globex", "Enterprise"),
            ("C003 ", "Initech", "SME"),
            ("C003", "Initech", "SME"),
        ],
    )
    insert(con, "raw_trades", [trade("T1", client_id="c007"), trade("T2", client_id="C003")])
    insert(con, "raw_fx_rates", [fx_rate(1, "GBP", "USD", 1.27)])

    dim_clients.stage_clients(con)
    dim_clients.stage_segment_changes(con)
    fact_daily_exposure.stage_trades(con)
    fx_to_usd.stage_fx_rates(con)
    fact_daily_exposure.deduplicate_trades(con)
    fact_daily_exposure.apply_filters(con)
    dim_clients.build_dimension(con)
    fx_to_usd.build_lookup(con)
    fact_daily_exposure.build_trades_enriched(con)
    fact_daily_exposure.build_fact(con)

    assert con.execute("SELECT count(*) FROM stg_clients").fetchone()[0] == 2
    assert con.execute("SELECT count(*) FROM fact_daily_exposure").fetchone()[0] == 2
    assert con.execute("SELECT sum(trade_count) FROM fact_daily_exposure").fetchone()[0] == 2

    run_all_checks(con)  # would raise on any fan-out


# --------------------------------------------------------------------------
# 4. Point-in-time segment lookup
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("trade_date", "expected_segment"),
    [
        ("2025-01-01", "SME"),  # before the change
        ("2025-05-31", "SME"),  # day before the change
        ("2025-06-01", "Enterprise"),  # on the change date — half-open boundary
        ("2026-01-15", "Enterprise"),  # after the change
    ],
)
def test_segment_reflects_classification_on_the_trade_date(con, trade_date, expected_segment):
    """The fact carries the segment held on the trade date, not the current one.

    The change-date case is the one that matters: under a closed-closed interval
    convention it would match two dimension rows and duplicate the fact row.
    """
    # raw_clients holds the ORIGINAL segment, so it matches from_segment.
    insert(con, "raw_clients", [("C001", "Acme", "SME")])
    insert(con, "raw_client_segment_changes", [("C001", "SME", "Enterprise", "2025-06-01")])
    insert(con, "raw_trades", [trade("T1", trade_date=trade_date)])
    insert(con, "raw_fx_rates", [fx_rate(1, "GBP", "USD", 1.27, rate_date=trade_date)])

    dim_clients.stage_clients(con)
    dim_clients.stage_segment_changes(con)
    fact_daily_exposure.stage_trades(con)
    fx_to_usd.stage_fx_rates(con)
    fact_daily_exposure.deduplicate_trades(con)
    fact_daily_exposure.apply_filters(con)
    dim_clients.build_dimension(con)
    fx_to_usd.build_lookup(con)
    fact_daily_exposure.build_trades_enriched(con)
    fact_daily_exposure.build_fact(con)

    rows = con.execute("SELECT segment, trade_count FROM fact_daily_exposure").fetchall()
    assert rows == [(expected_segment, 1)], "exactly one row, carrying the point-in-time segment"

    run_all_checks(con)


def test_dimension_handles_multiple_changes_per_client(con):
    """Interval closing generalises beyond the single change the data contains.

    The supplied dataset holds at most one change per client. The schema permits
    more, so the logic is written with ``lead()`` and tested accordingly rather
    than fitted to the sample.
    """
    insert(con, "raw_clients", [("C001", "Acme", "SME")])
    insert(
        con,
        "raw_client_segment_changes",
        [
            ("C001", "SME", "Enterprise", "2024-01-01"),
            ("C001", "Enterprise", "Strategic", "2025-06-01"),
        ],
    )
    dim_clients.stage_clients(con)
    dim_clients.stage_segment_changes(con)
    dim_clients.build_dimension(con)

    rows = con.execute(
        """
        SELECT segment, effective_start_date, effective_end_date, is_current
        FROM dim_clients ORDER BY effective_start_date
        """
    ).fetchall()

    assert [r[0] for r in rows] == ["SME", "Enterprise", "Strategic"]
    assert rows[0][2] == rows[1][1], "intervals must abut with no gap"
    assert rows[1][2] == rows[2][1]
    assert [r[3] for r in rows] == [False, False, True]


# --------------------------------------------------------------------------
# 5. FX rate resolution
# --------------------------------------------------------------------------


def test_direct_rate_is_preferred_over_inverse(con):
    """Where both a direct and an invertible rate exist, direct wins."""
    insert(
        con,
        "raw_fx_rates",
        [
            fx_rate(1, "GBP", "USD", 1.27),  # direct GBP -> USD
            fx_rate(2, "USD", "GBP", 0.50),  # invertible, deliberately inconsistent
        ],
    )
    fx_to_usd.stage_fx_rates(con)
    fx_to_usd.build_lookup(con)

    rate, source = con.execute(
        "SELECT rate_to_usd, rate_source FROM fx_to_usd WHERE currency = 'GBP'"
    ).fetchone()
    assert rate == pytest.approx(1.27)
    assert source == "direct"


def test_inverse_rate_is_the_reciprocal_of_the_usd_base_row(con):
    """With no direct rate, X -> USD is derived as 1 / (USD -> X).

    This branch is exercised in production, not merely implemented: USD -> CAD is
    the only USD-base row in the feed, CAD *is* a trade base currency (79 raw
    rows), and no CAD -> USD rate exists — so all 65 CAD fact rows take it. The
    fixture pins the arithmetic and, crucially, the direction: inverting an
    existing X -> USD row would give USD -> X, which converts the wrong way.
    """
    insert(con, "raw_fx_rates", [fx_rate(1, "USD", "CAD", 1.25)])
    fx_to_usd.stage_fx_rates(con)
    fx_to_usd.build_lookup(con)

    rate, source = con.execute(
        "SELECT rate_to_usd, rate_source FROM fx_to_usd WHERE currency = 'CAD'"
    ).fetchone()
    assert rate == pytest.approx(0.8)
    assert source == "inverse"


def test_usd_trades_convert_at_parity_and_missing_rates_are_flagged(con):
    """USD converts to itself at 1.0; an uncovered currency-date is ``not_found``.

    No USD -> USD row exists in the feed, so a literal reading of the resolution
    chain would flag every USD trade as ``not_found`` with a NULL converted
    amount — plainly wrong for an identity conversion.
    """
    insert(con, "raw_clients", [("C001", "Acme", "SME")])
    insert(
        con,
        "raw_trades",
        [
            trade("T1", base_currency="USD", quote_currency="EUR", amount=1000.0),
            trade("T2", base_currency="JPY", quote_currency="USD", amount=2000.0),
        ],
    )
    insert(con, "raw_fx_rates", [fx_rate(1, "GBP", "USD", 1.27)])  # no JPY, no USD

    dim_clients.stage_clients(con)
    dim_clients.stage_segment_changes(con)
    fact_daily_exposure.stage_trades(con)
    fx_to_usd.stage_fx_rates(con)
    fact_daily_exposure.deduplicate_trades(con)
    fact_daily_exposure.apply_filters(con)
    dim_clients.build_dimension(con)
    fx_to_usd.build_lookup(con)
    fact_daily_exposure.build_trades_enriched(con)
    fact_daily_exposure.build_fact(con)

    rows = dict(
        con.execute(
            """
            SELECT base_currency, (fx_rate_used, total_amount_usd, fx_rate_source)
            FROM fact_daily_exposure
            """
        ).fetchall()
    )

    assert rows["USD"] == (1.0, 1000.0, "direct")
    assert rows["JPY"] == (None, None, "not_found"), "NULL, not zero — zero would understate sums"

    run_all_checks(con)


def test_invalid_rates_are_filtered_before_deduplication(con):
    """A key whose only rows are NULL and zero disappears entirely.

    This is the duplicate ``(base, quote, date)`` pair found in the feed. Filter
    first and the key falls through correctly; deduplicate first and an arbitrary
    pick could retain the invalid row.
    """
    insert(
        con,
        "raw_fx_rates",
        [
            fx_rate(1, "EUR", "USD", None),
            fx_rate(2, "EUR", "USD", 0.0),
            fx_rate(3, "GBP", "USD", 1.27),
            fx_rate(4, "CHF", "USD", 1.10, rate_date=None),
        ],
    )
    fx_to_usd.stage_fx_rates(con)
    fx_to_usd.build_lookup(con)

    fx_to_usd.assert_feed_unique(con)  # no surviving duplicate key
    currencies = [r[0] for r in con.execute("SELECT currency FROM fx_to_usd").fetchall()]
    assert currencies == ["GBP"]


def test_valid_rate_survives_a_duplicate_key_whose_twin_is_invalid(con):
    """Where a duplicate key pairs a good rate with a bad one, the good one wins.

    This is the shape the real feed actually has: ILS/USD on 2026-02-15 carries
    both 0.275537 and 0.0, and EUR/USD on 2026-02-20 both 1.085145 and NULL. It
    is the case that makes filter-before-deduplicate load-bearing rather than
    merely tidy — deduplicating first with an arbitrary pick could retain the
    zero and convert real trades at a rate of nothing, producing a plausible
    zero-exposure row instead of an error.
    """
    insert(
        con,
        "raw_fx_rates",
        [
            fx_rate(1, "ILS", "USD", 0.275537),
            fx_rate(2, "ILS", "USD", 0.0),
            fx_rate(3, "EUR", "USD", None),
            fx_rate(4, "EUR", "USD", 1.085145),
        ],
    )
    fx_to_usd.stage_fx_rates(con)
    fx_to_usd.assert_feed_unique(con)
    fx_to_usd.build_lookup(con)

    resolved = dict(con.execute("SELECT currency, rate_to_usd FROM fx_to_usd").fetchall())
    assert resolved == {"ILS": 0.275537, "EUR": 1.085145}


# --------------------------------------------------------------------------
# 6. Aggregation
# --------------------------------------------------------------------------


def test_weighted_average_is_amount_weighted_not_arithmetic(con):
    """The agreed rate is weighted by amount, so a large trade dominates."""
    insert(con, "raw_clients", [("C001", "Acme", "SME")])
    insert(
        con,
        "raw_trades",
        [
            trade("T1", amount=1000.0, agreed_rate=1.00),
            trade("T2", amount=9000.0, agreed_rate=2.00),
        ],
    )
    insert(con, "raw_fx_rates", [fx_rate(1, "GBP", "USD", 1.27)])

    dim_clients.stage_clients(con)
    dim_clients.stage_segment_changes(con)
    fact_daily_exposure.stage_trades(con)
    fx_to_usd.stage_fx_rates(con)
    fact_daily_exposure.deduplicate_trades(con)
    fact_daily_exposure.apply_filters(con)
    dim_clients.build_dimension(con)
    fx_to_usd.build_lookup(con)
    fact_daily_exposure.build_trades_enriched(con)
    fact_daily_exposure.build_fact(con)

    trades, base, weighted, usd = con.execute(
        """
        SELECT trade_count, total_amount_base, weighted_avg_agreed_rate, total_amount_usd
        FROM fact_daily_exposure
        """
    ).fetchone()

    assert trades == 2
    assert base == pytest.approx(10_000.0)
    # (1000*1.00 + 9000*2.00) / 10000 = 1.9, not the arithmetic mean of 1.5
    assert weighted == pytest.approx(1.9)
    assert usd == pytest.approx(12_700.0)


def test_null_agreed_rate_leaves_the_average_but_stays_in_the_totals(con):
    """A NULL agreed_rate is excluded from both sides of the weighted average.

    The plain form `sum(amount * agreed_rate) / sum(amount)` is wrong here: the
    numerator skips the NULL product while the denominator still counts that
    row's amount, dragging the average toward zero in proportion to the missing
    row's size. The failure is a plausible number rather than an error, which is
    why the FILTER clauses exist — and why they need a fixture, since the
    supplied data has no NULL agreed_rate to exercise them.
    """
    insert(con, "raw_clients", [("C001", "Acme", "SME")])
    insert(
        con,
        "raw_trades",
        [
            trade("T1", amount=1000.0, agreed_rate=1.20),
            trade("T2", amount=3000.0, agreed_rate=None),  # rate unknown, amount known
        ],
    )
    insert(con, "raw_fx_rates", [fx_rate(1, "GBP", "USD", 1.27)])
    build_analytics(con)

    row = con.execute(
        """
        SELECT trade_count, total_amount_base, weighted_avg_agreed_rate, total_amount_usd
        FROM fact_daily_exposure
        """
    ).fetchone()

    assert row[0] == 2, "the trade is real and still counts"
    assert row[1] == 4000.0, "its amount is known and still sums"
    assert row[2] == pytest.approx(1.20), "only the rated trade contributes to the average"
    assert row[3] == pytest.approx(4000.0 * 1.27), "conversion uses the amount, not the agreed rate"

    # The plain form would have produced 1000*1.20/4000 = 0.30 instead of 1.20.
    assert row[2] != pytest.approx(0.30)


def test_all_null_agreed_rates_yield_null_not_a_division_error(con):
    """Where no trade in a group has a rate, the average is NULL, not an error."""
    insert(con, "raw_clients", [("C001", "Acme", "SME")])
    insert(con, "raw_trades", [trade("T1", agreed_rate=None), trade("T2", agreed_rate=None)])
    insert(con, "raw_fx_rates", [fx_rate(1, "GBP", "USD", 1.27)])
    build_analytics(con)

    row = con.execute(
        "SELECT trade_count, weighted_avg_agreed_rate FROM fact_daily_exposure"
    ).fetchone()
    assert row == (2, None)


def test_status_and_cutoff_exclusions_are_counted_separately(con):
    """Every filter step reports the rows it removed, in a fixed order.

    A row failing several rules is attributed to the first it fails, so the counts
    are sequential rather than independent.
    """
    insert(con, "raw_clients", [("C001", "Acme", "SME")])
    insert(
        con,
        "raw_trades",
        [
            trade("T1"),
            trade("T2", status="CANCELLED"),
            trade("T3", status="PENDING"),
            trade("T4", trade_date="2026-06-02"),
            trade("T5", client_id=None),
            trade("T6", amount=-100.0),
            trade("T7", amount=0.0),
        ],
    )
    dim_clients.stage_clients(con)
    fact_daily_exposure.stage_trades(con)
    fact_daily_exposure.deduplicate_trades(con)
    exclusions = fact_daily_exposure.apply_filters(con)

    assert exclusions["non_active_status"] == 2
    assert exclusions["after_cutoff_date"] == 1
    assert exclusions["missing_client"] == 1
    assert exclusions["non_positive_amount"] == 2
    assert con.execute("SELECT count(*) FROM trades_clean").fetchone()[0] == 1


# --------------------------------------------------------------------------
# 7. Idempotency and quality gates
# --------------------------------------------------------------------------


def test_pipeline_is_idempotent(con):
    """Running the full build twice produces identical output."""
    insert(con, "raw_clients", [("C001", "Acme", "SME"), ("c001", "Acme", "SME")])
    insert(con, "raw_client_segment_changes", [("C001", "SME", "Enterprise", "2025-06-01")])
    insert(
        con,
        "raw_trades",
        [
            trade("T1", trade_date="2025-01-10", created_at="2024-11-01 10:00:00"),
            trade("T1", trade_date="2025-01-10", amount=999.0, created_at="2024-12-01 10:00:00"),
            trade("T2", trade_date="2026-01-15", base_currency="eur"),
        ],
    )
    insert(
        con,
        "raw_fx_rates",
        [
            fx_rate(1, "GBP", "USD", 1.27, rate_date="2025-01-10"),
            fx_rate(2, "EUR", "USD", 1.09, rate_date="2026-01-15"),
        ],
    )

    build_analytics(con)
    first_fact = con.execute("SELECT * FROM fact_daily_exposure ORDER BY ALL").fetchall()
    first_dim = con.execute("SELECT * FROM dim_clients ORDER BY ALL").fetchall()

    build_analytics(con)
    second_fact = con.execute("SELECT * FROM fact_daily_exposure ORDER BY ALL").fetchall()
    second_dim = con.execute("SELECT * FROM dim_clients ORDER BY ALL").fetchall()

    assert first_fact == second_fact
    assert first_dim == second_dim


def test_quality_check_detects_a_duplicate_rate_key(con):
    """The rate uniqueness assertion fires rather than silently picking a row.

    Two *valid* rates for one key would fan out the fact table. No source
    precedence rule is defined because none should be needed post-filter — if
    that stops being true, this raises rather than guessing.
    """
    insert(
        con,
        "raw_fx_rates",
        [
            fx_rate(1, "GBP", "USD", 1.27, source="feed_a"),
            fx_rate(2, "GBP", "USD", 1.29, source="feed_b"),
        ],
    )
    fx_to_usd.stage_fx_rates(con)

    with pytest.raises(DataQualityError, match="more than one"):
        fx_to_usd.assert_feed_unique(con)


# --------------------------------------------------------------------------
# 8. Quality gate failure paths
#
# A check that has never been observed to fail is not known to work. Each test
# below constructs the exact corruption the assertion exists to catch and proves
# it raises — the happy path alone would pass just as well against a check whose
# body had been deleted.
# --------------------------------------------------------------------------


def _minimal_build(con):
    """A small but complete build, used as the starting point for corruption."""
    insert(con, "raw_clients", [("C001", "Acme", "SME")])
    insert(con, "raw_trades", [trade("T1"), trade("T2", base_currency="EUR")])
    insert(
        con,
        "raw_fx_rates",
        [fx_rate(1, "GBP", "USD", 1.27), fx_rate(2, "EUR", "USD", 1.09)],
    )
    build_analytics(con)


def test_quality_check_detects_a_fact_grain_violation(con):
    """A duplicated grain key raises — this is the dimension fan-out detector."""
    _minimal_build(con)
    con.execute(
        """
        INSERT INTO fact_daily_exposure
        SELECT * FROM fact_daily_exposure WHERE base_currency = 'GBP'
        """
    )

    with pytest.raises(DataQualityError, match="declared grain is violated"):
        fact_daily_exposure.assert_grain_unique(con)


def test_quality_check_detects_unorderable_segment_changes(con):
    """Two changes for one client on one date raise rather than resolve arbitrarily.

    This is the case where a structurally valid dimension is semantically wrong.
    With no sequence column the `lead()` ordering resolves the tie arbitrarily,
    one interval collapses to zero length and is dropped, and a segment vanishes
    from the client's history — while the interval-integrity check still passes,
    because what remains is contiguous, non-overlapping and single-current.
    """
    insert(con, "raw_clients", [("C001", "Acme", "SME")])
    insert(
        con,
        "raw_client_segment_changes",
        [
            ("C001", "SME", "Mid", "2026-02-01"),
            ("C001", "Mid", "Enterprise", "2026-02-01"),  # same date, no way to order
        ],
    )
    dim_clients.stage_clients(con)
    dim_clients.stage_segment_changes(con)

    with pytest.raises(DataQualityError, match="more than one segment change"):
        dim_clients.assert_change_log_orderable(con)

    # And the check it protects would NOT have caught it: build anyway and show
    # the resulting dimension passes interval validation while having lost "Mid".
    dim_clients.build_dimension(con)
    dim_clients.assert_intervals_valid(con)
    segments = [r[0] for r in con.execute("SELECT segment FROM dim_clients ORDER BY effective_start_date").fetchall()]
    assert "Mid" not in segments, "the dropped segment is exactly what makes this silent"


def test_quality_check_detects_overlapping_dimension_intervals(con):
    """An overlapping interval raises: a point-in-time lookup would fan out."""
    _minimal_build(con)
    con.execute(
        """
        INSERT INTO dim_clients
        VALUES ('C001', 'Acme', 'Enterprise', DATE '2020-01-01', DATE '9999-12-31', TRUE)
        """
    )

    with pytest.raises(DataQualityError, match="overlap or leave a gap"):
        dim_clients.assert_intervals_valid(con)


def test_quality_check_detects_trades_lost_in_the_dimension_join(con):
    """Fact trade counts not reconciling with the cleaned set raises.

    Catches both directions at once — rows dropped by the inner dimension join
    and rows multiplied by a fan-out — because either leaves totals wrong with
    nothing raised.
    """
    _minimal_build(con)
    con.execute("DELETE FROM fact_daily_exposure WHERE base_currency = 'GBP'")

    with pytest.raises(DataQualityError, match="reconciliation failed"):
        fact_daily_exposure.assert_no_trades_lost(con)


def test_quality_check_detects_an_inconsistent_conversion(con):
    """A `not_found` row carrying a USD amount raises."""
    _minimal_build(con)
    con.execute(
        """
        UPDATE fact_daily_exposure
        SET fx_rate_source = 'not_found'
        WHERE base_currency = 'GBP'
        """
    )

    with pytest.raises(DataQualityError, match="inconsistent rate source"):
        fact_daily_exposure.assert_conversions_consistent(con)


def test_failure_messages_report_how_many_rows_offended(con):
    """A quality error must say *how many* rows are wrong, not just that some are.

    "One bad key to chase" and "the feed is broken" call for different responses,
    and the person reading the alert cannot get that number any other way. This
    is pinned because the count is supplied by a shared helper — a refactor that
    drops the placeholder would otherwise degrade every message at once, silently
    and without failing anything.
    """
    _minimal_build(con)
    con.execute(
        """
        INSERT INTO fact_daily_exposure
        SELECT * FROM fact_daily_exposure WHERE base_currency = 'GBP'
        """
    )

    with pytest.raises(DataQualityError) as excinfo:
        fact_daily_exposure.assert_grain_unique(con)

    assert excinfo.value.args[0].startswith("1 "), "the offending count must lead the message"


def test_a_failed_check_leaves_the_previous_target_intact(con, tmp_path):
    """A rejected load must not overwrite the last good target.

    ``CREATE OR REPLACE TABLE`` auto-commits, and the checks run last — so
    without an enclosing transaction a failing assertion aborts the process only
    *after* the target has been replaced by the data it just rejected. The
    previous, good tables would already be gone and only a non-zero exit code
    would say so.
    """
    source = tmp_path / "raw.duckdb"
    target = tmp_path / "analytics.duckdb"

    src = duckdb.connect(str(source))
    src.execute("CREATE TABLE raw_clients (client_id VARCHAR, client_name VARCHAR, segment VARCHAR)")
    src.execute(
        "CREATE TABLE raw_client_segment_changes "
        "(client_id VARCHAR, from_segment VARCHAR, to_segment VARCHAR, effective_date DATE)"
    )
    src.execute(
        "CREATE TABLE raw_fx_rates (rate_id INTEGER, base_currency VARCHAR, "
        "quote_currency VARCHAR, mid_rate DOUBLE, rate_date DATE, source VARCHAR)"
    )
    src.execute(
        "CREATE TABLE raw_trades (trade_id VARCHAR, client_id VARCHAR, trade_date DATE, "
        "status VARCHAR, base_currency VARCHAR, quote_currency VARCHAR, amount DOUBLE, "
        "amount_in_thousands BOOLEAN, agreed_rate DOUBLE, created_at TIMESTAMP)"
    )
    src.execute("INSERT INTO raw_clients VALUES ('C001', 'Acme', 'SME')")
    src.execute(
        "INSERT INTO raw_trades VALUES ('T1','C001',DATE '2026-01-15','ACTIVE','GBP','USD',"
        "1000.0,FALSE,1.25,TIMESTAMP '2025-10-01 09:00:00')"
    )
    src.execute("INSERT INTO raw_fx_rates VALUES (1,'GBP','USD',1.27,DATE '2026-01-15','feed_a')")
    src.close()

    run_pipeline(source_db=source, target_db=target)
    good = duckdb.connect(str(target), read_only=True)
    baseline = good.execute("SELECT * FROM fact_daily_exposure ORDER BY ALL").fetchall()
    good.close()
    assert baseline, "the first run must produce a target to protect"

    # Corrupt the feed so the rate uniqueness assertion fires on the next run.
    src = duckdb.connect(str(source))
    src.execute("INSERT INTO raw_fx_rates VALUES (2,'GBP','USD',9.99,DATE '2026-01-15','feed_b')")
    src.close()

    with pytest.raises(DataQualityError):
        run_pipeline(source_db=source, target_db=target)

    after = duckdb.connect(str(target), read_only=True)
    preserved = after.execute("SELECT * FROM fact_daily_exposure ORDER BY ALL").fetchall()
    after.close()

    assert preserved == baseline, "a rejected load must roll back, not overwrite the good target"


def test_original_state_reference_table_does_not_leak_into_the_dimension(con):
    """The dimension is correct even when raw_clients holds the original segment.

    ``raw_clients.segment`` matches ``from_segment``, so the reference table is an
    original-state snapshot. Joining it straight onto trades would stamp every
    trade with the client's *original* segment regardless of date — the mirror
    image of the naive current-segment bug, and equally wrong.

    Building from the change log alone avoids that. This test pins the behaviour
    by giving raw_clients a segment that is deliberately stale.
    """
    insert(con, "raw_clients", [("C001", "Acme", "SME")])  # original, now stale
    insert(con, "raw_client_segment_changes", [("C001", "SME", "Enterprise", "2025-06-01")])
    dim_clients.stage_clients(con)
    dim_clients.stage_segment_changes(con)
    dim_clients.build_dimension(con)

    rows = con.execute(
        "SELECT segment, is_current FROM dim_clients ORDER BY effective_start_date"
    ).fetchall()
    assert rows == [("SME", False), ("Enterprise", True)]
    assert dim_clients.reconcile_segment_chain(con) == 0


def test_segment_chain_inconsistency_is_reported_not_fatal(con):
    """A reference table that disagrees with the change log warns rather than fails.

    The dimension never reads raw_clients.segment for a reclassified client, so
    the mismatch cannot corrupt the output. Failing the load would block a correct
    result over an upstream defect the pipeline has already routed around.
    """
    insert(con, "raw_clients", [("C001", "Acme", "Strategic")])  # agrees with neither
    insert(con, "raw_client_segment_changes", [("C001", "SME", "Enterprise", "2025-06-01")])
    dim_clients.stage_clients(con)
    dim_clients.stage_segment_changes(con)
    dim_clients.build_dimension(con)

    assert dim_clients.reconcile_segment_chain(con) == 1  # reported
    rows = con.execute(
        "SELECT segment FROM dim_clients ORDER BY effective_start_date"
    ).fetchall()
    assert rows == [("SME",), ("Enterprise",)]  # dimension still correct
