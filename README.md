# Grain analytics pipeline

Transforms the raw operational database `source_data/grain_raw.duckdb` into an
analytical layer at `target/grain_analytics.duckdb`, containing two tables:

| Table | Grain | Purpose |
|---|---|---|
| `dim_clients` | one row per client per segment interval | Type 2 SCD supporting point-in-time segment lookups |
| `fact_daily_exposure` | one row per `(date, client, base_currency, quote_currency)` | daily FX exposure, converted to USD |

The reasoning behind every ambiguous choice is in **[`DECISIONS.md`](DECISIONS.md)**.
This file explains *what the code is and how it works*; `DECISIONS.md` explains
*why it does what it does*. Where the two overlap, this file cross-references
rather than repeats.

---

## Quick start

```bash
pip install -r requirements.txt

# place the source database first — it is not distributed with this project
mkdir -p source_data && cp /path/to/grain_raw.duckdb source_data/

python pipeline.py        # builds target/grain_analytics.duckdb
python -m pytest -q       # 32 tests
```

The run prints its progress to stdout and mirrors it to `logs/pipeline.log`.
It exits `0` on success and `1` if a data quality check fails.

Requirements: Python 3.9+, and `duckdb`, `pandas`, `numpy`, `pytest` from
`requirements.txt`. Only `duckdb` and `pytest` are actually imported — see
[Why no pandas](#why-no-pandas).

---

## Output tables

### `dim_clients`

| Column | Type | Notes |
|---|---|---|
| `client_id` | VARCHAR | canonical form, `upper(trim(...))` |
| `client_name` | VARCHAR | |
| `segment` | VARCHAR | the segment held **during this interval** |
| `effective_start_date` | DATE | inclusive |
| `effective_end_date` | DATE | **exclusive** |
| `is_current` | BOOLEAN | true where the interval is open-ended |

Intervals are **half-open** — `start <= date < end` — which is the only
convention under which a trade falling exactly on a change date cannot match two
dimension rows. Open ends use sentinel dates (`1900-01-01`, `9999-12-31`) rather
than NULLs, so the point-in-time join stays a plain range predicate.

A client reclassified on 2026-02-10 produces two rows:

```
C004 | Delta Merchants | SME        | 1900-01-01 | 2026-02-10 | false
C004 | Delta Merchants | Enterprise | 2026-02-10 | 9999-12-31 | true
```

A client never reclassified produces one row spanning all time:

```
C003 | Cornerstone FX  | Enterprise | 1900-01-01 | 9999-12-31 | true
```

### `fact_daily_exposure`

| Column | Type | Notes |
|---|---|---|
| `trade_date` | DATE | grain |
| `client_id` | VARCHAR | grain |
| `client_name` | VARCHAR | from `dim_clients` |
| `segment` | VARCHAR | **as at `trade_date`**, not current |
| `base_currency` | VARCHAR | grain |
| `quote_currency` | VARCHAR | grain |
| `trade_count` | BIGINT | deduplicated trades, not raw rows |
| `total_amount_base` | DOUBLE | unit-normalised |
| `weighted_avg_agreed_rate` | DOUBLE | amount-weighted |
| `fx_rate_used` | DOUBLE | NULL where unresolved |
| `total_amount_usd` | DOUBLE | NULL where unresolved, never 0 |
| `fx_rate_source` | VARCHAR | `direct` \| `inverse` \| `not_found` |

One row of each resolution outcome, from the real data:

```
2026-01-12 | C006 | Frontier Fintech  | SME        | EUR | USD | 2 | 4057463.87 | 1.274079 | 1.084060 | 4398534.28 | direct
2026-02-09 | C009 | Ironclad Finance  | Enterprise | CAD | USD | 2 |    1117.53 | 0.765378 | 0.733989 |     820.25 | inverse
2026-01-24 | C011 | Keystone Forex    | Enterprise | SGD | USD | 2 |    6018.58 | 0.563774 |     NULL |       NULL | not_found
```

The `not_found` row shows the deliberate NULL: a zero would be indistinguishable
from genuine zero exposure and would silently understate any downstream `sum()`.

**The point-in-time join, visible in the output.** C004's trades either side of
its 2026-02-10 reclassification carry different segments, and the trade *on* the
change date takes the new one:

```
2026-01-28 | SME        | AUD
2026-02-04 | SME        | SGD
2026-02-10 | Enterprise | GBP   <- on the change date: half-open interval
2026-02-14 | Enterprise | GBP
```

---

## Repository layout

```
pipeline.py                  entry point — `python pipeline.py`
requirements.txt             duckdb, pandas, numpy, pytest
pytest.ini                   testpaths and pythonpath
ASSIGNMENT.md                the brief, kept for reference
DECISIONS.md                 every ambiguous decision and its rationale
README.md                    this file

grain_pipeline/
    __init__.py              package docstring, re-exports run_pipeline
    config.py                every business literal in one place
    logging_setup.py         stdout + logs/pipeline.log
    staging.py               all normalisation
    cleaning.py              deduplication, then the counted filter chain
    dimensions.py            dim_clients (Type 2 SCD)
    rates.py                 direct / inverse rate resolution to USD
    facts.py                 point-in-time join, then the daily aggregate
    quality.py               assertions that fail loudly, plus one that warns
    run.py                   orchestration and connection management

tests/
    conftest.py              in-memory `src` schema fixtures and row builders
    test_pipeline.py         32 tests

target/grain_analytics.duckdb    the built output
logs/pipeline.log                the most recent run's log
```

### Dependency structure

Strictly two-layer, no cycles:

```
config.py ──> logging_setup.py ──┐
    │                            │
    └────────────────────────────┴──> staging  cleaning  dimensions
                                      rates    facts     quality
                                                 │
                                                 └──> run.py ──> pipeline.py
```

`config.py` and `logging_setup.py` are leaves. Every transformation module
depends on those two and nothing else, which is what allows any of them to be
imported and driven in isolation by a test. `run.py` is the only module that
knows about the others.

This is also why `logging_setup.py` is a separate module rather than part of
`run.py`: every transformation calls `get_logger()`, so putting it in `run.py` —
which imports every transformation — would be a circular import.

---

## File-by-file

### `pipeline.py`

The entry point the brief requires. A thin wrapper: it puts the project root on
`sys.path` so the run works from a clean checkout without installation, calls
`run_pipeline()`, and converts an uncaught exception into a logged error and exit
code `1`. No business logic.

### `grain_pipeline/config.py`

Every business literal, so that no value is buried in a query and a reviewer can
see all of them at once:

- paths (`SOURCE_DB`, `TARGET_DB`, `LOG_FILE`) derived from the project root
- `SOURCE_SCHEMA = "src"` — the name the source database is attached under
- `TRADE_DATE_CUTOFF`, `ACTIVE_STATUS`, `USD`
- `DATE_FLOOR` / `DATE_CEILING` — the SCD2 sentinel bounds
- `ISO_4217_PATTERN` — `^[A-Z]{3}$`
- `CURRENCY_ALIASES` — `{"NIS": "ILS"}`, the non-ISO codes to resolve
- the three `fx_rate_source` enum values

Keeping these here is also what makes the idempotency claim checkable: no
`current_date` or `now()` appears anywhere, so there is one place to verify that.

### `grain_pipeline/logging_setup.py`

Configures a named logger writing to both stdout and `logs/pipeline.log`.

The exclusion counts are a **stated deliverable**, not debug output, so they go
through the logging framework rather than `print()`. `configure_logging()` is
idempotent — calling it twice does not duplicate handlers — and degrades to
stdout alone if the log file cannot be opened, on the grounds that losing a log
file is recoverable and losing the load is not.

### `grain_pipeline/staging.py`

All normalisation, before any filtering or joining. Four builders, each producing
a temp table:

| Function | Output | Does |
|---|---|---|
| `build_stg_trades` | `stg_trades` | amount units, identifiers, currencies, status |
| `build_stg_clients` | `stg_clients` | canonicalises and collapses duplicate client identifiers |
| `build_stg_segment_changes` | `stg_segment_changes` | canonicalises; drops rows with no effective date |
| `build_stg_fx_rates` | `stg_fx_rates` | canonicalises; removes invalid rates |

Two helpers carry most of the logic:

- `_canonical_text(col)` → `upper(nullif(trim(col), ''))`. Trims, uppercases, and
  folds the empty string to NULL so a blank identifier is handled by the same
  exclusion as a true NULL.
- `_canonical_currency(col)` → the above, plus a `CASE` resolving
  `CURRENCY_ALIASES`. Built from the config dict, so adding an alias needs no
  change here.

**Ordering is the point of this module,** and three orderings are deliberate:

1. **Amount units before deduplication** — so `5000/false` and `5/true` are seen
   as the same value rather than as a real discrepancy.
2. **Currency normalisation before the missing-currency exclusion** — so `usd`
   and `" Eur "` are not discarded as malformed.
3. **Alias resolution as part of normalisation, not as a filter** — `NIS` is
   already three uppercase letters, so it passes the ISO *shape* test and is
   never excluded; it simply matches nothing in the rate feed. See
   `DECISIONS.md` §6.1.

The same normalisation is applied to `raw_fx_rates` as to `raw_trades`, so both
sides of the FX join see one spelling per currency.

`_log_currency_aliases()` reports how many values each alias rewrote, so the
remap is visible rather than silent.

### `grain_pipeline/cleaning.py`

Deduplication, then the counted exclusion chain.

`deduplicate_trades()` reduces `stg_trades` to one row per `trade_id`, keeping
the earliest `created_at`. The tiebreak is **content-based** — `amount`,
`agreed_rate`, `status`, `base_currency` — rather than positional, because
physical row order (`rowid`, or an unordered scan) is not a guarantee the
database owes us across runs, and idempotency requires a total ordering.

Deduplication runs **before** the filters. See `DECISIONS.md` §4 — this is one of
the decisions most worth understanding, because "keep the earliest version" and
"include only ACTIVE trades" conflict when versions disagree on status.

`apply_filters()` walks `FILTER_STEPS`, a tuple of `FilterStep(name, predicate,
description)` records. For each step it creates the next filtered table, counts
what was removed, logs it, and swaps the tables. It **returns** the counts as a
dict as well as logging them, which is what lets a test assert on the exclusions
rather than parse log output:

```python
exclusions = apply_filters(con)
assert exclusions["missing_or_invalid_currency"] == 1
```

The steps, in fixed order:

| Step | Excludes |
|---|---|
| `non_active_status` | status is not `ACTIVE` |
| `after_cutoff_date` | `trade_date` missing or after 2026-06-01 |
| `missing_client` | `client_id` NULL or not resolving to a known client |
| `missing_or_invalid_currency` | base/quote currency NULL or not ISO-shaped |
| `non_positive_amount` | amount NULL, zero or negative |

Because the filters are sequential, a row violating several rules is attributed
to the **first** one it fails. The counts are "removed at this step", not "total
rows violating this rule". The order is fixed so the numbers are reproducible.

### `grain_pipeline/dimensions.py`

Builds `dim_clients` as a Type 2 SCD from three interval sets unioned together:

- `pre_change_intervals` — floor date until a client's first change, carrying
  `from_segment`
- `post_change_intervals` — each change until the next, or until the ceiling
  sentinel for the most recent; carries `to_segment`, closed with
  `lead(effective_date) OVER (PARTITION BY client_id ORDER BY effective_date)`
- `unchanged_intervals` — one all-time row for clients with no change record

History is built **forward**, because `raw_clients.segment` matches
`from_segment` in the change log — meaning the reference table holds each
client's *original* classification, not their current one. The dimension is
therefore built from the change log alone, which is self-describing. See
`DECISIONS.md` §3.1: joining `raw_clients.segment` onto trades directly would
stamp every trade with the client's *original* segment regardless of date — the
mirror image of the naive current-segment bug, and harder to spot.

The `lead()` closing logic is generic, so a client reclassified two or more times
produces a correct chain without modification, even though the supplied data has
at most one change each.

### `grain_pipeline/rates.py`

Builds `fx_to_usd`: one resolved rate per `(currency, date)`.

The fact converts a **base-currency** amount into USD, so for base currency X the
pipeline needs `X → USD`. Two candidate sources are unioned and ranked:

| Source | Condition | Rate | Preference |
|---|---|---|---|
| `direct` | feed row `X → USD` | `mid_rate` | 0 |
| `inverse` | feed row `USD → X` | `1 / mid_rate` | 1 |

`QUALIFY row_number() OVER (PARTITION BY currency, rate_date ORDER BY preference,
rate_to_usd) = 1` makes direct win wherever both exist, per the requirement.

**Direction matters.** Inversion is `X → USD = 1 / (USD → X)`. Inverting an
existing `X → USD` row would give `USD → X`, which converts the wrong way.

This module reads the already-cleaned `stg_fx_rates`, so it needs no guard
against zero or NULL rates — which matters, because the inverse branch divides by
the rate.

Note that `quote_currency` plays **no part** in rate resolution — the lookup is
keyed on `(base_currency, trade_date)` alone, because the measure is the base
amount expressed in USD. See `DECISIONS.md` §8.2.

### `grain_pipeline/facts.py`

Two stages, so each is legible and separately testable.

`build_trades_enriched()` attaches to each cleaned trade:

- the **point-in-time segment**, via an inner join to `dim_clients` on
  `trade_date >= effective_start_date AND trade_date < effective_end_date`
- the **USD rate**, via a left join to `fx_to_usd` on currency and date

USD-base trades are special-cased to rate `1.0` / `direct` *before* the lookup,
so the identity conversion cannot be masked by a spurious feed row. An unmatched
left join produces `coalesce(r.rate_source, 'not_found')`.

`build_fact_daily_exposure()` groups to the declared grain. Two aggregates
deserve attention:

```sql
sum(amount * agreed_rate) FILTER (WHERE agreed_rate IS NOT NULL)
  / nullif(sum(amount)    FILTER (WHERE agreed_rate IS NOT NULL), 0)
```

The `FILTER` clauses are inert on the supplied data — `agreed_rate` has no NULLs
— but the plain form is wrong the moment one appears: the numerator would skip
the NULL product while the denominator still counted that row's amount, dragging
the average toward zero. The failure would be a plausible number rather than an
error.

```sql
sum(amount * fx_rate_used) AS total_amount_usd
```

Where no rate resolved, every term is NULL and `sum()` returns NULL — the
intended "not calculable", never zero.

`fx_rate_used` and `fx_rate_source` are taken with `max()`, which is safe because
both are constant within a group: the rate is keyed by currency and date, and
both are grain columns.

### `grain_pipeline/quality.py`

Five assertions that **raise** and abort the load, and one reconciliation that
**warns**. All are run by `run_all_checks()`.

| Check | Catches |
|---|---|
| `assert_rate_feed_unique` | duplicate rate keys surviving cleaning — a fan-out source |
| `assert_dimension_intervals_valid` | overlapping or gapped intervals, zero-length intervals, multiple current rows |
| `assert_fact_grain_unique` | the declared grain being violated — usually a dimension fan-out |
| `assert_no_trades_lost` | trade counts not reconciling — loss *and* duplication in one check |
| `assert_conversions_consistent` | a `not_found` row with a USD amount, or a resolved row without one, or a non-positive rate |
| `reconcile_segment_chain` | **warns only** — reference table disagreeing with the change log |

Every failure above is **silent by construction**. A fan-out does not raise an
error; it produces a well-formed table with inflated numbers that looks entirely
normal until somebody reconciles it against something else. A dropped row is
worse, because the total simply comes out lower and nothing indicates it should
not have. These are asserted rather than assumed precisely because inspection
would not catch them.

`reconcile_segment_chain` warns rather than raises because it cannot corrupt the
output — the dimension never reads `raw_clients.segment` for a reclassified
client, so a mismatch is a statement about the *reference table*. Failing the
load would block a correct result over an upstream inconsistency the pipeline has
already routed around. The count is surfaced so it can be raised with the source
system owner. See `DECISIONS.md` §3.2.

### `grain_pipeline/run.py`

`build_analytics(con, source_schema)` runs every transformation against an open
connection, in dependency order. It is deliberately separate from connection
management, which is what lets the tests drive the whole pipeline against an
in-memory database.

`run_pipeline(source_db, target_db)` handles the rest: it checks the source
exists and raises a message naming the expected path if not, creates `target/` if
absent, connects to the **target** database, attaches the **source** as `src` in
`READ_ONLY` mode, builds inside a transaction, and detaches in a `finally` block.

**The transaction is what makes the quality checks protective rather than
informative.** `CREATE OR REPLACE TABLE` auto-commits, and the checks run last —
so without it, a failing assertion would abort the process only *after* the
target had been overwritten with the data it just rejected, destroying the last
good load. DDL in DuckDB is transactional, so a `DataQualityError` rolls the
whole build back and the previous target survives intact. See `DECISIONS.md`
§11.1.

Connecting to the target and attaching the source (rather than the reverse) is
what makes `CREATE OR REPLACE TABLE dim_clients` write to the target by default,
and `READ_ONLY` guarantees the raw database cannot be modified.

---

## How a run looks

```
Staged clients: 15 raw rows collapsed to 13 canonical clients (2 duplicate identifier variants merged).
Staged segment changes: 5 rows.
Staged trades: 692 rows (amount units, identifiers, currencies and status normalised).
  Currency alias applied: NIS -> ILS on 10 trade currency values (NIS is not an ISO 4217 code; the rate feed publishes ILS).
Staged FX rates: 1,095 raw rows, 3 excluded as invalid (NULL/zero/negative rate or missing date), 1,092 retained.
Deduplicated trades: 692 rows in, 15 superseded versions removed, 677 distinct trades out.
Filter chain starting with 677 trades.
  [non_active_status           ] excluded    133 rows (status is not ACTIVE) | 544 remaining
  [after_cutoff_date           ] excluded      5 rows (trade_date is missing or later than 2026-06-01) | 539 remaining
  [missing_client              ] excluded      5 rows (client_id is missing or does not resolve to a known client) | 534 remaining
  [missing_or_invalid_currency ] excluded      4 rows (base or quote currency is missing or not a valid ISO 4217 code) | 530 remaining
  [non_positive_amount         ] excluded      3 rows (amount is missing, zero or negative) | 527 remaining
Filter chain complete: 150 trades excluded in total, 527 retained.
Built dim_clients: 18 interval rows across 13 clients (13 current). 0 rows with an unknown segment.
Resolved FX rates to USD: 1,092 (currency, date) pairs across 7 currencies — 936 direct, 156 inverse.
Built fact_daily_exposure: 513 rows covering 527 trades.
  FX rate resolution: 376 direct, 65 inverse, 72 not_found.
Running data quality checks.
  PASS  FX rate feed is unique on (base_currency, quote_currency, rate_date).
  PASS  dim_clients intervals are contiguous, non-overlapping and single-current per client.
  PASS  Segment change chain is continuous and agrees with the client reference table.
  PASS  fact_daily_exposure is unique on its declared grain.
  PASS  Trade counts reconcile: 527 cleaned trades accounted for in the fact table.
  PASS  USD conversions are consistent with their recorded rate source.
All data quality checks passed.
```

The status count reconciles as follows: 135 rows are non-`ACTIVE` in the raw
table (79 `CANCELLED`, 56 `PENDING`), of which 2 had already been removed as
superseded duplicate versions — leaving 133 for that step. That gap is the
deduplicate-before-filter ordering made visible in the counts.

---

## Business rules and where each is enforced

| Rule (from the brief) | Enforced in |
|---|---|
| Only `ACTIVE` trades | `cleaning.py` — `FILTER_STEPS[0]` |
| Only `trade_date <= 2026-06-01` | `cleaning.py` — `FILTER_STEPS[1]` |
| Amounts in the correct unit | `staging.py` — `build_stg_trades`, before dedup |
| Currency codes normalised to ISO 4217 | `staging.py` — `_canonical_currency` |
| Exclude missing client or currency | `cleaning.py` — `FILTER_STEPS[2..3]` |
| Exclude zero or negative amount | `cleaning.py` — `FILTER_STEPS[4]` |
| Keep the earliest version of a trade | `cleaning.py` — `deduplicate_trades` |
| Direct rate, else inverse, else `not_found` | `rates.py` + `facts.py` |
| Exclude zero/NULL/undated FX rates | `staging.py` — `build_stg_fx_rates` |
| Segment as at the trade date | `dimensions.py` + the join in `facts.py` |
| Idempotent | `run.py` — see below |
| Log exclusions at each step | `cleaning.py` — `apply_filters` |

---

## Tests

32 tests in `tests/test_pipeline.py`, organised into eight sections.

### How they work

`conftest.py` provides a `con` fixture: an in-memory DuckDB with an empty `src`
schema mirroring the source tables. DuckDB resolves `src.raw_trades` identically
whether `src` is an **attached database** or a **plain schema** — so the tests run
the *production SQL unmodified* against fixture data. They exercise the real
transformation logic, not a Python reimplementation of it. **This property is
what makes the tests worth anything, and it should be preserved.**

Two builders keep the fixtures readable by letting a test override only what it
cares about:

```python
trade("T1", base_currency="NIS", amount=1000.0, agreed_rate=0.27)
fx_rate(1, "ILS", "USD", 0.275)
```

### What they cover

**1. Amount unit normalisation**
- `test_amount_in_thousands_is_scaled_and_survives_deduplication` — the flag
  scales the amount, and scaling happens before dedup, so `5000/false` and
  `5/true` are recognised as the same value.

**2. Deduplication**
- `test_deduplication_keeps_earliest_created_at`
- `test_deduplication_is_deterministic_across_insertion_orders` — the same rows
  inserted in a different physical order still select the same winner. This is
  the test that pins the content-based tiebreak.

**3. Normalisation ordering and exclusions**
- `test_lowercase_currency_is_normalised_not_excluded` — `gbp` and `" Eur "`
  survive; a genuine NULL is excluded. Normalising *after* the filter would
  discard the first two, which is the trap in the requirement's ordering.
- `test_non_iso_currency_alias_is_resolved_to_its_iso_code` — `NIS` resolves to
  `ILS` and converts directly, while `SGD` (no feed coverage at all) correctly
  stays `not_found`. The contrast row is the point: resolving a documented alias
  is a different act from inventing a rate.
- `test_client_identifier_variants_collapse_without_fanout` — `c007`/`C007` and
  `C003`/`C003 ` collapse to one client without fanning out the fact.

**4. Point-in-time segment lookup**
- `test_segment_reflects_classification_on_the_trade_date` — parametrised over
  four dates including the change date itself, pinning the half-open boundary.
- `test_dimension_handles_multiple_changes_per_client` — two changes produce a
  correct three-interval chain, a case the supplied data never exercises.

**5. FX rate resolution**
- `test_direct_rate_is_preferred_over_inverse`
- `test_inverse_rate_is_the_reciprocal_of_the_usd_base_row`
- `test_usd_trades_convert_at_parity_and_missing_rates_are_flagged` — USD at
  `1.0`/`direct`; an uncovered currency at NULL/`not_found`, **not zero**.
- `test_invalid_rates_are_filtered_before_deduplication` — a key whose only rows
  are NULL and zero disappears entirely.
- `test_valid_rate_survives_a_duplicate_key_whose_twin_is_invalid` — the shape
  the real feed actually has. This is the case that makes filter-before-dedupe
  load-bearing: dedupe-first with an arbitrary pick could retain a `0.0` and
  convert real trades at a rate of nothing.

**6. Aggregation**
- `test_weighted_average_is_amount_weighted_not_arithmetic` — a large trade at
  one rate and a small one at another must not produce the arithmetic mean.
- `test_status_and_cutoff_exclusions_are_counted_separately` — each row is
  attributed to the first rule it fails, so the counts are reproducible.

**7. Idempotency and quality checks**
- `test_pipeline_is_idempotent` — two full builds produce identical contents.
- `test_quality_check_detects_a_duplicate_rate_key` — the assertion actually
  fires. A check that has never been seen to fail is not known to work.
- `test_original_state_reference_table_does_not_leak_into_the_dimension`
- `test_segment_chain_inconsistency_is_reported_not_fatal` — an inconsistent
  chain is counted and warned about, and the load still completes.

**8. Quality gate failure paths** — each of the five raising assertions gets the
corruption it exists to catch, and must raise. Testing only the happy path would
pass equally well against a check whose body had been deleted.
- `test_quality_check_detects_a_fact_grain_violation`
- `test_quality_check_detects_overlapping_dimension_intervals`
- `test_quality_check_detects_trades_lost_in_the_dimension_join`
- `test_quality_check_detects_an_inconsistent_conversion`
- `test_a_failed_check_leaves_the_previous_target_intact` — builds a good target,
  corrupts the feed, and asserts the failed rerun leaves the good rows in place
  rather than overwriting them.

### Mutation-tested

The suite was validated by breaking the production code and confirming something
goes red. All of these are caught: the half-open interval (`<` → `<=`), dedup
ordering (earliest → latest), rate inversion (`1/r` → `r`), direct-beats-inverse
preference, removal of the NIS alias, removal of the USD parity branch, stripping
the dedup tiebreak columns, removing the weighted-average `FILTER` clauses,
dropping the ISO regex from the currency filter, dropping the client-resolution
subquery, changing the cutoff from `<=` to `<`, removing the build transaction,
and gutting any one of the five raising assertions.

Running a subset:

```bash
python -m pytest -q -k dedup          # deduplication tests
python -m pytest -q -k "rate or fx"   # FX resolution tests
python -m pytest -v                   # names of all 32
```

---

## Idempotency

`python pipeline.py` twice produces identical table contents. Verified on the
real source database by taking a SHA-256 over both output tables under an
explicit `ORDER BY`, across two consecutive runs — identical digests both times.

The ordering in that check is deliberate: `CREATE OR REPLACE TABLE` guarantees
identical *contents*, not identical physical row order, so a hash over an
unordered scan would be testing something the database does not promise.

It rests on four properties:

- **Full replace, never append** — `CREATE OR REPLACE TABLE`, so a rerun cannot
  accumulate rows; and the build runs in one transaction, so a rerun that fails
  partway leaves the previous contents rather than a half-written mixture.
- **Deterministic deduplication** — ordered by `created_at` then by the row's own
  business columns; no arbitrary choice and no dependence on physical position.
- **Deterministic rate resolution** — the uniqueness assertion on the cleaned
  feed guarantees a single candidate per key.
- **No wall-clock or random inputs** — no `current_date`, `now()` or generated
  identifier participates in any output value. The `2026-06-01` cutoff is a
  literal, not a relative date.

---

## Why no pandas

`pandas` and `numpy` are listed in `requirements.txt` because the brief permits
them, but neither is imported. Source and target are both DuckDB, the work is
entirely set-based, and round-tripping through dataframes would add conversion
cost and type drift for no benefit. The brief permits any of the listed libraries
rather than mandating them. See `DECISIONS.md` §12.

---

## Notable findings in the source data

Full detail in `DECISIONS.md` §1; the ones that changed the code:

- **`NIS` is not an ISO 4217 code.** Ten `ACTIVE` trades use it for the Israeli
  shekel, whose ISO code is `ILS`. It is three uppercase letters, so it passes
  the shape test and is never excluded — it simply matches nothing in the feed,
  and the trades reach the fact table looking healthy with a NULL USD exposure.
  Each one's `agreed_rate` sits within ~2% of the `ILS` rate on the same date.
  Resolved via `CURRENCY_ALIASES`. (§6.1)
- **`SGD` has no feed coverage at all** — 73 trades, correctly `not_found`. The
  contrast with `NIS` is deliberate: one is a recoverable alias, the other is
  genuinely missing market data. (§13)
- **Duplicate rate keys pair a *valid* rate with an invalid one**, not two
  invalid rows — which is what makes filtering before deduplication
  load-bearing rather than tidy. (§8.1)
- **`CAD` is a trade base currency with no `CAD → USD` feed row**, so every CAD
  trade takes the inverse branch — 65 fact rows. (§8.3)
- **No trade has `USD` as its base currency**, so the parity rule of §8.4 is
  defensive rather than load-bearing on this data.
- **Client identifiers vary by case and whitespace** — `c007`/`C007`,
  `C003`/`C003 `. Left uncorrected, the dimension join fans out. (§2)
- **`raw_clients.segment` matches `from_segment`**, so it is an *original-state*
  snapshot rather than a current-state one. (§3.1)
