# DECISIONS.md

This document records the decisions taken where the assignment requirements were
ambiguous, incomplete, or in tension with each other, together with the data
observations that drove them.

Every decision below traces back to something observed in the source data rather
than something assumed. Where the source data did not exercise a rule, the rule
is still implemented to specification and covered by a synthetic test fixture.

---

## 1. Profiling summary

Findings from profiling `source_data/grain_raw.duckdb` before any code was
written. These drive the decisions in the following sections.

**`raw_clients`**

- Contains duplicate client records under case and whitespace variants of the
  same identifier: `c007` / `C007`, and `C003` / `C003 ` (trailing blank).
- Duplicate pairs agree on both `client_name` and `segment`.
- Neither duplicated client appears in `raw_client_segment_changes`.

**`raw_client_segment_changes`**

- Exactly one row per `client_id` in the current dataset.
- No `valid_to` / `effective_end` column — intervals must be derived.
- `raw_clients.segment` matches `from_segment`, establishing that `raw_clients`
  holds each client's **original** state, not their current classification.
- No multiple changes per client on the same `effective_date`.
- Contains no rows for the duplicated client identifiers.

**`raw_fx_rates`** — 1,095 rows

- Seven currency pairs, all quoted against USD except one: `AUD/USD`, `CHF/USD`,
  `EUR/USD`, `GBP/USD`, `ILS/USD`, `JPY/USD`, and `USD/CAD`.
- 156 distinct `rate_date` values spanning 2026-01-01 to 2026-06-05 — a 156-day
  range, so coverage is **complete on a full calendar**, weekends and holidays
  included, with no gaps.
- Three invalid rows, one of each defect the requirement names:
  `ILS/USD` on 2026-02-15 with `mid_rate = 0`, `EUR/USD` on 2026-02-20 with a
  NULL `mid_rate`, and a `GBP/USD` row with a NULL `rate_date`.
- The first two of those create duplicate
  `(base_currency, quote_currency, rate_date)` keys — and in both cases the
  duplicate pairs a **valid** rate with the invalid one, from the same `source`
  (`reuters`). This is what makes the filter-before-deduplicate ordering
  load-bearing; see section 8.1.
- No `USD → USD` row exists.
- No SGD row exists in any position, as base or quote.

**`raw_trades`** — 692 rows

- 15 `trade_id` values appear exactly twice; none appear three or more times.
  677 distinct trades, so 2.2% of trades carry a superseded version. Duplicates
  generally differ in `amount`.
- `created_at` is never NULL and is never tied within a duplicated `trade_id`,
  so it provides a usable ordering column for version selection.
- `created_at` normally precedes `trade_date` by several months. Five rows have a
  `created_at` greater than `trade_date`; of those, four fall on a genuinely
  later calendar day and one is the same day at a later hour (`trade_date` is a
  `DATE`, so the comparison promotes it to midnight). Neither case affects
  version selection, since `created_at` is used only to order versions within a
  `trade_id`.
- Duplicate `trade_id` rows always share the same `status` — verified directly,
  and the basis for section 4.
- `status` values: `ACTIVE` (557), `CANCELLED` (79), `PENDING` (56).
- `quote_currency` is `USD` on every row. `base_currency` contains 4 NULLs and
  mixed case (8 rows spelled `eur`).
- Trade `base_currency` values: ILS (95), SGD (94), GBP (93), CHF (89), EUR (89,
  including the lowercase rows), **CAD (79)**, AUD (72), JPY (67), **NIS (10)**,
  NULL (4).
- **CAD is a trade base currency**, and the feed carries no `CAD → USD` row — so
  the inverse branch is exercised in production, not merely implemented. See
  section 8.3.
- **USD is never a trade base currency.** The USD parity rule of section 8.4 is
  therefore defensive rather than load-bearing on this data.
- `NIS` and `SGD` have no rate under those codes. They are different problems and
  get different treatment — see section 6.1.
- 5 rows carry a NULL `client_id`. Every non-NULL `client_id` resolves to
  `raw_clients` after canonicalisation.
- `amount_in_thousands` has no NULLs: 549 rows false, 143 true.
- `agreed_rate` has **no NULLs, no zeros and no negative values** across all 692
  rows.

---

## 2. Client identity and deduplication

**Observation.** `raw_clients` contains the same logical client under multiple
identifier spellings. `c007` differs from `C007` by case; `C003 ` differs from
`C003` by a trailing blank (confirmed by byte-length inspection — not a
zero-width or homoglyph character, so trimming is a sufficient fix). The
duplicate pairs agree on name and segment, so there is no survivorship conflict
to resolve.

**Decision.** Client identifiers are canonicalised as `upper(trim(client_id))`
before any join or grouping. Duplicates collapsing to the same canonical key are
reduced to a single dimension row.

**Rationale.** Left uncorrected, this defect has two distinct failure modes, both
silent:

- On the **fact side**, a client's exposure splits across two rows that should be
  one, understating per-client totals.
- On the **dimension side**, a one-to-many join fans out the fact table and
  duplicates every affected row, breaking the declared grain.

In this dataset only the second risk is live — `raw_trades` references neither
the lowercase `c007` nor the padded `C003 ` variant, so no fact-side split
occurs. The normalisation is applied regardless, because the defect exists in the
source and the pipeline should not depend on which variants the trade feed
happens to use today.

The same `upper(trim(...))` canonicalisation is applied to `client_id` in
`raw_trades` and `raw_client_segment_changes`, so that all three tables join on a
consistent key.

---

## 3. `dim_clients` — Type 2 slowly changing dimension

**Requirement.** The table must support point-in-time lookups so that historical
analyses reflect the segment a client was in at the time of the trade.

**Decision.** A Type 2 SCD with one row per client per segment interval, keyed by
canonical `client_id` and bounded by `effective_start_date` and
`effective_end_date`.

### 3.1 Reconstruction direction

`raw_clients.segment` matches `from_segment` in the change log, which establishes
that the reference table holds each client's **original** classification, not
their current one. History is therefore built **forward**:

- A client **with** a change row yields two intervals — `from_segment` from the
  beginning of time until `effective_date`, then `to_segment` from
  `effective_date` onward.
- A client **with no** change row yields a single interval spanning all time,
  carrying `raw_clients.segment`. For these clients the distinction is moot: with
  no reclassification, original and current segment are the same value.

**Why this matters beyond the implementation.** Because `raw_clients` is an
original-state snapshot rather than a current-state one, joining
`raw_clients.segment` directly onto trades would stamp every trade with the
client's *original* segment irrespective of date. That is the mirror image of the
naive current-segment bug the requirement warns against, and produces wrong
answers just as reliably — it simply fails in the opposite direction, which makes
it harder to spot by inspection.

The dimension is consequently built from the change log alone, which is
self-describing: each interval takes its segment from `from_segment` or
`to_segment` rather than from the reference table. The build is therefore correct
under either interpretation of what `raw_clients.segment` represents, which is
the property worth having when the semantics of a source column are asserted
rather than documented.

### 3.2 Reconciliation rather than assertion

The pipeline logs, but does not fail on, disagreement between
`raw_clients.segment` and the earliest `from_segment` for a client, and on any
discontinuity in the change chain (where one row's `from_segment` does not equal
the previous row's `to_segment`).

These are reported at warning level rather than raised because they cannot
corrupt the output: the dimension does not read `raw_clients.segment` for
reclassified clients, so a mismatch is a statement about the *reference table*
rather than about the dimension. Failing the load on it would block a correct
result over an upstream inconsistency the pipeline has already routed around. The
count is surfaced so the disagreement is visible and can be raised with the
source system owner.

### 3.3 Generalising beyond the observed data

The current dataset contains at most one change per client. The interval closing
logic is nevertheless implemented generically, using
`lead(effective_date) over (partition by client_id order by effective_date)`, so
that clients with two or more changes produce a correct interval chain without
modification.

This costs nothing on the present data and avoids a rebuild the first time a
client is reclassified twice — which the schema clearly permits even though the
sample does not contain it.

### 3.4 Interval boundary convention

Intervals are **half-open**: `effective_start_date <= trade_date <
effective_end_date`.

This is the only convention under which a trade falling exactly on a change date
cannot match two dimension rows. A closed-closed convention would double-join on
that boundary and silently duplicate fact rows.

### 3.5 Open boundaries

Rather than NULLs, open intervals use sentinel dates: `1900-01-01` for the
earliest `effective_start_date` and `9999-12-31` for the current
`effective_end_date`. This keeps the point-in-time join to a plain `BETWEEN`-style
predicate with no NULL handling, which is both simpler to read and less prone to
three-valued-logic mistakes.

An `is_current` boolean flag is included as a convenience for consumers who want
present-day segmentation without a date predicate.

### 3.6 Trades predating all known history

Not applicable in practice, since the earliest interval extends back to the
sentinel floor date and therefore covers any trade date. Documented here because
the requirement is silent on it and the behaviour should be explicit rather than
incidental.

---

## 4. Trade deduplication

**Observation.** `trade_id` is not unique. Duplicate rows generally differ in
`amount`. A `created_at` timestamp is available and duplicate rows always share
the same `status`.

**Decision.** Deduplicate by `trade_id`, keeping the row with the earliest
`created_at`, with the remaining business columns as a deterministic tiebreaker.
Deduplication runs **before** the status and date filters.

**Rationale on ordering.** The requirements state both "keep the earliest
version" and "include only ACTIVE trades", which conflict when a `trade_id`
appears with different statuses across versions: filtering first would retain a
later `ACTIVE` row whose earliest version was `CANCELLED`, while deduplicating
first would drop it.

This was checked directly — all 15 duplicated `trade_id` values share the same
status in this dataset, so the two orderings produce identical results here. The
deduplicate-first ordering is chosen anyway, because "keep the earliest version"
reads as a property of the raw record set rather than of the filtered subset, and
because a later `ACTIVE` row following a `CANCELLED` one is more plausibly a bad
resubmission than a genuine reinstatement.

**Rationale on the tiebreaker.** Idempotency requires a stable total ordering.
`created_at` alone is not guaranteed unique, and physical row order — `rowid` or
an unordered scan — is not a guarantee the database owes us across runs. The
tiebreak is therefore **content-based**: after `created_at`, the ordering falls
through to `amount`, `agreed_rate`, `status` and `base_currency`. Ordering on the
row's own values is stable by construction in a way that position is not, so the
same input file cannot produce a different winner on a later run.

In this dataset no duplicate pair actually ties on `created_at`, so the
tiebreaker never fires. It costs nothing and removes a class of run-to-run
difference that would otherwise be invisible until it happened.

**On `created_at` anomalies.** Five rows have a `created_at` later than their
`trade_date` — four on a genuinely later day, one the same day at a later hour.
These are recorded as a data-quality observation and are not treated specially:
`created_at` is used purely as a version-ordering signal within a `trade_id`, not
as a business date, so the anomaly does not affect version selection.

---

## 5. Amount normalisation

**Observation.** `amount_in_thousands` is a boolean column with no NULLs.

**Decision.** Where `amount_in_thousands` is true, the amount is multiplied by
1,000 to produce a canonical amount in whole base-currency units. Normalisation
happens **before** deduplication and before the positive-amount filter.

**Rationale.** Ordering matters in both directions. Normalising before
deduplication means two versions of the same trade recorded under different
conventions — for example `5000 / false` and `5 / true` — are correctly seen as
carrying the same value rather than as a genuine amount discrepancy. Normalising
before the exclusion filter means the zero-or-negative test is applied to real
values rather than to scaled ones.

A defensive default of `false` is applied should a NULL appear in future data, so
that an unflagged row is never silently inflated by three orders of magnitude.

---

## 6. Currency normalisation

**Observation.** `base_currency` and `quote_currency` in `raw_trades` contain
mixed case; `base_currency` also contains NULLs.

**Decision.** Currency codes are canonicalised as `upper(trim(...))` and
validated against the ISO 4217 three-letter pattern. Normalisation runs **before**
the "missing currency information" exclusion.

The identical normalisation is applied to `base_currency` and `quote_currency` in
`raw_fx_rates`.

**Rationale.** Applying the exclusion before normalisation would discard valid
rows such as `usd` or `Usd ` as malformed. Applying normalisation to the trades
table but not the rates table would cause the FX join to miss silently — the
requirement mentions normalisation only under the trades section, but the join
cannot be correct unless both sides are treated the same way.

`status` values receive the same `upper(trim(...))` treatment. The requirement
does not call for it, but the defect class is identical to the currency case and
an untrimmed `ACTIVE ` would be excluded silently.

### 6.1 Non-ISO currency codes — `NIS` → `ILS`

**Observation.** Ten trades carry `base_currency = 'NIS'`. `NIS` is the
colloquial code for the New Israeli Sheqel; its ISO 4217 code is `ILS`, and the
rate feed publishes that currency only under `ILS`. All ten are `ACTIVE`, and
form a contiguous identifier block, `T00621`–`T00630`.

The evidence that these are Israeli shekel trades is **comparative**, not
absolute. Individually the ten `agreed_rate` values deviate from the `ILS` feed
rate on the same date by between 0.08% and 7.09%, which on its own proves
little — trades are agreed at a spread to mid, and a 7% deviation is not
self-evidently a match.

What identifies them is that **no other currency behaves this way.** Taking the
ratio of `agreed_rate` to the market mid-rate on the same trade date, per
currency:

| Trade currency | n | mean ratio | std dev |
|---|---|---|---|
| **NIS** (vs `ILS` feed) | 10 | **1.011** | **0.053** |
| GBP | 93 | 0.606 | 0.331 |
| CHF | 89 | 0.691 | 0.399 |
| EUR | 89 | 0.756 | 0.416 |
| AUD | 72 | 1.129 | 0.742 |
| ILS | 91 | 2.776 | 1.563 |
| JPY | 67 | 120.191 | 76.006 |

Every currency in this dataset carries an `agreed_rate` that is essentially
uncorrelated with the market rate — the values are synthetic noise. The ten `NIS`
rows are the sole exception, centring on the `ILS` mid-rate at a ratio of 1.011
with an order of magnitude less dispersion than any other currency. They were
generated from real `ILS` rates while everything else was randomised.

A consequence worth stating: because `agreed_rate` is uncorrelated with the
market across the rest of the dataset, `weighted_avg_agreed_rate` is computed
correctly but is not economically meaningful on this data. The arithmetic is
right; the inputs are synthetic.

**Decision.** An explicit alias map in `config.py` rewrites `NIS` to `ILS` during
currency canonicalisation, applied to both `raw_trades` and `raw_fx_rates` so the
two sides of the FX join stay consistent. The number of rewritten values is
logged.

**Rationale.** This is the defect that case folding cannot catch, and it is worth
being precise about why. `NIS` is already three uppercase letters, so it passes
the ISO 4217 *shape* test in section 7 and is never excluded. It then matches
nothing in the rate feed. The result is a trade that survives every filter, is
counted in `trade_count` and `total_amount_base`, and arrives in the fact table
looking entirely healthy — with `fx_rate_source = 'not_found'` and a NULL USD
exposure. Nothing errors and no count looks wrong. The requirement to normalise
codes "to standard ISO 4217 format" is not satisfied by case folding alone; a
code that is not an ISO code is exactly what that instruction is about.

**On the boundary of this rule.** `SGD` also resolves to `not_found` — 73 trades
— and is deliberately **left alone**. The distinction is the point: `NIS` is a
documented alias for a currency the feed already carries, so resolving it
recovers a rate that exists. `SGD` is a real ISO code for which no market data
was supplied at all, so there is no rate to recover and a NULL exposure is the
honest answer. Rewriting an alias and inventing a rate are different acts, and
the alias map is an explicit whitelist rather than fuzzy matching so that the
line between them stays visible.

Were the pipeline to encounter further non-ISO codes, the correct response is to
add them to the map deliberately after confirming the mapping — not to broaden
the rule into guesswork.

---

## 7. Exclusions and filtering

Filters are applied in the following order, with a record count logged before and
after each step:

1. Amount unit normalisation (no exclusion)
2. Identifier and currency normalisation, including non-ISO alias resolution
   (no exclusion — see section 6.1)
3. Deduplicate `trade_id`, earliest `created_at`
4. Exclude non-`ACTIVE` status
5. Exclude `trade_date > 2026-06-01`
6. Exclude NULL or unresolvable `client_id`
7. Exclude NULL or non-conforming `base_currency` / `quote_currency`
8. Exclude `amount <= 0`

**Observed counts on the supplied data.** 692 raw rows, 15 superseded versions
removed, 677 entering the filter chain:

| Step | Excluded | Remaining |
|---|---|---|
| Non-`ACTIVE` status | 133 | 544 |
| `trade_date` after 2026-06-01 | 5 | 539 |
| Missing or unresolvable `client_id` | 5 | 534 |
| Missing or non-ISO-shaped currency | 4 | 530 |
| Zero or negative amount | 3 | 527 |

The status count reconciles as follows: 135 rows are non-`ACTIVE` in the raw
table (79 `CANCELLED`, 56 `PENDING`), of which 2 were already removed as
superseded duplicate versions, leaving 133 for this step to exclude. That gap is
the deduplicate-before-filter ordering of section 4 made visible in the counts.

**On the limits of the ISO shape test.** The currency exclusion tests that a code
matches `^[A-Z]{3}$` after normalisation. That catches a NULL or a malformed
value, but it cannot catch a well-formed code that is not an ISO one — which is
the `NIS` case, and the reason alias resolution belongs in normalisation
(step 2) rather than here. The 4 rows excluded at this step are all NULL
`base_currency`.

**On overlapping exclusions.** A single row may violate several rules at once.
Because filters are applied sequentially, each row is attributed to the *first*
rule it fails, and the logged counts are therefore sequential rather than
independent. They should be read as "removed at this step", not as "total rows
violating this rule". The ordering above is fixed so that the counts are
reproducible.

**On "missing client information".** This is read as covering both NULL
`client_id` and a `client_id` with no match in `raw_clients`. In this dataset
only the NULL case occurs — every non-NULL identifier resolves after
canonicalisation — but both are handled, since an unresolvable reference is as
much a gap as an absent one.

**On `trade_date` boundary.** `trade_date` is typed `DATE`, so the
`<= 2026-06-01` comparison needs no cast and carries no risk of silently
excluding same-day trades after midnight. Had it been a timestamp, an explicit
cast would have been required.

---

## 8. FX rate resolution

**Requirement.** Prefer a direct rate; fall back to inverting a `USD → base`
rate; otherwise flag as `not_found`.

### 8.1 Rate feed cleaning, and why order matters

Invalid rates are excluded — NULL `mid_rate`, zero `mid_rate`, and NULL
`rate_date` — **before** any deduplication of the rate feed.

The feed contains two duplicate `(base_currency, quote_currency, rate_date)` keys,
both from the same `source` (`reuters`), and in each case the duplicate pairs a
**valid** rate with an invalid one:

| Key | Rows |
|---|---|
| `ILS/USD` 2026-02-15 | `0.275537` and `0.0` |
| `EUR/USD` 2026-02-20 | `1.085145` and NULL |

This ordering is therefore load-bearing rather than merely tidy. Filtering first
removes the invalid twin and leaves exactly one valid rate on each key.
Deduplicating first — with an arbitrary "pick one" — could retain the zero on
2026-02-15 and discard a perfectly good rate. Two real ILS trades fall on that
date, so the consequence would have been two fact rows converting at a rate of
nothing: `total_amount_usd = 0`, no error raised, and a zero indistinguishable
from genuine zero exposure.

The third invalid row, a `GBP/USD` rate with a NULL `rate_date`, is not part of a
duplicate key and is simply dropped — it cannot be placed on a timeline.

After filtering, a uniqueness assertion is applied on
`(base_currency, quote_currency, rate_date)`. No precedence rule between sources
is defined, because none is needed: post-filter, exactly one row per key should
remain. If that assertion ever fires, the correct response is to define a
precedence rule deliberately, not to guess at one silently now.

Negative rates are excluded alongside zeros. The requirement names only zero, but
a negative FX rate is not a meaningful quantity, and inverting one would silently
produce a negative converted amount rather than failing.

### 8.2 Direction of inversion

The fact table converts a **base-currency** amount into USD, so for a trade with
base currency X the pipeline needs an `X → USD` rate. Inversion therefore runs in
one direction only: where no `X → USD` row exists but the feed carries `USD → X`,
the rate is derived as `X → USD = 1 / (USD → X)`.

Inverting an existing `X → USD` row would instead yield `USD → X`, which converts
USD amounts *into* X — the wrong direction for this pipeline, and unnecessary in
any case, since a currency with a direct rate never reaches the inverse branch.

**`quote_currency` plays no part in rate resolution.** It is a grain column of
the fact table, but the lookup is keyed on `(base_currency, trade_date)` alone,
because the measure being produced is the base amount expressed in USD — not the
base amount expressed in the quote currency. Joining on the trade's own
`quote_currency` would be a natural misreading and a damaging one: a trade quoted
into anything other than USD would find no matching feed row and fall to
`not_found` despite the feed carrying everything needed to convert it. On this
data `quote_currency` is USD on every row, so the two readings coincide and the
distinction is invisible in the output — which is exactly why it is recorded
here rather than left to be inferred from the SQL.

### 8.3 Rate matrix and branch coverage

Read as a matrix against the trade currencies actually present, the feed
determines exactly which branches are exercised:

| Trade base currency | Resolution | Fact rows | Trades |
|---|---|---|---|
| GBP, EUR, ILS, CHF, AUD, JPY | Direct `base → USD` | 376 | 387 |
| CAD | Inverse of `USD → CAD` | 65 | 67 |
| SGD | `not_found` — no feed coverage | 72 | 73 |
| USD | Special case, rate `1.0` | 0 | 0 — USD is never a trade base currency |

`USD → CAD` is the only row in the feed where USD appears as the base, so
inversion can only ever serve CAD. CAD **is** a trade base currency — 79 raw
rows, 67 after cleaning — and no `CAD → USD` row exists, so **every CAD trade
takes the inverse branch**. It is exercised on real data, and the reciprocal
direction of section 8.2 is what makes those 65 rows correct rather than
inverted.

Two rows of this matrix are worth reading carefully because they cut in opposite
directions:

- **`SGD` is genuinely unresolvable.** The feed contains no SGD row in any
  position. `not_found` with a NULL exposure is the correct outcome, not a
  failure to try — see section 6.1 for why this is treated differently from the
  `NIS` alias, which *is* recoverable.
- **The USD parity rule is defensive, not load-bearing.** Every trade in the
  supplied data quotes *into* USD, so no trade has USD as its base and the
  special case never fires. It is retained because the rule is correct and the
  schema plainly permits a USD-base trade; it is covered by a synthetic fixture
  rather than by production rows. The honest claim is that it prevents a future
  defect, not that it prevents a current one.

### 8.4 USD-denominated trades

**Observation.** No `USD → USD` row exists in the feed.

**Decision.** USD-base trades are assigned a rate of `1.0` and labelled
`direct`.

**Rationale.** Followed literally, the direct/inverse/not_found chain would send
every USD trade to `not_found` with a NULL converted amount, which is plainly
wrong — a USD amount converted to USD is the amount itself. `direct` is chosen
over inventing a fourth enum value because the requirement fixes the three
permitted values, and an identity conversion is more honestly described as
direct than as not found. The special case is applied before the lookup rather
than as a fallback after it, so it cannot be masked by a spurious feed row.

**Scope of this rule on the supplied data.** No trade in the source has USD as
its base currency — `quote_currency` is USD on all 692 rows — so this branch is
never taken in production. It is retained as defensive handling of a case the
schema permits, and covered by a test fixture rather than by real rows.

### 8.5 No temporal fallback

**Observation.** The feed publishes rates on weekends and holidays, giving full
calendar coverage across the trade date range.

**Decision.** No carry-forward of the last known rate is implemented. The rate
for a trade is taken from the trade date itself, or the row is flagged
`not_found`.

**Rationale.** Carry-forward is standard production practice where a market data
feed follows a trading calendar and trades can land on non-publishing days. That
condition does not hold here — the feed is complete across the calendar — so
carry-forward would add unreachable complexity and deviate from a specification
that defines only three resolution outcomes. Were the feed to move to a trading
calendar, this is the decision that would need revisiting first.

### 8.6 Unconverted rows

Where a rate resolves to `not_found`, `rate_used` is NULL and `total_amount_usd`
is NULL rather than zero. A NULL correctly signals "not calculable"; a zero would
be indistinguishable from a genuine zero-exposure row and would silently
understate any downstream sum.

---

## 9. Weighted average agreed rate

**Decision.** `weighted_avg_agreed_rate` is the amount-weighted mean of
`agreed_rate`, using the unit-normalised amount and evaluated within the
`(date, client, base_currency, quote_currency)` group:

```sql
sum(amount * agreed_rate) FILTER (WHERE agreed_rate IS NOT NULL)
  / nullif(sum(amount)    FILTER (WHERE agreed_rate IS NOT NULL), 0)
```

**On the `FILTER` clauses.** `agreed_rate` was confirmed to contain **no NULLs,
no zeros and no negative values** across all 692 source rows, so on this data the
filters are inert and the expression reduces to `sum(amount * agreed_rate) /
sum(amount)`. They are written explicitly anyway, because the plain form is
wrong the moment a NULL appears: `sum(amount * agreed_rate)` would skip the NULL
product while `sum(amount)` still counted that row's amount, dragging the average
toward zero in proportion to the missing row's size. The failure would be a
plausible-looking number rather than an error, which is the kind worth spending a
`FILTER` clause on.

A row with a NULL `agreed_rate` therefore leaves both the numerator and the
denominator, but is **retained** in `trade_count` and `total_amount_base` — the
trade is real and its amount is known even where its rate is not. Where every row
in a group has a NULL rate the result is NULL rather than a division error, and
the pipeline logs a warning naming the affected row count.

Because amounts are already filtered to strictly positive values, the denominator
cannot be zero for a group that exists.

---

## 10. Idempotency

Running `python pipeline.py` twice produces identical table contents. Verified on
the real source database by taking a SHA-256 over both output tables under an
explicit `ORDER BY` on their key columns, across two consecutive runs: 18
`dim_clients` rows and 513 `fact_daily_exposure` rows, identical digests both
times.

The ordering in that check is deliberate. `CREATE OR REPLACE TABLE` guarantees
identical *contents*, not identical physical row order, so a hash taken over an
unordered scan would be testing something the database does not promise — it
could fail on a correct run, or pass by luck. Sorting first makes the check test
idempotency rather than storage layout.

Idempotency itself rests on four properties:

- **Full replace, never append.** Target tables are written with
  `CREATE OR REPLACE TABLE`, so a rerun cannot accumulate rows.
- **Deterministic deduplication.** Version selection is ordered by `created_at`
  and then by the row's own business columns, leaving no arbitrary choice and no
  dependence on physical position.
- **Deterministic rate resolution.** The uniqueness assertion on the cleaned rate
  feed guarantees a single candidate per key, so no tiebreak is needed.
- **No wall-clock or random inputs.** No `current_date`, `now()`, or generated
  identifier participates in any output value. The `2026-06-01` cutoff is a
  literal, not a relative date.

The `target/` directory is created if absent, so the pipeline runs correctly from
a clean checkout.

---

## 11. Data quality assertions

Beyond the required exclusion logging, the pipeline runs five assertions that
**raise and abort the load**, and one reconciliation that **warns without
failing**. All six pass on the supplied data.

Raising:

1. **Rate feed key uniqueness** after invalid rows are filtered — see section 8.1.
2. **Dimension interval integrity** — no overlapping intervals and no gaps within
   a client's timeline, no zero-length or inverted intervals, and at most one
   current row per client.
3. **Fact grain uniqueness** — exactly one row per
   `(trade_date, client_id, base_currency, quote_currency)`.
4. **Trade count reconciliation** — the sum of `trade_count` in the fact table
   equals the number of cleaned trades (527). This catches loss and duplication
   in the same check: rows dropped by the inner dimension join, and rows
   multiplied by a fan-out.
5. **Conversion consistency** — a `not_found` row has a NULL `total_amount_usd`
   and a resolved row does not, and no `fx_rate_used` is zero or negative.

Warning only:

6. **Segment chain reconciliation** — see section 3.2 for why this one does not
   raise.

**Why these, and why raising.** Every failure mode above is silent by
construction. A fan-out from the dimension join or from a duplicate rate row does
not raise an error; it produces a well-formed table with inflated numbers that
looks entirely normal until somebody reconciles it against something else. A
dropped row is worse still, because the total simply comes out lower and nothing
indicates that it should not have. These are asserted rather than assumed
precisely because inspection would not catch them.

A partially-correct analytical table is worse than an absent one, because it will
be trusted. Where a check can distinguish "this output is wrong" from "this
upstream input is untidy", it raises; where it cannot corrupt the output, it
warns and names the count.

---

## 12. Implementation structure

Two requirements are really design constraints, and the structure follows from
them. "At least 3 meaningful unit tests covering actual business logic" and "log
how many records are excluded at each filtering step" are both unsatisfiable by a
single monolithic query — there would be nothing to test in isolation and no
intermediate count to observe. The logic is therefore decomposed into a staging
layer, a counted filter chain, and separate dimension, rate and fact builds.

Transformations are DuckDB SQL rather than pandas. Source and target are both
DuckDB, the work is entirely set-based, and round-tripping through dataframes
would add conversion cost and type drift for no benefit. `pandas` and `numpy` are
available in `requirements.txt` but are not used; the requirement permits any of
the listed libraries rather than mandating them, and reaching for one here would
have made the code slower and harder to review.

Testability is preserved by having each transformation take a connection and read
from named tables. The source database is attached as `src`, and DuckDB resolves
`src.raw_trades` identically whether `src` is an attached database or a plain
schema — so the tests build an in-memory `src` schema and run the *production* SQL
unmodified. The tests exercise the real logic rather than a Python
reimplementation of it.

---

## 13. Known limitations

Deliberate scope boundaries, recorded so they are visible rather than accidental:

- **Excluded rows are counted, not quarantined.** Production practice would route
  rejected records to a quarantine table for investigation. Here they are logged
  as counts only, per the stated requirement.
- **The dimension is rebuilt in full on each run.** Appropriate at this data
  volume and required for idempotency; a production SCD2 would apply incremental
  merge logic against the existing dimension.
- **No source precedence rule for FX rates.** Justified above — none is needed
  post-filter, and the assertion will surface the need if the feed changes.
- **The USD parity branch is covered by a synthetic fixture only.** No trade in
  the supplied data has USD as its base currency, so section 8.4's rule is never
  exercised in production. The `inverse` and `not_found` branches, by contrast,
  are both exercised on real rows — see section 8.3.
- **`trade_count` counts deduplicated trades**, not raw rows. Stated explicitly
  because "number of trades" is ambiguous where the source contains duplicates.
- **73 SGD trades carry no USD exposure.** The feed supplies no SGD rate in any
  position, so `total_amount_usd` is NULL for those rows and any USD total
  computed from this table excludes them. This is correct rather than
  regrettable — the alternative is inventing a rate — but it is a real gap in
  analytical coverage and belongs on this list rather than buried in a rate
  source column. The fix is upstream: obtain SGD market data.
- **The currency alias map is a whitelist of one.** `NIS → ILS` is resolved
  because it was confirmed against the feed (section 6.1). Any further non-ISO
  code will resolve to `not_found` until somebody adds it deliberately. That is
  the intended failure mode — the alternative is fuzzy matching that silently
  invents currency identities.
