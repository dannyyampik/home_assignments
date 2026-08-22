# DECISIONS.md

Decisions taken where the requirements were ambiguous, incomplete, or in tension
with each other, and the data observations that drove them.

---

## 1. What the profiling found

Established by direct query before any code was written.

**`raw_clients`** — 15 rows, 13 distinct clients
- Duplicate identifiers for one logical client: `c007`/`C007` (case),
  `C003`/`C003 ` (trailing blank, confirmed by byte length — not a zero-width or
  homoglyph character, so `trim` suffices). Both pairs agree on name and segment.
- `segment` matches `from_segment` in the change log, so this is an
  **original-state** snapshot, not current state.

**`raw_client_segment_changes`** — 5 rows
- One row per client, for 5 of the 13 clients. No `valid_to` column: intervals
  must be derived. No sequence column, and no two changes share a client and date.

**`raw_fx_rates`** — 1,095 rows
- Seven pairs, all quoted against USD except `USD/CAD`: `AUD`, `CHF`, `EUR`,
  `GBP`, `ILS`, `JPY` → USD, plus USD → `CAD`.
- 156 distinct dates over a 156-day span (2026-01-01 to 2026-06-05) — complete on
  a **full calendar**, weekends included.
- Three invalid rows, one per defect the brief names: `ILS/USD` 2026-02-15 with
  `mid_rate = 0`, `EUR/USD` 2026-02-20 with a NULL rate, and a `GBP/USD` row with
  a NULL date.
- The first two create duplicate `(base, quote, date)` keys — each pairing a
  **valid** rate with the invalid one. See §8.1.
- No `USD → USD` row. No SGD row in any position.

**`raw_trades`** — 692 rows
- 15 `trade_id`s appear exactly twice (2.2%); none more. 677 distinct.
- Duplicate rows always share a `status`, and never tie on `created_at`.
- `created_at` is never NULL. Five rows have `created_at > trade_date` — four on a
  later calendar day, one the same day at a later hour.
- `status`: `ACTIVE` 557, `CANCELLED` 79, `PENDING` 56.
- `quote_currency` is `USD` on every row. `base_currency` has 4 NULLs and 8 rows
  spelled `eur`.
- Base currencies: ILS 95, SGD 94, GBP 93, CHF 89, EUR 89, **CAD 79**, AUD 72,
  JPY 67, **NIS 10**, NULL 4.
- **CAD is a base currency** with no `CAD → USD` feed row, so the inverse branch
  is exercised in production. **USD is never a base currency**, so the parity rule
  of §8.4 is defensive rather than load-bearing.
- 5 NULL `client_id`; every non-NULL one resolves after canonicalisation.
- `amount_in_thousands` has no NULLs (549 false, 143 true).
- `agreed_rate` has **no NULLs, no zeros, no negatives**.

---

## 2. Client identity

**Observation.** The same logical client appears under several identifier
spellings, and the pairs agree on name and segment, so there is no survivorship
conflict.

**Decision.** Canonicalise as `upper(trim(client_id))` in all three tables that
carry it, and collapse duplicates to one row.

**Why.** Uncorrected, this fails silently in two ways: on the fact side a
client's exposure splits across rows that should be one; on the dimension side a
one-to-many join fans out the fact and duplicates every affected row. Only the
second is live here — no trade references `c007` or `C003 ` — but the defect is
in the source, and the pipeline should not depend on which spellings the trade
feed happens to use today.

---

## 3. `dim_clients` — Type 2 SCD

One row per client per segment interval, keyed by canonical `client_id` and
bounded by `effective_start_date` / `effective_end_date`.

### 3.1 Both sources are needed, for different things

`raw_clients.segment` matches `from_segment`, which establishes that it holds the
client's **original** classification. Joining it onto trades directly would stamp
every trade with the original segment regardless of date — the mirror image of
the naive current-segment bug the brief warns about, and harder to spot because it
fails in the less obvious direction.

The dimension therefore uses:

| Source | Supplies |
|---|---|
| `raw_clients` | the client universe, and `client_name` |
| `raw_client_segment_changes` | the history, for the 5 clients that have one |

For a client **with** changes, each interval takes its segment from
`from_segment` or `to_segment`, so the history is self-describing and does not
depend on how `raw_clients.segment` is interpreted. History is built forward:
`from_segment` from the floor date until the change, `to_segment` from the change
onward.

For a client with **no** change row there is no history to reconstruct and the
change log says nothing about them, so their single all-time interval necessarily
takes its segment from `raw_clients`. That is unambiguous precisely because,
never having been reclassified, their original and current segment are the same
value.

### 3.2 Reconciliation warns rather than fails

Two disagreements are logged, not raised: `raw_clients.segment` differing from a
client's earliest `from_segment`, and a break in the chain where one row's
`from_segment` does not equal the previous row's `to_segment`.

Both apply only to the 5 clients that appear in the change log — for the other 8
there is no `from_segment` to compare against, and `raw_clients` is the dimension's
source rather than a cross-check on it.

They warn because neither can corrupt the output: for a reclassified client the
dimension never reads `raw_clients.segment`, so a mismatch is a statement about
the reference table. Failing the load would block a correct result over an
upstream inconsistency the pipeline has already routed around. The count is
surfaced so it can be raised with the source system owner.

### 3.3 Multiple changes, and the one case that is refused

Interval closing uses `lead(effective_date) OVER (PARTITION BY client_id ORDER BY
effective_date)`, so a client reclassified twice produces a correct chain without
modification, even though the sample has at most one change each.

That generality has a boundary, and it is **enforced rather than assumed**. Two
changes for one client on the same date cannot be ordered — the change log has no
sequence column. Left alone, the tie resolves arbitrarily, one interval collapses
to zero length and is dropped, and a segment silently vanishes from the client's
history *while the interval-integrity check still passes*, because what remains is
contiguous, non-overlapping and single-current.

There is no correct answer to guess at, so `assert_change_log_orderable` declares
the ordering assumption as a contract and fails the load when it is violated.

### 3.4 Half-open intervals

`effective_start_date <= trade_date < effective_end_date`. This is the only
convention under which a trade falling exactly on a change date cannot match two
dimension rows; closed-closed would double-join on the boundary and silently
duplicate fact rows.

Note this is deliberately *not* SQL `BETWEEN`, which is inclusive on both sides
and would reintroduce the double match.

### 3.5 Open boundaries

Sentinel dates rather than NULLs — `1900-01-01` and `9999-12-31` — so the
point-in-time join stays a simple range predicate with no three-valued logic. The
earliest interval extends to the floor sentinel, so it covers every trade date in
the source domain and the brief's silence on trades predating known history needs
no special handling.

An `is_current` flag is included for consumers wanting present-day segmentation
without a date predicate. It is defined as "the last interval in the chain"; with
a future-dated change that differs from "the interval containing today", which is
a known limitation (§13).

---

## 4. Trade deduplication

**Observation.** `trade_id` is not unique; duplicates generally differ in
`amount`, always share a `status`, and never tie on `created_at`.

**Decision.** One row per `trade_id`, earliest `created_at`, deduplicated
**before** the status and date filters.

**Ordering.** The brief says both "keep the earliest version" and "include only
ACTIVE trades". These conflict when versions of one `trade_id` disagree on
status: filter first and a later `ACTIVE` row survives whose earliest version was
`CANCELLED`; deduplicate first and it is dropped. "Keep the earliest version" is
read as a property of the raw record set rather than of the filtered subset, so
deduplication runs first.

The two orderings are equivalent on this data, so the choice is not forced by it.
It is a reading of the requirement, not an inference about what a
`CANCELLED`-then-`ACTIVE` sequence means — that is a question for the business,
and the two answers carry opposite risks: dropping a genuine reinstatement
understates exposure, while keeping a stale resubmission overstates it. Recorded
in §13 as an open question. A fixture pins the chosen behaviour so it cannot
change silently.

**Tiebreak.** Idempotency requires a total ordering and `created_at` is not
guaranteed unique. The ordering is content-based rather than positional —
physical row order is not a guarantee the database owes us across runs — and
covers **every** column that can distinguish two versions: `amount`,
`agreed_rate`, `status`, `base_currency`, `quote_currency`, `client_id`,
`trade_date`. Rows tying on all of them are identical, so which survives is
immaterial. That is what makes it a total order rather than a longer tiebreak. It
never fires on this data.

**`created_at` anomalies.** Five rows have `created_at` after `trade_date`. Not
treated specially: `created_at` orders versions within a `trade_id`, it is not
used as a business date.

---

## 5. Amount normalisation

**Decision.** Where `amount_in_thousands` is true, multiply by 1,000. This runs
**before** deduplication and before the positive-amount filter.

**Why the ordering.** Normalising before deduplication means two versions
recorded under different conventions — `5000/false` and `5/true` — are seen as
carrying the same value rather than as a real discrepancy. Normalising before the
filter means the zero-or-negative test applies to real values.

A NULL flag defaults to `false`, so an unflagged row is never silently inflated
by three orders of magnitude. The opposite risk is real and unexamined — a large
trade with a missing flag is understated 1000× instead — but the column has no
NULLs, so neither default is exercised. Recorded in §13.

---

## 6. Currency normalisation

**Decision.** Canonicalise as `upper(trim(...))` and check the result matches
`^[A-Z]{3}$`, applied identically to `raw_trades` and `raw_fx_rates`.

**Why.** Applying the exclusion before normalisation would discard valid rows
such as `usd`. Normalising the trades table but not the rate feed would make the
FX join miss silently — the brief mentions normalisation only under trades, but
the join cannot be correct unless both sides are treated the same way. `status`
gets the same treatment for the same reason.

**What the check is and is not.** `^[A-Z]{3}$` is a **format** check, not
validation against the ISO 4217 register. It catches a NULL or a malformed value;
it cannot catch a well-formed code that is not an ISO one. Validating properly
would need an allowed-code reference set, which the assignment does not supply.
That gap is exactly what §6.1 addresses.

### 6.1 `NIS` → `ILS`

**Observation.** Ten `ACTIVE` trades carry `base_currency = 'NIS'`. `NIS` is the
conventional shorthand for the New Israeli Sheqel; its ISO 4217 code is `ILS`,
and the supplied rate feed publishes that currency only as `ILS`.

**Decision.** A source-specific alias map in `config.py` rewrites `NIS` to `ILS`
during canonicalisation, applied to both trades and the rate feed. The number of
rewritten values is logged. It is an explicit whitelist of one entry, configured
deliberately — not a generic inference rule.

**Why.** `NIS` is three uppercase letters, so it passes the format check and is
never excluded; it simply matches no rate. The trades survive every filter, are
counted in `trade_count` and `total_amount_base`, and reach the fact table looking
healthy with `fx_rate_source = 'not_found'` and a NULL USD exposure. Nothing
errors and no count looks wrong. "Codes normalised to standard ISO 4217 format"
is not satisfied by case folding alone, and a code that is not an ISO code is
what that instruction is about.

**Corroboration.** The identification rests on the currency convention, not on
the data. The data agrees: taking the ratio of `agreed_rate` to the market
mid-rate on the same date, `NIS` sits at 1.011 (sd 0.053) against the `ILS` feed,
while every other currency ranges from 0.61 to 120 with an order of magnitude
more dispersion. Individually the ten deviate by 0.08% to 7.09%, which proves
little alone — it is the comparison that is decisive.

*(A consequence worth noting: `agreed_rate` is uncorrelated with the market
across the rest of the dataset, so `weighted_avg_agreed_rate` is computed
correctly on economically synthetic input.)*

**The boundary.** `SGD` also resolves to `not_found` — 73 trades — and is
deliberately left alone. `NIS` is an alias for a currency the feed already
carries, so resolving it recovers a rate that exists; `SGD` is a real ISO code
for which no market data was supplied, so there is nothing to recover and a NULL
is the honest answer. Rewriting an alias and inventing a rate are different acts.
Any further non-ISO code resolves to `not_found` until somebody adds it
deliberately.

---

## 7. Exclusions

Applied in this fixed order, with a count logged at each step:

| # | Step | Excluded |
|---|---|---|
| 1 | Non-`ACTIVE` status | 133 |
| 2 | `trade_date` missing or after 2026-06-01 | 5 |
| 3 | `client_id` missing or unresolvable | 5 |
| 4 | Currency missing or not `^[A-Z]{3}$` | 4 |
| 5 | Amount missing, zero or negative | 3 |

692 raw → 15 superseded versions removed → 677 → 150 excluded → **527 retained**.

**Sequential, not independent.** A row violating several rules is attributed to
the first it fails, so counts read as "removed at this step". 135 rows are
non-`ACTIVE` in the raw table; 2 had already gone as superseded duplicates,
leaving 133 — that gap is the §4 ordering, visible in the counts. The order is
fixed so the numbers reproduce.

**"Missing client information"** is read as covering both a NULL `client_id` and
one with no match in the reference data. Only the NULL case occurs here, but an
unresolvable reference is as much a gap as an absent one.

**`trade_date`** is typed `DATE`, so `<= 2026-06-01` needs no cast and cannot
silently exclude same-day trades after midnight.

---

## 8. FX rate resolution

### 8.1 Clean the feed before deduplicating it

Invalid rates — NULL, zero, negative, or undated — are removed **before** any
deduplication. This is load-bearing, not tidy. Both duplicate keys pair a valid
rate with an invalid one:

| Key | Rows |
|---|---|
| `ILS/USD` 2026-02-15 | `0.275537` and `0.0` |
| `EUR/USD` 2026-02-20 | `1.085145` and NULL |

Filter first and the good rate survives. Deduplicate first with an arbitrary pick
and you can keep the zero — and two real ILS trades fall on 2026-02-15, so the
consequence is two fact rows converting at a rate of nothing, indistinguishable
from genuine zero exposure.

After filtering, uniqueness on `(base, quote, date)` is asserted. No source
precedence rule is defined because post-filter none should be needed; if the
assertion fires, the right response is to define one deliberately rather than to
have guessed silently now. The check runs **before** the lookup is built, since
once built a duplicate has already been resolved by the ranking tiebreak.

Negative rates are excluded alongside zeros: the brief names only zero, but
inverting a negative rate would silently produce a negative converted amount.

### 8.2 Direction of inversion

The fact converts a **base** amount into USD, so for base currency X it needs
`X → USD`. Where no such row exists but the feed carries `USD → X`, the rate is
`1 / (USD → X)`. Inverting an existing `X → USD` row would give `USD → X`, which
converts the wrong way.

**`quote_currency` plays no part in resolution.** The lookup is keyed on
`(base_currency, trade_date)` alone, because the measure is the base amount
expressed in USD — not in the quote currency. Joining on the trade's own
`quote_currency` would be a natural misreading and a damaging one: a trade quoted
into anything but USD would fall to `not_found` despite the feed carrying what is
needed. On this data `quote_currency` is USD on every row, so the two readings
coincide and the distinction is invisible in the output — which is why it is
recorded rather than left to be inferred from the SQL.

### 8.3 What each branch actually does

| Base currency | Resolution | Fact rows | Trades |
|---|---|---|---|
| GBP, EUR, ILS, CHF, AUD, JPY | Direct `base → USD` | 376 | 387 |
| CAD | Inverse of `USD → CAD` | 65 | 67 |
| SGD | `not_found` — no feed coverage | 72 | 73 |
| USD | Parity, rate `1.0` | 0 | 0 — never a base currency |

`USD → CAD` is the only feed row with USD as base, so inversion can only serve
CAD — and every CAD trade takes it, since no `CAD → USD` row exists.

### 8.4 USD at parity

No `USD → USD` row exists, so a literal reading of the chain would send every USD
trade to `not_found` with a NULL amount — plainly wrong for an identity
conversion. USD-base trades get rate `1.0` labelled `direct`: the brief fixes the
three permitted values, and an identity conversion is more honestly direct than
not found. Applied before the lookup so it cannot be masked by a spurious feed
row.

No trade in this data has USD as its base, so the branch is defensive and covered
by a fixture rather than by real rows.

### 8.5 No temporal fallback

The feed has 156 distinct dates over a 156-day span — complete on a full
calendar. Carry-forward of the last known rate would therefore add unreachable
complexity and deviate from a specification defining only three outcomes. This is
the first decision to revisit if the feed ever moves to a trading calendar.

### 8.6 Unconverted rows

Where a rate resolves to `not_found`, both `fx_rate_used` and `total_amount_usd`
are NULL rather than zero. A zero is indistinguishable from genuine zero exposure
and would silently understate any downstream sum.

---

## 9. Weighted average agreed rate

```sql
sum(amount * agreed_rate) FILTER (WHERE agreed_rate IS NOT NULL)
  / nullif(sum(amount)    FILTER (WHERE agreed_rate IS NOT NULL), 0)
```

Amount-weighted, using the unit-normalised amount, within the
`(date, client, base_currency, quote_currency)` group.

`agreed_rate` has no NULLs, zeros or negatives, so the `FILTER` clauses are inert
here. They are written anyway because the plain form is wrong the moment a NULL
appears: the numerator would skip the NULL product while the denominator still
counted that row's amount, dragging the average toward zero in proportion to the
missing row's size. That failure is a plausible number rather than an error.

A NULL-rate row leaves both sides of the average but is **retained** in
`trade_count` and `total_amount_base` — the trade is real and its amount is known
even where its rate is not. A group where every rate is NULL yields NULL rather
than a division error, and the count is logged. Two fixtures pin this, since the
data cannot.

Amounts are already filtered to strictly positive, so the denominator cannot be
zero for a group that exists.

**Validation.** A zero or negative `agreed_rate` **raises**. This is the price the
business transacts on, so it warrants the rigour `amount` gets from an exclusion
rule and market rates get from zero/negative filtering; without it, the agreed
rate was the one quantity in the pipeline nothing validated. It raises rather
than excluding because the brief enumerates the exclusion rules and this is not
among them — dropping the trade would invent a rule, whereas failing the load
surfaces the defect for a decision. A NULL rate **warns** instead: it is the
designed-for case above, not an error.

**No plausibility band against the market rate.** It is the obvious next check
and is deliberately absent. In this dataset `agreed_rate` is uncorrelated with
the mid-rate on the same date — per-currency ratios span 0.01 to 299, and a
0.5×–2× band would reject 258 of 511 comparable trades. A tolerance rule needs a
source that genuinely prices against the market; asserting one here would encode
noise and fail every run.

---

## 10. Idempotency

Two consecutive runs on the real database produce identical SHA-256 digests over
both tables, taken under an explicit `ORDER BY`. The ordering matters:
`CREATE OR REPLACE TABLE` guarantees identical *contents*, not identical physical
row order, so hashing an unordered scan would test something the database does
not promise — it could fail on a correct run or pass by luck.

It rests on four properties:

- **Full replace, never append**, and the whole build runs in one transaction, so
  a run that fails partway leaves the previous contents rather than a half-written
  mixture (§11.1).
- **Deterministic deduplication** — a total ordering over the row's own values,
  with no dependence on physical position (§4).
- **Deterministic rate resolution** — the uniqueness assertion guarantees a single
  candidate per key.
- **No wall-clock or random input.** No `current_date`, `now()` or generated
  identifier participates in any output value; the `2026-06-01` cutoff is a
  literal.

---

## 11. Data quality

Six checks. Five **raise**; one **warns**. Each raising check has a test that
constructs the corruption it exists to catch and asserts that it fires — a check
never observed to fail is not known to work.

| Check | Model | Catches |
|---|---|---|
| Change log orderable | `dim_clients` | same-date changes that cannot be sequenced (§3.3) |
| Interval integrity | `dim_clients` | overlaps, gaps, zero-length intervals, multiple current rows |
| Rate feed uniqueness | `fx_to_usd` | duplicate keys surviving cleaning (§8.1) |
| Agreed rate validity | `fact_daily_exposure` | a zero or negative traded price (§9) |
| Grain uniqueness | `fact_daily_exposure` | the declared grain violated — usually a dimension fan-out |
| Trade reconciliation | `fact_daily_exposure` | rows lost *or* duplicated, in one check |
| Segment chain (warns) | `dim_clients` | reference table disagreeing with the change log (§3.2) |
| Missing agreed rate (warns) | `fact_daily_exposure` | trades with no rate, excluded from the average (§9) |

Plus conversion consistency: a `not_found` row with a USD amount, a resolved row
without one, or a non-positive rate.

**Why raise.** Every failure above is silent by construction. A fan-out produces
a well-formed table with inflated numbers and raises nothing; a dropped row just
makes a total smaller. A partially-correct analytical table is worse than an
absent one, because it will be trusted. The rule is: **raise when the output would
be wrong, warn when an input is untidy but the output is unaffected.**

### 11.1 Assertions decide; the transaction enforces

These are two mechanisms with different jobs, and separating them matters:

- The **assertions** decide whether output is acceptable.
- The **transaction** decides whether unacceptable output ever becomes visible.

Without the second, the first is only monitoring. `CREATE OR REPLACE TABLE`
auto-commits, so a failing assertion would abort the run *after* the target had
been replaced by the data it just rejected. Verified: injecting a bogus `0.5`
`EUR/USD` rate makes the uniqueness check fire and the process exit 1, and leaves
a fully written target in which that date's EUR exposure was converted at `0.5`
instead of `1.085415` — understated by about half, with no error visible to any
consumer.

DDL in DuckDB is transactional, so the whole build including its checks now runs
in one transaction and a rejected load rolls back to the last good state. The
rollback is logged explicitly, so an operator knows the data is stale rather than
wrong. Measured cost is nil — runs complete in 0.15–0.37s either way.

---

## 12. Implementation structure

One module per model, under `grain_pipeline/pipeline/`, with shared machinery in
`grain_pipeline/utils/`. Each model owns its own sources, staging, build and
quality checks behind a single `build(con, source_schema)`; `run.py` is three
calls in dependency order.

**Why a model is the unit.** A model is the thing with one reason to change.
Segmentation rules change: one file. A new currency alias: `config.py` alone. A
second fact: a new file and one line in `run.py`. Organising by processing stage
instead spreads each of those across several files and gives no file a single
owner.

The decomposition also makes two stated requirements satisfiable — "at least 3
meaningful unit tests covering actual business logic" and "log how many records
are excluded at each filtering step" are both impossible inside one monolithic
query.

**Dependencies are data, not imports.** `fact_daily_exposure` reads `stg_clients`
and `dim_clients` from the dimension, and `fx_to_usd` from the lookup. No model
imports another; each declares its reads and writes in its docstring and `run.py`
resolves the order. The import graph is acyclic and one-directional — `pipeline/`
imports from `utils/`, never the reverse — and any model can be rebuilt in
isolation against tables that already exist.

**`fx_to_usd` is an intermediate model**, not a dimension or a fact: not
published to the target, and a conformed lookup any model needing USD conversion
would join to. Folding it into the fact would make that fact the only model
owning two unrelated sources.

**Checks live with their models** because a check is part of building a table
correctly. This also fixed an ordering defect: the rate-feed assertion used to run
after `fx_to_usd` was built, by which point a duplicate key had already been
resolved by the ranking tiebreak.

**SQL rather than pandas.** Source and target are both DuckDB and the work is
set-based; round-tripping through dataframes would add conversion cost and type
drift for no benefit. `pandas` and `numpy` are in `requirements.txt` because the
brief permits them, but neither is imported.

**Testability.** Each transformation takes a connection and reads named tables.
The source is attached as `src`, and DuckDB resolves `src.raw_trades` identically
whether `src` is an attached database or a plain schema — so the tests build an
in-memory `src` schema and run the *production* SQL unmodified.

---

## 13. Known limitations and open questions

- **`is_current` means "last interval in the chain", not "the interval containing
  today".** These differ if a change is dated in the future. Not reachable here —
  the latest change is 2026-03-15 — but a consumer using the flag for present-day
  segmentation would get the wrong answer. Fixing it introduces a wall-clock
  dependency, which is why it was avoided.
- **No surrogate key on `dim_clients`**, and the fact stores no reference to the
  dimension *version* that produced each row. Adequate at this size; at scale it
  costs auditability (which version produced a historical fact?) and forces every
  future fact to repeat the range join.
- **`client_name` is carried in the fact.** Not required by the brief, and a
  client-level attribute reachable through the join already needed for `segment`.
  Denormalisation for query convenience, at the cost of a rename touching history.
- **`agreed_rate` is checked for sign, not for plausibility.** Zero and negative
  raise and NULL warns (§9), but nothing tests whether a rate is *reasonable*.
  The supplied data cannot support such a rule — its agreed rates are
  uncorrelated with the market — so this needs a source that prices against mid.
- **Nothing flags the share of unconverted exposure.** 72 of 513 fact rows (14%)
  carry no USD figure. It is counted and internally consistent, but an upstream
  outage pushing that to 60% would fire no check.
- **Deduplicate-before-filter is a reading, not a resolved rule.** The risk
  direction — dropping a genuine reinstatement versus keeping a stale
  resubmission — is a business question (§4).
- **A NULL `amount_in_thousands` defaults to `false`.** Never exercised, but the
  default silently understates rather than inflates; quarantining is probably
  better than either default (§5).
- **Excluded rows are counted, not quarantined**, per the stated requirement.
- **The dimension is rebuilt in full each run**, which is required for idempotency
  at this size; a production SCD2 would merge incrementally.
- **`trade_count` counts deduplicated trades**, not raw rows — stated because
  "number of trades" is ambiguous where the source contains duplicates.
