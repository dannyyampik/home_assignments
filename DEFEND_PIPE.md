# DEFEND_PIPE.md

Interview preparation for the Grain pipeline. **Not part of the submission —
delete before zipping.**

This is written adversarially: it assumes the interviewer is looking for the
weakest point, not the strongest. Every challenge below was found by actually
attacking the code — mutating it, constructing edge cases, and re-deriving the
quantitative claims in `DECISIONS.md` against the real database. Where a
challenge lands, the honest answer is given rather than a defence.

**Read Part 11 first if you are short on time.** It splits into what was found
and fixed (volunteer it) and what is still live (concede it). Owning a weakness
before it is found is worth more than defending one after.

---

## The 90-second opener

If asked "walk me through what you built":

> It reads the raw DuckDB database and writes two tables: a Type 2 slowly
> changing dimension for clients, and a daily FX exposure fact at
> `(date, client, base_currency, quote_currency)`. Transformations are DuckDB
> SQL, organised as one module per model — the dimension, an intermediate rate
> lookup, and the fact — each owning its own staging, build and quality checks,
> over a shared utilities package. That split is driven by two of the
> requirements, since neither per-step exclusion counts nor meaningful unit tests
> are achievable inside one monolithic query.
>
> 692 raw trades reduce to 527 after deduplication and five counted exclusion
> steps, producing 513 fact rows. Six data quality checks run at the end; five
> roll the load back and one warns.
>
> The most interesting thing I found is a defect the brief's filters cannot
> catch. Ten trades use `NIS` for the Israeli shekel, which is the colloquial
> code — the ISO code is `ILS`, and that is what the rate feed publishes. `NIS`
> is three uppercase letters, so it passes an ISO shape check and is never
> excluded; it just silently fails to match a rate and lands in the fact table
> looking healthy with a null USD exposure. That is the failure mode I care most
> about: no error, no wrong-looking count, just a quietly missing number.

Then stop. Let them pick the thread.

---

## Numbers to have memorised

| | |
|---|---|
| Raw trades / distinct / duplicate ids | 692 / 677 / 15 (2.2%) |
| Excluded (sequential) | 133 status, 5 cutoff, 5 client, 4 currency, 3 amount = **150** |
| Clean trades → fact rows | 527 → **513** |
| Rate resolution | 376 direct, 65 inverse, **72 not_found** (all SGD) |
| `dim_clients` | 18 interval rows, 13 clients, 5 reclassified |
| Rate feed | 1,095 rows, 3 invalid, 1,092 retained; 156 dates over a 156-day span |
| Tests | 37 |
| Trade currencies | ILS 95, SGD 94, GBP 93, CHF 89, EUR 89, CAD 79, AUD 72, JPY 67, NIS 10, null 4 |

`quote_currency` is `USD` on all 692 rows. `USD` is never a base currency.

---

## Part 1 — Design and architecture

**Q: Walk me through how the code is organised.**

One module per model, plus a package of shared machinery:

```
grain_pipeline/pipeline/   dim_clients.py  fx_to_usd.py  fact_daily_exposure.py  run.py
grain_pipeline/utils/      config.py  logging_setup.py  sql.py  filters.py  quality.py
```

Each model owns everything needed to produce its table — its own sources, its
own staging, its own build, its own quality checks — behind a single
`build(con, source_schema)`. `run.py` is three calls in dependency order.

**Q: Why is a model the unit rather than a processing stage?**

Because a model is the thing that has one reason to change. Segmentation rules
change: one file. A new currency alias: `config.py` and nothing else. A second
fact: a new file and one line in `run.py`. Organising by stage — a staging
module, a cleaning module, a dimension module — spreads each of those changes
across several files and leaves no file with a single owner.

**Q: Isn't this over-engineered for a 692-row dataset?**

The decomposition is not decoration; two of the requirements demand it. "At least
3 meaningful unit tests covering actual business logic" and "log how many records
are excluded at each filtering step" are both impossible inside one monolithic
query — nothing to test in isolation, no intermediate count to observe.

What I would concede is that at three models this is close to the minimum
structure that separates business logic from machinery. The point of the layout
is that it is the shape that *grows*: at twenty models it takes `dimensions/`,
`facts/` and `intermediate/` subdirectories under `pipeline/` without any model
changing.

**Q: How do the models depend on each other?**

Through **data**, not imports. `fact_daily_exposure` reads the `stg_clients` and
`dim_clients` tables the dimension produced, and `fx_to_usd` from the lookup. No
model imports another; each declares its reads and writes in its docstring and
`run.py` resolves the order.

That matters for two reasons. It keeps the import graph acyclic and
one-directional — `pipeline/` imports from `utils/`, never the reverse — and it
means any model can be rebuilt in isolation against tables that already exist,
which is the same contract a warehouse gives you between models.

**Q: Why does `fx_to_usd` get its own module? It's not a dim or a fact.**

It is an intermediate model — a conformed lookup, not published to the target,
that any model needing a USD conversion joins to. It earns a module for the same
reasons a dimension does: its own source, its own cleaning rules, its own quality
check, one reason to change. Folding it into the fact would make that fact the
only model owning two unrelated sources and would bury a reusable lookup inside a
single consumer.

**Q: Why do the quality checks live inside the models rather than in one place?**

A check is part of building a table correctly, not a separate concern bolted on
afterwards. Keeping them together means the guard and the thing it guards are
read as one unit and change together.

It also fixed a real ordering defect. The rate-feed uniqueness assertion used to
run *after* `fx_to_usd` was built — by which point a duplicate key had already
been resolved by the `QUALIFY` tiebreak, so the check was reporting on a decision
silently already taken. It now runs between staging and the lookup build, so it
fires before anything acts on the ambiguity. `utils/quality.py` keeps only the
framework: `DataQualityError` and the `require` / `warn` / `passed` helpers.

**Q: What is in `utils` and how do you decide what goes there?**

Anything every model reuses and nothing specific to any of them: configuration,
logging, SQL fragments, the counted-filter chain, the check framework. The test
is whether it mentions a business concept. `canonical_currency()` is in `utils`
because normalisation is a rule about text; the `NIS → ILS` mapping it applies is
in `config.py` because that is a fact about currencies.

Keeping it separate is what makes the models comparable — two models that
normalise an identifier call the same fragment rather than each spelling out
`upper(nullif(trim(...), ''))`, so a change to the rule happens once.

**Q: Why `logging_setup.py` as its own module?**

Every model calls `get_logger()`. Putting it in `run.py` — which imports every
model — would be a circular import that fails at load time. It is a leaf because
the dependency graph requires one there.

**Q: Why f-string SQL rather than parameter binding?**

Parameter binding does not work for identifiers or structural SQL, which is what
is being interpolated — a schema name, a `CASE` built from the alias map, a date
literal, a regex. Every interpolated value comes from a module-level constant in
`config.py`; none comes from source data or user input. It is not an injection
surface. But expect the question.

**Q: You restructured this. How do you know you didn't break anything?**

Both output tables hash identically before and after, under an explicit
`ORDER BY` — `dim_clients` at 18 rows and `fact_daily_exposure` at 513, same
SHA-256 — and every logged exclusion count is unchanged. Then the whole mutation
battery was re-run against the new layout: 17 deliberate breakages, all caught.
A refactor that cannot be shown to be behaviour-preserving is a rewrite.

---

## Part 2 — `dim_clients` and the SCD2

**Q: Why half-open intervals?**

It is the only convention under which a trade falling exactly on a change date
cannot match two dimension rows. Closed-closed double-joins on the boundary and
silently duplicates every affected fact row. This is demonstrable in the output:
client C004 was reclassified on 2026-02-10 and has a trade on that exact date; it
carries `Enterprise`, and there is exactly one row.

**Q: Why sentinel dates rather than NULLs?**

The point-in-time join stays a plain range predicate with no three-valued logic.
A `NULL` end date forces `COALESCE` or `OR effective_end_date IS NULL` in every
consumer's query, and someone eventually forgets.

**Counter you must answer:** `9999-12-31` is a magic value that leaks — date
arithmetic on a current row yields an ~8,000-year interval, and it can sort
oddly in partitioned storage. The stronger position is: sentinels *in storage*
are a deliberate trade for join simplicity, and the alternative (NULL in storage,
`COALESCE` at query time) is equally defensible. Do not pretend there is only one
right answer.

**Q: You have both sentinels and an `is_current` flag. Isn't that redundant?**

Partly, yes — and worse than redundant. See Part 11, B2: `is_current` does
not mean what a consumer would assume.

**Q: Why is there no surrogate key on `dim_clients`?**

This is a real gap and worth conceding cleanly. There is no surrogate key, and
the fact stores no foreign key to a specific dimension *version* — `fact_daily_exposure.py`
re-derives `segment` through the range join at build time and discards the join
key. Consequences:

- **No auditability.** If the SCD2 build is later corrected, there is no way to
  trace which dimension version produced a historical fact row. For a fintech
  that may need to reproduce what it reported on a given date, that matters.
- **No conformed-dimension reuse.** Every future fact needing client attributes
  reimplements the same range join instead of a cheap equi-join.

What to say: at two tables and 13 clients the range join is correct and cheap
(the `client_id` equality lets DuckDB hash-join then range-filter). If this had
to back five more facts, I would add a surrogate key — a hash of
`(client_id, effective_start_date)` — persist it on the fact, and turn every
downstream lookup into an equi-join.

**Q: Why build history forward from the change log rather than backward from `raw_clients`?**

Because `raw_clients.segment` matches `from_segment`, not `to_segment` — the
reference table holds each client's *original* classification. Joining it onto
trades directly would stamp every trade with the original segment regardless of
date: the mirror image of the naive current-segment bug, and harder to spot
because it fails in the less obvious direction.

The dimension is built from the change log alone, which is self-describing, so it
is correct under *either* reading of what `raw_clients.segment` means. That is
the property worth having when a source column's semantics are asserted rather
than documented.

---

## Part 3 — Deduplication and filtering

**Q: Why deduplicate before filtering?**

"Keep the earliest version" reads as a property of the raw record set, not of the
filtered subset. The orderings only diverge when a `trade_id` appears with
different statuses across versions — verified not to happen here, since all 15
duplicate pairs share a status.

**The counter-argument you must be ready for, because it is strong:** for a
hedging desk, dropping a legitimately reinstated trade *understates* exposure and
leaves real risk unhedged, whereas retaining a stale resubmission *overstates* it
and gets caught by downstream reconciliation. You picked the "when in doubt,
drop" direction with no data forcing the choice.

The honest answer: the ordering was chosen on record-set semantics, not on risk
direction, and the risk asymmetry is a fair point. If this were production I
would want the business to decide, and I would surface a `CANCELLED`-then-`ACTIVE`
sequence as a quarantined exception rather than silently resolving it either way.
Do not pretend you reasoned about hedging risk when you reasoned about semantics.

**Q: Your tiebreak claims to be a total order. Is it?**

Not quite, and the claim is slightly overstated. `ORDER BY created_at, amount,
agreed_rate, status, base_currency` omits `trade_date`, `client_id` and
`quote_currency`. Two versions tying on all five ordered columns but differing in
`trade_date` would still resolve arbitrarily. Inert here — no duplicate pair even
ties on `created_at`, so the tiebreak never fires — but say "deterministic on
these columns" rather than "a total order."

**Q: Why is `rowid` not the tiebreak?**

Because physical row order is not a guarantee the database owes us across runs,
and idempotency depends on the ordering being stable. Content-based ordering is
stable by construction. (`DECISIONS.md` §4 originally said `rowid`; that was a
documentation error, since corrected.)

**Q: Why are the exclusion counts sequential rather than independent?**

Because filters are applied in sequence, so a row violating several rules is
attributed to the first it fails. They read as "removed at this step", not "total
rows violating this rule". The order is fixed so the numbers reproduce.

Be ready to reconcile the one that looks off: 135 rows are non-`ACTIVE` in the
raw table (79 `CANCELLED`, 56 `PENDING`) but the step reports 133, because 2 had
already been removed as superseded duplicate versions. That gap *is* the
dedupe-before-filter ordering, visible in the counts.

---

## Part 4 — FX rate resolution

**Q: Why is the inversion `1 / (USD → X)` rather than inverting the direct rate?**

The fact converts a base amount into USD, so for base X it needs `X → USD`.
Inverting an existing `X → USD` row yields `USD → X`, which converts the wrong
way. And a currency with a direct rate never reaches the inverse branch anyway.

On this data the branch is live, not theoretical: `USD → CAD` is the only feed row
with USD as base, CAD is a trade base currency with 79 raw rows, and no
`CAD → USD` exists — so all 65 CAD fact rows take it.

**Q: Why are USD trades assigned 1.0 and labelled `direct`?**

No `USD → USD` row exists, so a literal reading of the chain would flag every USD
trade `not_found` with a null amount — plainly wrong for an identity conversion.
`direct` over a fourth enum value because the brief fixes the three permitted
values.

**Concede immediately:** no trade in this data has USD as its base currency, so
this branch never fires. It is defensive handling of a case the schema permits,
covered by a fixture rather than real rows. Do not oversell it as load-bearing.

**Q: Why does rate resolution ignore `quote_currency`?**

The lookup is keyed on `(base_currency, trade_date)` because the measure is the
base amount expressed in USD. Joining on the trade's own `quote_currency` would
be a natural misreading and a damaging one — a trade quoted into anything but USD
would fall to `not_found` despite the feed carrying what is needed.

**The sharp follow-up:** the declared grain includes `quote_currency` as though
it varies, but the conversion model has no notion of the quote leg at all. What
does `total_amount_usd` mean for a trade quoted into GBP? Honest answer: on this
data nothing breaks, because `quote_currency` is USD on every row. If it varied,
`total_amount_usd` would still be a correct base-to-USD conversion, but the
column name would be doing the work of hiding that the quote leg is unmodelled —
and I would want a second measure for the quote-side amount before calling the
table complete.

**Q: Why filter invalid rates before deduplicating the feed?**

Because the two duplicate keys each pair a **valid** rate with an invalid one:
`ILS/USD` on 2026-02-15 carries `0.275537` and `0.0`; `EUR/USD` on 2026-02-20
carries `1.085145` and `NULL`. Filter first and the good rate survives.
Deduplicate first with an arbitrary pick and you can keep the zero — and two real
ILS trades fall on 2026-02-15, so the consequence is two fact rows converting at
a rate of nothing, indistinguishable from genuine zero exposure.

**Q: Why no carry-forward of the last known rate?**

The feed has 156 distinct dates over a 156-day span — genuinely complete on a
full calendar, weekends included. Carry-forward is standard where a feed follows
a trading calendar and trades land on non-publishing days; that condition does
not hold. It is the first decision I would revisit if the feed moved to a trading
calendar.

---

## Part 5 — The fact table

**Q: Why is `client_name` in the fact table?**

This one is a fair hit. The brief lists the required fact columns and
`client_name` is not among them — only `segment` is required to be joined from
`dim_clients`. Carrying `client_name` denormalises a client-level attribute into
513 rows for no benefit, since any consumer needing `segment` is already joining
the dimension. A client rename now means touching a fact table that should be
immutable history.

The defensible framing: it was included for query convenience, and the cost is
real but small at this size. If challenged, agree — this is the column to drop.
Do not defend it as a design choice.

**Q: Is `max(fx_rate_used)` safe?**

Yes, and verified. The rate is looked up only on `(base_currency, trade_date)`,
both grain columns, and `fx_to_usd` is deduplicated to one row per
`(currency, rate_date)` — so the value is constant within a group. Zero groups in
the real output have more than one distinct `fx_rate_used` or `fx_rate_source`.

**Q: Why is `total_amount_usd` NULL rather than 0 when no rate resolves?**

A zero is indistinguishable from genuine zero exposure and would silently
understate any downstream `sum()`. NULL correctly means "not calculable."
`assert_conversions_consistent` enforces the implication in both directions.

**Q: Does your weighted average look sane?**

The arithmetic is right; the inputs are not economically meaningful. Across the
dataset `agreed_rate` is essentially uncorrelated with the market rate — ratio of
agreed to mid ranges from 0.61 (GBP) to 120 (JPY) with enormous dispersion. Those
are synthetic values. The one exception is the NIS block at 1.011 ± 0.053, which
is what identifies it. Have this ready: it looks like a trap and it is actually
your strongest evidence.

---

## Part 6 — Data quality

**Q: Why do five checks raise and one only warn?**

The five guard failure modes that are silent by construction — a fan-out produces
a well-formed table with inflated numbers and raises nothing. A partially-correct
analytical table is worse than an absent one, because it will be trusted.

`reconcile_segment_chain` warns because it *cannot* corrupt the output: the
dimension never reads `raw_clients.segment` for a reclassified client, so a
mismatch is a statement about the reference table. Failing the load would block a
correct result over an upstream inconsistency the pipeline already routes around.
The rule is: raise when the output is wrong, warn when the input is untidy.

**Q: What was the most important check you were missing?**

`agreed_rate` — and it is now there. It had no validation of any kind while
`amount` had an exclusion rule and market rates had zero/negative/NULL filtering
plus a uniqueness assertion. For a company whose product is FX hedging, a
fat-fingered agreed rate is plausibly the highest-value defect in the dataset.
Zero and negative now raise; NULL warns, because that is the designed-for case
rather than an error.

No plausibility band against the market rate, deliberately: in this data
`agreed_rate` is uncorrelated with the mid-rate — ratios span 0.01 to 299 — so
any useful band would reject over half the trades. See Part 11, A5.

**Q: Anything else missing?**

Yes — nothing flags that **14% of fact rows (72 of 513) carry no USD figure at
all**. It is logged as a count and internally consistency-checked, but if an
upstream outage pushed that to 60%, no check would fire. A threshold check on
unconverted exposure share is the obvious addition.

---

## Part 7 — Testing

**Q: Your tests run the production SQL?**

Yes, and this is the property worth defending. The source is attached as `src`,
and DuckDB resolves `src.raw_trades` identically whether `src` is an attached
database or a plain schema. The tests build an in-memory `src` schema and run the
*production* SQL unmodified — no reimplementation, no parallel query string.

**Q: Prove your tests catch anything. What happens if I break the logic?**

Mutation-tested. These all fail correctly when mutated: the half-open interval
(`<` → `<=`), dedup ordering (earliest → latest), rate inversion (`1/r` → `r`),
direct-beats-inverse preference, the NIS alias removal, the USD parity branch,
and stripping the dedup tiebreak columns.

**And they were validated by mutation** — see Part 11, A2 for the full list
and for the gaps that were found and closed that way.

---

## Part 8 — Idempotency

**Q: How do you know it is idempotent?**

Two consecutive runs on the real database produce identical SHA-256 digests over
both tables, taken under an explicit `ORDER BY`. The ordering matters:
`CREATE OR REPLACE TABLE` guarantees identical *contents*, not identical physical
row order, so hashing an unordered scan would test something the database does
not promise — it could fail on a correct run or pass by luck.

It rests on full replace rather than append, deterministic deduplication,
deterministic rate resolution guaranteed by the uniqueness assertion, and no
wall-clock or random input anywhere. The `2026-06-01` cutoff is a literal.

**Q: Is `any_value()` in the client collapse a threat to that?**

Potentially, and it is undisclosed in the document. `build_stg_clients` uses
`any_value(client_name)` / `any_value(segment)`. That is safe *today* because the
duplicate pairs agree — but that is a fact about this data, not a schema
guarantee, and `any_value` is not contractually stable across query plans. If a
future refresh introduced a disagreeing pair, idempotency could break. No test
exercises a disagreeing pair. The fix is a deterministic survivorship rule
(`min()`, or ordering by a recency column) rather than `any_value`.

---

## Part 9 — The data findings

**Q: Walk me through the NIS finding.**

Ten `ACTIVE` trades, contiguous ids `T00621`–`T00630`, base currency `NIS`. That
is the colloquial code for the Israeli shekel; ISO 4217 is `ILS` and the feed
publishes only `ILS`. `NIS` is three uppercase letters so it passes an ISO shape
check — it is never excluded, it just matches no rate and lands in the fact
looking healthy with a null exposure.

**Present the evidence comparatively, not absolutely.** Individually the ten
agreed rates deviate from the ILS mid by 0.08% to 7.09%, which proves little on
its own. What identifies them is that no other currency behaves this way: NIS
sits at a ratio of 1.011 ± 0.053 to the market rate, while every other currency
ranges from 0.61 to 120 with dispersion an order of magnitude larger. Those ten
rows were generated from real ILS rates; everything else is noise.

**Q: Why did you not do the same for SGD?**

Because there is nothing to recover. SGD appears nowhere in the feed, as base or
quote. `NIS` is a documented alias for a currency the feed already carries;
resolving it recovers a rate that exists. Inventing an SGD rate would be a
different act entirely. The alias map is an explicit whitelist of one, so the
line between those two stays visible.

**Q: What else did you find?**

- **CAD** is a trade base currency (79 raw rows) with no `CAD → USD` feed row —
  so the inverse branch is exercised on real data, 65 fact rows.
- **Duplicate rate keys pair a valid rate with an invalid one**, which is what
  makes filter-before-dedupe load-bearing.
- **Client identifiers vary by case and whitespace** (`c007`/`C007`,
  `C003`/`C003 `) — 15 raw rows collapse to 13 clients. Uncorrected, the
  dimension join fans out.
- **`raw_clients.segment` matches `from_segment`**, making it an original-state
  snapshot rather than a current-state one.

---

## Part 10 — Scale and production

**Q: Does this scale?**

Parts of it, and be precise about which. What is already right: the work is
entirely set-based and pushed into the engine, with no row-by-row Python and no
dataframe materialisation. Most of this SQL — window functions, `QUALIFY`,
`FILTER`, range joins — runs on Snowflake nearly unchanged.

What genuinely does not scale, in the order it breaks:

1. **Full rebuild every run.** Both outputs are `CREATE OR REPLACE TABLE` and the
   fact reprocesses all history each execution. That is O(all history) per run
   and it is the first thing to fall over. You would need an incremental fact
   keyed on a watermark and merge-based SCD2.
2. **Everything is a temp table in one connection.** One process, one machine, no
   resume-from-failed-step.
3. **Orchestration is a hardcoded function body.** Ten calls in a fixed line, no
   DAG, no selective rebuild. Fine at ten steps, untenable at fifty.
4. **No backfill parameterisation.** The cutoff is a literal — though it is
   centralised in `config.py`, so parameterising is small.

**Q: How would you productionise it?**

Answer in Snowflake and Azure Data Factory terms — that is both honest and your
actual depth. Incremental models, merge-based SCD2, warehouse-side compute,
quarantine tables for rejected rows instead of counts. Do not reach for AWS or
streaming vocabulary you would then have to defend; say plainly that your
background is batch and Snowpipe rather than log-based CDC if it comes up.

**Q: Isn't it a problem that you built for this scope rather than for scale?**

No, and push back gently. Building an orchestrator or an incremental merge
framework for 692 rows would be the worse error. The defensible line is: I built
for the stated scope, the expensive parts are already set-based, and I can tell
you precisely what changes at 100× and at 10,000×.

---

## Part 11 — Own these before they are found

Two groups, and they play differently in a conversation.

**Group A** is where you found a real defect and closed it. Volunteer these
unprompted — they are the strongest evidence you audit your own work, and each
one has a reproduction and a fix behind it.

**Group B** is still live. Concede these before the interviewer gets there; a
weakness you name yourself costs nothing, and the same weakness found for you
costs the room's confidence.

---

### Group A — found and fixed

**A1. A failed quality check used not to protect the target database.**

The strongest thing to volunteer. Tell it as a story with a fix at the end.

`CREATE OR REPLACE TABLE` is auto-committed and the checks ran *last*, so the
original design detected corruption **after** the target had already been
overwritten with the data it was about to reject. Reproduced by injecting a bogus
`EUR/USD` rate of `0.5`: the uniqueness assertion fired and the process exited 1,
but the persisted target had 513 rows in which that date's EUR exposure was
converted at `0.5` rather than the genuine `1.085415` — understated by about
half, with no error visible to any downstream consumer. A nightly run would have
replaced the last-known-good database with a subtly wrong one.

The checks detected and reported, but did not protect. That is a monitoring
feature, not a control.

**The fix:** the whole build, including its checks, now runs inside one explicit
transaction. DuckDB has transactional DDL, so a `DataQualityError` rolls the
build back and the previous load survives intact. Verified: after the failed run
the target still held `1.085415` and all 513 rows, and the rollback is logged so
an operator knows the data is stale rather than wrong. Cost measured at nil —
runs still complete in 0.15–0.37s. `test_a_failed_check_leaves_the_previous_target_intact`
pins it.

At scale you would express the same property as a blue/green swap or a staged
publish rather than one long transaction.

**A2. The quality checks were themselves largely unverified.**

The second half of the same story. Originally only the rate-feed check had a test
that constructed bad input and asserted `DataQualityError`; gutting
`assert_grain_unique` so it could never raise left all 22 tests green. The rest
were exercised only on their happy path.

Ten tests were added and the suite was then **mutation-tested** to prove they
work. All of these now go red: gutting any of the raising assertions, removing
the weighted-average `FILTER` clauses, dropping the ISO format check, dropping
the client-resolution subquery, changing the cutoff from `<=` to `<`, removing
the build transaction, and reversing the dedup order.

If asked how you knew the tests were weak: **say you mutation-tested them.**
Breaking the code deliberately and checking something goes red is the only way to
know a suite has teeth, and it beats quoting a coverage percentage.

**A3. Same-day segment changes silently dropped a segment.**

The best example in the project of "structurally valid, semantically wrong".

`lead(effective_date) OVER (PARTITION BY client_id ORDER BY effective_date)` has
nothing to order by when one client has two changes on the same date — the change
log carries no sequence column. The tie resolved arbitrarily, one interval
collapsed to zero length and was dropped by the positive-length guard, and a
segment vanished from that client's history. Meanwhile the interval-integrity
check **passed**, because what remained was still contiguous, non-overlapping and
single-current.

So: an arbitrary result, a lost segment, and every assertion green.

**The fix:** `assert_change_log_orderable` now declares the ordering assumption
as a contract and fails the load. There is no correct answer to guess at, so
guessing was the wrong response. The test does something deliberate — it builds
the corrupted dimension anyway and shows the neighbouring check still passes,
which is the whole point.

**A4. The dedup tiebreak was described as a total ordering and was not one.**

It covered `created_at`, `amount`, `agreed_rate`, `status`, `base_currency` —
five columns. Two versions tying on all five and differing in `trade_date` or
`client_id` still fell back to physical row order, which is precisely the
run-to-run instability the tiebreak exists to remove. It now covers every column
that can distinguish two versions; rows tying on all of them are identical, so
which survives is immaterial.

Worth adding: the first fix **was not actually pinned**. Truncating the ordering
back to five columns left every test green, because no fixture had rows tying
past `amount`. A fixture was built for exactly that case, and the truncation now
fails it.

**A5. `agreed_rate` was the one quantity nothing validated.**

Worth volunteering because the *asymmetry* is the interesting part, not the gap
itself. `amount` had an exclusion rule; market rates had zero, negative and NULL
filtering plus a uniqueness assertion; the **agreed** rate — the price the
business actually transacts on, at an FX hedging company — had nothing. Careful
defensive work had gone into a USD parity branch that never fires, and none into
the number most likely to matter.

**The fix:** a zero or negative agreed rate now raises; a NULL warns. The split
matters and shows you thought about it — a NULL is the designed-for case from
`DECISIONS.md` §9 (the trade is real, its amount is known, it just leaves the
weighted average), whereas a zero is a meaningless price that would flow straight
into that average and produce a plausible number over nonsense. It raises rather
than excluding because the brief enumerates the exclusion rules and this is not
among them — dropping the trade would invent a rule; failing the load surfaces it
for a decision.

**The part worth having ready: why there is no plausibility band.** It is the
obvious next check, and I deliberately did not add it. In this data `agreed_rate`
is uncorrelated with the market mid-rate on the same date — per-currency ratios
span 0.01 to 299, and a 0.5×–2× band rejects 258 of 511 comparable trades. Any
band tight enough to be useful would fail every run. A tolerance rule needs a
source that genuinely prices against mid; asserting one here would encode noise.

That is the difference between adding a check and adding a *correct* check.

---

### Group B — still live

**B1. Nothing flags the share of unconverted exposure.**

72 of 513 fact rows — **14%** — carry no USD figure, all SGD. It is counted, and
`assert_conversions_consistent` checks it is internally coherent, but nothing
would fire if an upstream outage pushed that to 60%. For a daily exposure report
that is a meaningful blind spot. A threshold check on the unconverted share is
the obvious addition.

**B2. `is_current` does not mean "the segment held today."**

It is defined as `effective_end_date = 9999-12-31`, which means "the last interval
in the chain". With a future-dated change those differ:

```
C001 | SME        | 1900-01-01 | 2027-01-01 | is_current = False
C001 | Enterprise | 2027-01-01 | 9999-12-31 | is_current = True
```

The client is SME today; the flag says Enterprise. Not reachable here (latest
change is 2026-03-15) but a one-line construction, and future-dated
reclassifications are normal in production. Fixing it means comparing against
`current_date`, which introduces a wall-clock dependency — say that too, because
avoiding wall-clock input is what the idempotency argument rests on.

**B3. `trade_id` is trimmed but never uppercased.**

`client_id`, `status` and both currency columns get `upper(trim(...))`;
`trade_id` gets only `trim()`. Inert today — every id is already uppercase. But
`DECISIONS.md` §2 argues client identifiers must be canonicalised "because the
defect exists in the source and the pipeline should not depend on which variants
the trade feed happens to use today", and the source demonstrably has case
defects in `client_id` and currencies. The same argument applies to `trade_id`
and was not applied. A case variant there would silently defeat deduplication.

**B4. `amount_in_thousands` defaults to `FALSE`, which understates.**

`DECISIONS.md` §5 frames this as purely defensive — an unflagged row is never
inflated 1000×. The flip side: a genuinely large trade with a missing flag is
silently *understated* 1000×, and for a hedging business understated exposure is
unhedged risk. It is a business-risk trade-off, not an obvious choice. A NULL
flag probably belongs in a quarantine rather than defaulting either way. Never
exercised — the column has no NULLs.

**B5. Schema choices a reviewer may push on.** Covered in detail in Parts 2 and
5; know that they are open rather than settled:

- **No surrogate key on `dim_clients`**, and the fact stores no reference to the
  dimension *version* that produced each row — so a historical fact cannot be
  traced back to the dimension row that made it.
- **`client_name` sits in the fact.** Not required by the brief, and reachable
  through the join already needed for `segment`. If challenged, agree — this is
  the column to drop.

**B6. Documentation volume.** `README.md` and `DECISIONS.md` overlap in places
(idempotency especially). Deliberate — they answer *what* and *why* for different
readers — but if challenged on volume, agree the two could be more sharply
separated rather than defending the page count.

---

## Part 12 — Questions to ask them

Good questions here signal seniority more than any answer does.

1. When a quality check fails on a nightly load, what should happen — fail the
   whole load, quarantine and continue, or publish with a flag? Who gets paged?
2. Is there a defined source precedence for the FX feed when two providers
   disagree on the same pair and date? I deliberately did not invent one.
3. How are non-ISO currency codes like `NIS` handled today — normalised at
   ingestion, or does every downstream consumer deal with it?
4. Is the segment change log append-only and immutable, or can rows be corrected
   retroactively? That determines whether the dimension can be rebuilt or needs
   versioning.
5. For exposure reporting, is a trade with no available rate excluded, carried at
   the last known rate, or surfaced as a gap? That is a business decision my
   `not_found` handling is currently guessing at.
6. What is the real cardinality — trades per day, clients, currency pairs? It
   changes whether incremental loading is worth the complexity.

---

## Closing note to self

The strongest move in the whole conversation is Part 11, A1. Volunteering
that a failed check leaves the target overwritten — and having reproduced it,
measured the damage, and verified the transaction fix — demonstrates exactly the
ownership the role asks for. Leading with a weakness you found and fixed beats
defending a design that has none.
