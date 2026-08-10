# PROGRESS.md

Working notes for the Grain home assignment. Not part of the submission — delete
this file before zipping.

---

## Status

The pipeline is **feature-complete and passing** against a synthetic source
database that reproduces every defect found while profiling the real one. It has
**not yet been run against the real `grain_raw.duckdb`**. That is the next step.

| Item | State |
|---|---|
| `pipeline.py` runs from repo root | Done |
| `dim_clients` (Type 2 SCD) | Done |
| `fact_daily_exposure` | Done |
| Per-step exclusion logging | Done |
| Idempotency | Verified — byte-identical SHA-256 over both tables across two runs |
| Unit tests | 20 passing |
| `DECISIONS.md` | Done, 13 sections |
| Run against real data | **Outstanding** |

---

## Verified facts about the source data

Established by direct query, not assumption. Each one drove a decision — see
`DECISIONS.md` for the reasoning.

**`raw_clients`**
- Duplicate identifiers for the same logical client: `c007`/`C007` (case),
  `C003`/`C003 ` (trailing blank, confirmed by byte-length inspection — not a
  zero-width or homoglyph character, so `trim` is sufficient).
- Duplicate pairs agree on `client_name` and `segment` — no survivorship conflict.
- Neither duplicated client appears in the change log.
- `segment` matches **`from_segment`**, so this is an **original-state** snapshot,
  not current state. See "Correction" below.

**`raw_client_segment_changes`**
- Exactly one row per `client_id`. No `valid_to` column.
- No same-day multiple changes.

**`raw_fx_rates`**
- Bases: AUD, EUR, CHF, JPY, ILS, GBP, USD. Quotes: CAD, USD only.
- One NULL `rate_date`; one NULL `mid_rate`.
- A duplicate `(base, quote, date)` key from the same source where one row is
  NULL and the other zero — both invalid.
- Rates published on weekends and holidays: **full calendar coverage**, so no
  carry-forward rule is needed.
- No `USD → USD` row.

**`raw_trades`**
- Duplicate `trade_id`s, generally differing in `amount`. `created_at` available
  for version ordering. Duplicates **always share a status**.
- `created_at` normally precedes `trade_date`; five rows invert that.
- `status`: PENDING, ACTIVE, CANCELLED.
- `base_currency` has NULLs and mixed case; `quote_currency` mixed case, no NULLs.
- A few NULL `client_id`; every non-NULL id resolves after canonicalisation.
- `amount_in_thousands` has no NULLs. No `agreed_rate = 0`.
- **CAD never appears as a trade base currency.**

---

## Correction applied (important)

An earlier reading had `raw_clients.segment` matching `to_segment`, implying a
current-state reference table. It actually matches **`from_segment`** — the table
holds each client's **original** classification.

**The SQL did not change.** The dimension is built from `from_segment` and
`to_segment` in the change log and never reads `raw_clients.segment` for a
reclassified client, so it was already correct under either reading. What changed
is the documentation, and the correction produced a stronger finding: because
`raw_clients` is an original-state snapshot, joining it directly onto trades would
stamp every trade with the client's *original* segment regardless of date — the
mirror image of the naive current-segment bug the assignment warns about, and
harder to spot because it fails in the less obvious direction.

Two tests now pin this (`test_original_state_reference_table_does_not_leak_into_the_dimension`,
`test_segment_chain_inconsistency_is_reported_not_fatal`).

---

## Notable design conclusions

**The `inverse` rate branch is unreachable on this data.** `USD → CAD` is the only
feed row with USD as base, so inversion can only ever serve CAD — and CAD is not a
trade base currency. Confirmed empirically: the synthetic run resolves 546 inverse
rates in `fx_to_usd`, of which **zero** reach the fact table. The branch is
implemented to specification and covered by a synthetic fixture rather than
trusted.

**Direction of inversion matters.** The fact converts base → USD, so inversion is
`X → USD = 1 / (USD → X)`. Inverting an existing `X → USD` row would give
`USD → X`, which converts the wrong way.

**USD trades need a special case.** With no `USD → USD` row, a literal reading of
the resolution chain flags every USD trade as `not_found` with a NULL converted
amount. They are assigned `1.0` / `direct`.

**Invalid rates must be filtered before deduplication.** The NULL/zero duplicate
pair means filter-first correctly drops the key entirely; dedupe-first could
retain the invalid row.

**Two requirements are really design constraints.** "3 meaningful unit tests" and
"log exclusions at each step" are both unsatisfiable by one monolithic query. The
module decomposition follows from them.

---

## Outstanding before submission

1. **Run against the real `grain_raw.duckdb`.** Everything so far is verified
   against a synthetic reproduction.
2. **Check `agreed_rate` for NULLs.** No zeros were found, but NULLs were never
   confirmed. If there are none, tighten `DECISIONS.md` §9 from a contingency to a
   confirmed observation.
3. **Check whether any `not_found` rows appear** in the real output. If none, the
   reachability table in §8.3 is confirmed as written; if some do, record which
   currency-dates and why.
4. **Confirm the deduplication count** matches what was seen during profiling.
5. **Confirm the segment reconciliation check passes** on real data (it warns
   rather than fails, so check the log rather than the exit code).
6. Delete `.venv`, `__pycache__`, `.pytest_cache`, `PROGRESS.md` and
   `source_data/` before zipping. Include `target/grain_analytics.duckdb`.

---

## Three things to be ready to defend

These are where the submission holds up or doesn't:

1. **The half-open interval convention.** `start <= date < end` is the only
   convention under which a trade on a change date cannot match two dimension
   rows. A closed-closed interval fans out the fact silently.
2. **Deduplicate before filtering.** "Keep the earliest version" reads as a
   property of the raw record set, not the filtered subset. Equivalent on this
   data since duplicates share a status — the point is that it was checked.
3. **USD at parity labelled `direct`.** Defensible either way; be ready to say why
   inventing a fourth enum value was rejected.
