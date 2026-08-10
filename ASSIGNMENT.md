# Grain — Data Engineer Home Assignment

*(Verbatim copy of the brief supplied by Grain, kept in the repo for reference.)*

## Background

Grain processes cross-currency trades on behalf of clients — businesses that send
and receive payments in foreign currencies. Each trade has a base currency (what
the client holds) and a quote currency (what they want), along with an agreed FX
rate and an amount.

DuckDB database with raw operational data:

| Table | Description |
|---|---|
| `raw_trades` | Individual FX trades submitted to the platform |
| `raw_fx_rates` | Mid-market FX rates sourced from a market data feed |
| `raw_clients` | Client reference data (names, segments) |
| `raw_client_segment_changes` | A log of changes to client segmentation over time |

The data is **raw** — it has not been cleaned or validated.

## Task

Build a Python pipeline (`pipeline.py`) that reads from
`source_data/grain_raw.duckdb` and writes to `target/grain_analytics.duckdb`.

### 1. `dim_clients`

Clients are occasionally reclassified into a different segment. Historical
analyses must reflect the segment a client *was in at the time* — the table must
support point-in-time lookups. Design the schema however best supports that;
document the choice in `DECISIONS.md`.

### 2. `fact_daily_exposure`

One row per `(date, client, base_currency, quote_currency)`. For each group:
number of trades, total amount in base currency, weighted-average agreed rate
(weighted by amount), total amount converted to USD, the FX rate used, and
whether the rate was obtained directly or via inversion of the inverse pair (or
flagged as unavailable). `segment` must reflect the client's segment **as it was
on that trade date**, joined from `dim_clients`.

### Business rules

- Include only **ACTIVE** trades.
- Include only trades with `trade_date <= 2026-06-01`.
- Trade amounts must be in the correct unit — check the raw data carefully.
- Currency codes normalised to standard ISO 4217 format.
- Exclude trades with missing client or currency information.
- Exclude trades with zero or negative amount.
- If the same trade appears more than once, keep the earliest version.
- For USD conversion, use the FX rate from `raw_fx_rates` on the trade date.
  Prefer a direct rate (base → USD); fall back to the inverse of a USD → base
  rate if no direct rate exists. Record whether the rate was `direct`, `inverse`,
  or `not_found`.
- Exclude FX rates that are zero, NULL, or have a missing date.
- **The pipeline must be idempotent.**
- Log how many records are excluded at each filtering step.

## Requirements

`python pipeline.py` from the repo root must read the source and write both
tables to the target.

### Tests

At least **3 meaningful unit tests** in `tests/test_pipeline.py` covering actual
business logic.

### DECISIONS.md

Document the choices made where the requirements were ambiguous.

## Submission

Run `python pipeline.py` one final time from a clean state, zip the project
directory (including `target/grain_analytics.duckdb`), and send it. Do **not**
include `source_data/grain_raw.duckdb`.
