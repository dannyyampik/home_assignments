"""Reusable SQL fragments and small query helpers.

The canonicalisation fragments live here rather than in any one model because
every model that reads an identifier or a currency code needs the identical
treatment. Applying normalisation to the trades table but not the rate feed, for
instance, would cause the FX join to miss silently — the requirement mentions
normalisation only under the trades section, but the join cannot be correct
unless both sides are treated the same way (DECISIONS.md, section 6).
"""

from __future__ import annotations

import duckdb

from .config import CURRENCY_ALIASES


def canonical_text(column: str) -> str:
    """SQL fragment: trim, uppercase, and treat the empty string as NULL.

    An identifier that is blank or whitespace-only carries no information, so it
    is folded to NULL and handled by the same exclusion as a true NULL.
    """
    return f"upper(nullif(trim({column}), ''))"


def canonical_currency(column: str) -> str:
    """SQL fragment: canonicalise a currency code and resolve non-ISO aliases.

    'Normalised to ISO 4217' is not satisfied by case folding alone. A code such
    as 'NIS' is already uppercase and three letters, so it passes the ISO shape
    test and is never excluded — but it matches no row in the rate feed, which
    publishes the same currency as 'ILS'. The result is a trade that survives
    every filter and then silently resolves to not_found with a NULL USD
    exposure.

    Aliases are resolved alongside the other normalisations and before any
    filtering or joining, so both the exclusion chain and the FX join see one
    spelling per currency (DECISIONS.md, section 6.1).
    """
    canonical = canonical_text(column)
    if not CURRENCY_ALIASES:
        return canonical
    branches = " ".join(
        f"WHEN '{alias}' THEN '{iso}'" for alias, iso in sorted(CURRENCY_ALIASES.items())
    )
    return f"CASE {canonical} {branches} ELSE {canonical} END"


def scalar(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    """Run a single-value query and return it as an int, treating NULL as 0."""
    row = con.execute(sql).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def row_count(con: duckdb.DuckDBPyConnection, table: str) -> int:
    """Row count of a table or fully-qualified relation."""
    return scalar(con, f"SELECT count(*) FROM {table}")
