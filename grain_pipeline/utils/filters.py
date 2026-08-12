"""The counted exclusion chain.

Generic machinery: a model supplies an ordered tuple of :class:`FilterStep` and
this runs them one at a time, counting and logging what each removed. "Log how
many records are excluded at each filtering step" is a stated deliverable, so
the counts are produced by the same code that does the filtering rather than
recomputed afterwards — a count derived separately from the filter can drift
from it.

Because the steps are applied in sequence, a row violating several rules is
attributed to the *first* rule it fails. The counts therefore read as "removed
at this step", not "total rows violating this rule", and the step order is fixed
so the numbers reproduce across runs (DECISIONS.md, section 7).
"""

from __future__ import annotations

from dataclasses import dataclass

import duckdb

from .logging_setup import get_logger
from .sql import row_count


@dataclass(frozen=True)
class FilterStep:
    """One stage of an exclusion chain.

    ``predicate`` is the SQL that rows must satisfy to be *kept*; ``description``
    states the condition that gets a row *excluded*, since that is what the log
    line is reporting.
    """

    name: str
    predicate: str
    description: str


def apply_filter_chain(
    con: duckdb.DuckDBPyConnection,
    source_table: str,
    output_table: str,
    steps: tuple[FilterStep, ...],
    unit: str = "rows",
) -> dict[str, int]:
    """Run ``steps`` in order, materialising ``output_table``.

    Returns a mapping of step name to rows removed, so a caller can assert on
    the counts as well as read them in the log. Tests use the return value; the
    log is for operators — which is why ``unit`` exists: the chain is generic,
    but "527 trades retained" tells an operator what survived and "527 rows" does
    not.

    The scratch tables are unqualified, so a caller must not invoke this chain
    re-entrantly from inside another chain. Usage is strictly sequential today;
    namespacing them by ``output_table`` is the fix if that ever changes.
    """
    logger = get_logger()
    exclusions: dict[str, int] = {}

    con.execute(f"CREATE OR REPLACE TEMP TABLE _filtered AS SELECT * FROM {source_table}")
    remaining = row_count(con, "_filtered")
    logger.info("Filter chain starting with %s %s.", f"{remaining:,}", unit)

    for step in steps:
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE _filtered_next AS "
            f"SELECT * FROM _filtered WHERE {step.predicate}"
        )
        kept = row_count(con, "_filtered_next")
        removed = remaining - kept
        exclusions[step.name] = removed

        logger.info(
            "  [%-28s] excluded %6s rows (%s) | %s remaining",
            step.name,
            f"{removed:,}",
            step.description,
            f"{kept:,}",
        )

        con.execute("DROP TABLE _filtered")
        con.execute("ALTER TABLE _filtered_next RENAME TO _filtered")
        remaining = kept

    con.execute(f"CREATE OR REPLACE TEMP TABLE {output_table} AS SELECT * FROM _filtered")
    con.execute("DROP TABLE _filtered")
    logger.info(
        "Filter chain complete: %s %s excluded in total, %s retained.",
        f"{sum(exclusions.values()):,}",
        unit,
        f"{remaining:,}",
    )
    return exclusions
