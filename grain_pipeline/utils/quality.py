"""Data quality framework.

This module holds the *mechanism*; the checks themselves live with the model
they guard, because a check is part of building a table correctly rather than a
separate concern bolted on afterwards. ``dim_clients`` validates its own
intervals, ``fx_to_usd`` validates its own key uniqueness, and
``fact_daily_exposure`` validates its own grain and reconciliation.

Two severities, and the distinction is deliberate (DECISIONS.md, section 11):

``require``
    Raises :class:`DataQualityError`, which aborts the run and — because the
    whole build executes inside one transaction — rolls the target back to its
    last good state. Used where the *output* would be wrong.

``warn``
    Logs and continues. Used where an *input* is untidy but the output is
    unaffected, so failing the load would block a correct result over an
    upstream inconsistency the pipeline has already routed around.

Every failure mode these guard is silent by construction. A fan-out produces a
well-formed table with inflated numbers and raises nothing; a dropped row simply
makes a total smaller with nothing to indicate it should not have been. A
partially-correct analytical table is worse than an absent one, because it will
be trusted.
"""

from __future__ import annotations

from .logging_setup import get_logger


class DataQualityError(RuntimeError):
    """Raised when a data quality assertion fails. Aborts and rolls back the run."""


def require(condition_failed: int, message: str) -> None:
    """Raise :class:`DataQualityError` when a check finds offending rows.

    ``condition_failed`` is the count of rows or keys violating the rule; zero
    means the check passed.
    """
    if condition_failed:
        raise DataQualityError(message)


def warn(offending: int, message: str) -> None:
    """Log a data quality problem that cannot corrupt the output."""
    if offending:
        get_logger().warning("  WARN  %s", message)


def passed(message: str) -> None:
    """Record a check that found nothing, so a clean run is auditable too."""
    get_logger().info("  PASS  %s", message)
