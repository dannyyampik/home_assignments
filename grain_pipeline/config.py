"""Configuration constants for the Grain analytics pipeline.

All business-rule literals live here rather than being scattered through the SQL,
so that a reviewer can see every tunable value in one place and so that no
transformation depends on a wall-clock value (see DECISIONS.md, section 10).
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

SOURCE_DB = PROJECT_ROOT / "source_data" / "grain_raw.duckdb"
TARGET_DB = PROJECT_ROOT / "target" / "grain_analytics.duckdb"
LOG_DIR = PROJECT_ROOT / "logs"
LOG_FILE = LOG_DIR / "pipeline.log"

# Name the source database is attached under. Also usable as a plain schema
# name, which is what lets the tests run the production SQL unmodified against
# an in-memory database.
SOURCE_SCHEMA = "src"

# --- Business rules -------------------------------------------------------

TRADE_DATE_CUTOFF = date(2026, 6, 1)
ACTIVE_STATUS = "ACTIVE"
USD = "USD"

# Sentinel bounds for open-ended SCD2 intervals. Sentinels rather than NULLs so
# the point-in-time join stays a plain range predicate with no three-valued
# logic (DECISIONS.md, section 3.5).
DATE_FLOOR = date(1900, 1, 1)
DATE_CEILING = date(9999, 12, 31)

# ISO 4217 alphabetic codes are exactly three uppercase letters.
ISO_4217_PATTERN = "^[A-Z]{3}$"

# Non-ISO currency codes appearing in the trade feed, mapped to their ISO 4217
# equivalent. 'NIS' is the colloquial code for the New Israeli Sheqel, whose ISO
# code is 'ILS'; the rate feed publishes only under 'ILS'. Left untranslated,
# these trades match no rate and are silently flagged not_found with a NULL USD
# exposure (DECISIONS.md, section 6.1).
#
# Deliberately an explicit whitelist rather than fuzzy matching: every entry is
# a documented alias for the same currency, not a guess.
CURRENCY_ALIASES: dict[str, str] = {
    "NIS": "ILS",
}

RATE_SOURCE_DIRECT = "direct"
RATE_SOURCE_INVERSE = "inverse"
RATE_SOURCE_NOT_FOUND = "not_found"
