"""Logging configuration.

Exclusion counts are a stated deliverable, not a debugging aid, so they are
emitted through the standard logging framework to both stdout and a file rather
than printed.
"""

from __future__ import annotations

import logging
import sys

from .config import LOG_DIR, LOG_FILE

LOGGER_NAME = "grain_pipeline"


def configure_logging(level: int = logging.INFO) -> logging.Logger:
    """Configure and return the pipeline logger.

    Safe to call more than once; handlers are not duplicated.
    """
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(level)

    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except OSError:
        # A read-only filesystem should degrade to stdout rather than fail the
        # run: losing the log file is recoverable, losing the load is not.
        logger.warning("Could not open log file at %s; logging to stdout only.", LOG_FILE)

    return logger


def get_logger() -> logging.Logger:
    return logging.getLogger(LOGGER_NAME)
