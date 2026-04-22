"""Timestamp conversion helpers for America/Vancouver."""

from __future__ import annotations

from typing import Union

from pyspark.sql import Column
from pyspark.sql import functions as F

VANCOUVER_TZ = "America/Vancouver"


def convert_to_vancouver_time(
    column: Union[str, Column],
    source_timezone: str = "UTC",
) -> Column:
    """Convert a timestamp column to Vancouver local time.

    Uses Spark ``convert_timezone`` so the input is interpreted in ``source_timezone``
    and returned as a timestamp in ``America/Vancouver`` (same wall clock as
    ``America/Los_Angeles`` for modern dates; handles DST via the zone rules).

    Args:
        column: Column name or Spark ``Column`` containing ``timestamp`` / ``timestamptz``.
        source_timezone: IANA timezone name for how the stored values should be read
            (default ``UTC``, typical for ``current_timestamp()`` on UTC clusters).

    Returns:
        A ``Column`` of timestamps in Vancouver local civil time.
    """
    ts = F.col(column) if isinstance(column, str) else column
    return F.convert_timezone(F.lit(source_timezone), F.lit(VANCOUVER_TZ), ts)
