from datetime import datetime

from hastus_etl.utils.convert_time.vancouver_time import convert_to_vancouver_time
from hastus_etl.utils.hastus_utils import absence_creation_datetime, convert_hastus_time
from pyspark.sql import functions as F


def test_convert_hastus_time_hhmm_and_null_handling(spark):
    df = spark.createDataFrame(
        [
            ("0000000002 07:30",),
            ("",),
            (None,),
        ],
        ["raw_time"],
    )

    out = df.select(convert_hastus_time("raw_time").alias("converted")).collect()

    assert out[0]["converted"] == "31:30"
    assert out[1]["converted"] == ""
    assert out[2]["converted"] is None


def test_convert_hastus_time_extended_tail_format(spark):
    df = spark.createDataFrame([("0000000002 07:30:45",)], ["raw_time"])
    row = df.select(convert_hastus_time("raw_time", format_type="HH:mm:ss").alias("converted")).first()
    assert row["converted"] == "31:30:45"


def test_absence_creation_datetime_builds_expected_timestamp(spark):
    df = spark.createDataFrame(
        [
            ("2026-01-05", "0000000002 07:30:45"),
            ("20260105", "0000000002 07:30:45"),
            ("2026-01-05", ""),
        ],
        ["creation_date", "creation_time"],
    )

    rows = df.select(
        F.date_format(
            absence_creation_datetime("creation_date", "creation_time"),
            "yyyy-MM-dd HH:mm:ss",
        ).alias("ts_str")
    ).collect()

    assert rows[0]["ts_str"] == "2026-01-06 07:30:45"
    assert rows[1]["ts_str"] == "2026-01-06 07:30:45"
    assert rows[2]["ts_str"] is None


def test_convert_to_vancouver_time_handles_dst(spark):
    df = spark.createDataFrame(
        [
            ("2026-01-15 12:00:00",),
            ("2026-07-15 12:00:00",),
        ],
        ["utc_ts_str"],
    ).select(F.to_timestamp("utc_ts_str").alias("utc_ts"))

    rows = df.select(
        convert_to_vancouver_time("utc_ts").alias("van_ts")
    ).collect()

    assert rows[0]["van_ts"] == datetime(2026, 1, 15, 4, 0, 0)
    assert rows[1]["van_ts"] == datetime(2026, 7, 15, 5, 0, 0)
