from pyspark.sql import functions as F


def get_pipeline_config(spark, key):
    return spark.conf.get(key)

def get_hastus_source(spark):
    return get_pipeline_config(spark, "hastus_source")

def read_hastus_xml(spark, source_root, dataset, row_tag):
    source_path = f"{source_root}/{dataset}"
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "xml")
        .option("rowTag", row_tag)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(source_path)
    )


def convert_hastus_time(col_name, format_type="HH:mm"):
    """Convert HASTUS pseudo-dates into continuous transit hours."""
    source_col = F.col(col_name)
    day_val = F.substring(source_col, 9, 2).cast("int")
    hour_val = F.substring(source_col, 12, 2).cast("int")

    total_hours = (day_val - F.lit(1)) * F.lit(24) + hour_val
    formatted_hour = F.lpad(total_hours.cast("string"), 2, "0")

    if format_type == "HH:mm":
        new_time = F.concat(formatted_hour, F.substring(source_col, 14, 3))
    else:
        new_time = F.concat(formatted_hour, F.substring(source_col, 14, 100))

    return F.when(source_col.isNotNull() & (source_col != ""), new_time).otherwise(source_col)


def absence_creation_datetime(abs_creation_date_col, abs_creation_time_col):
    """Combine abs_creation_date and HASTUS abs_creation_time into a wall-clock timestamp.

    Uses the same day/hour offsets as convert_hastus_time (substring positions 9–10 and 12–13).
    Calendar day comes from abs_creation_date; elapsed time is (day-1)*24h + hour + :mm[:ss]
    from the tail of the pseudo time string (colon-separated fragment starting at position 14).
    """
    date_col = F.trim(F.col(abs_creation_date_col))
    time_col = F.trim(F.col(abs_creation_time_col))
    d = F.coalesce(
        F.to_date(date_col, "yyyyMMdd"),
        F.to_date(date_col, "yyyy-MM-dd"),
    )
    day_val = F.substring(time_col, 9, 2).cast("int")
    hour_val = F.substring(time_col, 12, 2).cast("int")
    total_hours = (day_val - F.lit(1)) * F.lit(24) + hour_val
    minute_val = F.when(
        F.substring(time_col, 14, 1) == ":",
        F.substring(time_col, 15, 2).cast("int"),
    ).otherwise(F.lit(0))
    second_val = F.when(
        (F.substring(time_col, 17, 1) == ":") & (F.length(time_col) >= F.lit(19)),
        F.substring(time_col, 18, 2).cast("int"),
    ).otherwise(F.lit(0))
    base_ts = F.to_timestamp(
        F.concat(F.date_format(d, "yyyy-MM-dd"), F.lit(" 00:00:00")),
        "yyyy-MM-dd HH:mm:ss",
    )
    offset_sec = (
        total_hours * F.lit(3600)
        + F.coalesce(minute_val, F.lit(0)) * F.lit(60)
        + F.coalesce(second_val, F.lit(0))
    )
    epoch = F.unix_timestamp(base_ts)
    out = F.to_timestamp(F.from_unixtime(epoch + offset_sec))
    return F.when(
        d.isNotNull()
        & time_col.isNotNull()
        & (time_col != "")
        & day_val.isNotNull()
        & hour_val.isNotNull(),
        out,
    ).otherwise(F.lit(None).cast("timestamp"))
