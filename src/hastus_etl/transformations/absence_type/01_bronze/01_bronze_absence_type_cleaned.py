import pyspark.sql.functions as F
from pyspark import pipelines as dp
from utils.convert_time import convert_to_vancouver_time
from utils.hastus_utils import absence_creation_datetime


# Create the Bronze Clean Streaming Table
@dp.table(
    name="01_bronze.absence_type_cleaned",
    comment="Absence type cleaned data from the raw data",
    table_properties={
        "quality": "bronze",
        "delta.feature.timestampNtz": "supported",
    },
)
@dp.expect_or_fail("valid_absence_code", "absence_code IS NOT NULL")
@dp.expect("valid_absence_description", "absence_description IS NOT NULL")
def absence_type_cleaned():
    return (
        dp.read_stream("01_bronze.absence_type_raw")
        .withColumn(
            "absence_creation_datetime",
            absence_creation_datetime("abs_creation_date", "abs_creation_time"),
        )
        .select(
            F.col("abst_identifier").alias("absence_code"),
            F.col("abst_description").alias("absence_description"),
            F.col("absence_creation_datetime"),
        )
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn(
            "dw_load_date",
            convert_to_vancouver_time("_ingest_ts", source_timezone="UTC"),
        )
        .withColumn(
            "dw_change_date",
            convert_to_vancouver_time("_ingest_ts", source_timezone="UTC"),
        )
        .drop("_ingest_ts")
    )
