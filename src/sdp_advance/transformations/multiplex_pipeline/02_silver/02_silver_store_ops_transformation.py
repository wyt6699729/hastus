from pyspark import pipelines as dp
import pyspark.sql.functions as F


@dp.table(
    name="multiplex_2_silver.store_ops_silver",
    comment="Silver table for store operations data",
    table_properties={"quality": "silver"},
)
def store_ops_silver():
    return (
        spark.readStream.table("multiplex_1_bronze.store_ops_intermediate")  # noqa: F821
        .filter(
            F.col("extracted_timestamp").isNotNull()
            & F.col("store_id").isNotNull()
            & F.col("event_type").isNotNull()
        )
        .select(
            F.coalesce(F.col("extracted_event_id"), F.col("event_id")).alias("event_id"),
            F.col("extracted_timestamp").alias("timestamp"),
            F.col("event_group"),
            F.col("event_type"),
            F.col("subsidiary_id"),
            F.col("store_id"),
            F.col("city"),
            F.col("region"),
            F.col("opened_by_employee_id"),
            F.to_date(F.col("extracted_timestamp")).alias("event_date"),
            F.hour(F.col("extracted_timestamp")).alias("event_hour"),
            F.split(F.col("store_id"), "_").getItem(2).alias("store_number"),
        )
    )
