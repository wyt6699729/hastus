from pyspark import pipelines as dp
import pyspark.sql.functions as F


@dp.table(
    name="multiplex_2_silver.logistics_silver",
    comment="Silver table for logistics data",
    table_properties={"quality": "silver"},
)
def logistics_silver():
    return (
        spark.readStream.table("multiplex_1_bronze.logistics_intermediate")  # noqa: F821
        .filter(F.col("warehouse_id").isNotNull() & F.col("batch_id").isNotNull())
        .select(
            F.coalesce(F.col("extracted_event_id"), F.col("event_id")).alias("event_id"),
            F.col("timestamp"),
            F.col("event_group"),
            F.col("event_type"),
            F.col("subsidiary_id"),
            F.col("warehouse_id"),
            F.col("carrier"),
            F.col("batch_id"),
            F.col("num_packages"),
            F.col("destination_region"),
            F.when(F.col("num_packages") > 0, F.lit(True))
            .otherwise(F.lit(False))
            .alias("is_valid_shipment"),
            F.to_date(F.col("timestamp")).alias("event_date"),
        )
    )
