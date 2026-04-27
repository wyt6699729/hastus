from pyspark import pipelines as dp
import pyspark.sql.functions as F

@dp.table(
    name="multiplex_1_bronze.marketing_intermediate",
    comment="Intermediate table for marketing data",
    table_properties={"delta.feature.variantType-preview": "supported"},
)
def marketing_intermediate():
    return (
        spark.readStream.table("multiplex_1_bronze.bronze_events")  # noqa: F821
        .filter(F.col("event_group") == "business_events_marketing")
        .select(
            F.col("event_id"),
            F.col("event_data_variant"),
            F.col("event_group"),
            F.expr("event_data_variant:event_id::STRING").alias("extracted_event_id"),
            F.expr("event_data_variant:timestamp::TIMESTAMP").alias("timestamp"),
            F.expr("event_data_variant:event_type::STRING").alias("event_type"),
            F.expr("event_data_variant:subsidiary_id::STRING").alias("subsidiary_id"),
            F.expr("event_data_variant:campaign_id::STRING").alias("campaign_id"),
            F.expr("event_data_variant:channel::STRING").alias("channel"),
            F.expr("event_data_variant:impressions::LONG").alias("impressions"),
            F.expr("event_data_variant:clicks::LONG").alias("clicks"),
            F.expr("event_data_variant:conversions::LONG").alias("conversions"),
            F.expr("event_data_variant:spend_usd::DOUBLE").alias("spend_usd"),
            F.col("source_file"),
            F.col("file_mod_time"),
        )
    )


@dp.table(
    name="multiplex_1_bronze.logistics_intermediate",
    comment="Intermediate table for logistics data",
    table_properties={"delta.feature.variantType-preview": "supported"},
)
def logistics_intermediate():
    return (
        spark.readStream.table("multiplex_1_bronze.bronze_events")  # noqa: F821
        .filter(F.col("event_group") == "business_events_logistics")
        .select(
            F.col("event_id"),
            F.col("event_data_variant"),
            F.col("event_group"),
            F.expr("event_data_variant:event_id::STRING").alias("extracted_event_id"),
            F.expr("event_data_variant:timestamp::TIMESTAMP").alias("timestamp"),
            F.expr("event_data_variant:event_type::STRING").alias("event_type"),
            F.expr("event_data_variant:subsidiary_id::STRING").alias("subsidiary_id"),
            F.expr("event_data_variant:warehouse_id::STRING").alias("warehouse_id"),
            F.expr("event_data_variant:carrier::STRING").alias("carrier"),
            F.expr("event_data_variant:batch_id::STRING").alias("batch_id"),
            F.expr("event_data_variant:num_packages::LONG").alias("num_packages"),
            F.expr("event_data_variant:destination_region::STRING").alias("destination_region"),
            F.col("source_file"),
            F.col("file_mod_time"),
        )
    )


@dp.table(
    name="multiplex_1_bronze.store_ops_intermediate",
    comment="Intermediate table for store operations data",
    table_properties={"delta.feature.variantType-preview": "supported"},
)
def store_ops_intermediate():
    return (
        spark.readStream.table("multiplex_1_bronze.bronze_events")  # noqa: F821
        .filter(F.col("event_group") == "business_events_store_ops")
        .select(
            F.col("event_id"),
            F.col("event_data_variant"),
            F.col("event_group"),
            F.expr("event_data_variant:event_id::STRING").alias("extracted_event_id"),
            F.expr("event_data_variant:timestamp::TIMESTAMP").alias("extracted_timestamp"),
            F.expr("event_data_variant:event_type::STRING").alias("event_type"),
            F.expr("event_data_variant:subsidiary_id::STRING").alias("subsidiary_id"),
            F.expr("event_data_variant:store_id::STRING").alias("store_id"),
            F.expr("event_data_variant:city::STRING").alias("city"),
            F.expr("event_data_variant:region::STRING").alias("region"),
            F.expr("event_data_variant:opened_by_employee_id::STRING").alias(
                "opened_by_employee_id"
            ),
            F.col("source_file"),
            F.col("file_mod_time"),
        )
    )