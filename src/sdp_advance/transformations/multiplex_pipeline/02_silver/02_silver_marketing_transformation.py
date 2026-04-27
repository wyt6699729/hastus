from pyspark import pipelines as dp
import pyspark.sql.functions as F

@dp.table(
    name="multiplex_2_silver.marketing_silver",
    comment="Silver table for marketing data",
    table_properties={"quality": "silver"},
)
def marketing_silver():
    return (
        spark.readStream.table("multiplex_1_bronze.marketing_intermediate")  # noqa: F821
        .select(
            F.coalesce(F.col("extracted_event_id"), F.col("event_id")).alias("event_id"),
            F.col("timestamp"),
            F.col("event_group"),
            F.col("event_type"),
            F.col("subsidiary_id"),
            F.col("campaign_id"),
            F.col("channel"),
            F.col("impressions"),
            F.col("clicks"),
            F.col("conversions"),
            F.col("spend_usd"),
            F.when(F.col("impressions") > 0, F.col("clicks") / F.col("impressions"))
            .otherwise(F.lit(0))
            .alias("click_through_rate"),
            F.when(F.col("clicks") > 0, F.col("spend_usd") / F.col("clicks"))
            .otherwise(F.lit(0))
            .alias("cost_per_click"),
        )
    )