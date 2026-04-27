from pyspark import pipelines as dp
import pyspark.sql.functions as F


@dp.materialized_view(
    name="multiplex_3_gold.marketing_campaign_summary",
    comment="Marketing campaign performance summary at campaign/subsidiary/channel grain",
    table_properties={"quality": "gold"},
)
def marketing_campaign_summary():
    source = spark.read.table("multiplex_2_silver.marketing_silver")  # noqa: F821
    aggregated = source.groupBy("campaign_id", "subsidiary_id", "channel").agg(
        F.count(F.lit(1)).alias("total_events"),
        F.sum("impressions").alias("total_impressions"),
        F.sum("clicks").alias("total_clicks"),
        F.sum("conversions").alias("total_conversions"),
        F.round(F.sum("spend_usd"), 2).alias("total_spend_usd"),
        F.sum("clicks").alias("_sum_clicks"),
        F.sum("impressions").alias("_sum_impressions"),
        F.sum("conversions").alias("_sum_conversions"),
        F.sum("spend_usd").alias("_sum_spend_usd"),
    )
    return aggregated.select(
        "campaign_id",
        "subsidiary_id",
        "channel",
        "total_events",
        "total_impressions",
        "total_clicks",
        "total_conversions",
        "total_spend_usd",
        F.round(
            F.when(
                F.col("_sum_impressions") != 0,
                (F.col("_sum_clicks") * F.lit(1.0) / F.col("_sum_impressions")) * 100,
            ),
            2,
        ).alias("ctr_percentage"),
        F.round(
            F.when(
                F.col("_sum_clicks") != 0,
                (F.col("_sum_conversions") * F.lit(1.0) / F.col("_sum_clicks")) * 100,
            ),
            2,
        ).alias("conversion_rate_percentage"),
        F.round(
            F.when(
                F.col("_sum_conversions") != 0,
                F.col("_sum_spend_usd") / F.col("_sum_conversions"),
            ),
            2,
        ).alias("cost_per_conversion"),
    )
