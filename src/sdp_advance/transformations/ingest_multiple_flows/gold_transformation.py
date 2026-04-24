from pyspark import pipelines as dp
import pyspark.sql.functions as F


@dp.materialized_view(
    name="multi_flow_3_gold.mv_daily_subsidiary_scorecard",
    comment="Daily summary by subsidiary from multi-flow silver orders",
    table_properties={"quality": "gold"},
)
def mv_daily_subsidiary_scorecard():
    silver_df = spark.read.table("multi_flow_2_silver.orders_silver_flows")  # noqa: F821
    return (
        silver_df.where(F.col("order_date").isNotNull())
        .groupBy("order_date", "subsidiary_id")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.sum("qty").alias("total_units"),
        )
    )


@dp.materialized_view(
    name="multi_flow_3_gold.mv_product_performance_by_subsidiary",
    comment="Product performance by subsidiary from multi-flow silver orders",
    table_properties={"quality": "gold"},
)
def mv_product_performance_by_subsidiary():
    silver_df = spark.read.table("multi_flow_2_silver.orders_silver_flows")  # noqa: F821
    return silver_df.groupBy("subsidiary_id", "category", "sku").agg(
        F.sum("qty").alias("units_sold"),
        F.round(F.sum("total_amount"), 2).alias("revenue"),
    )
