from pyspark import pipelines as dp
import pyspark.sql.functions as F


@dp.materialized_view(
    name="dq_3_gold.sales_analytics",
    comment="Business metrics aggregated from validated sales data",
    table_properties={"quality.layer": "gold"},
)
def sales_analytics():
    return (
        spark.read.table("dq_2_silver.sales_silver_valid")  # noqa: F821
        .groupBy("region", "country", "category")
        .agg(
            F.count_distinct("order_id").alias("total_orders"),
            F.count_distinct("customer_id").alias("unique_customers"),
            F.sum("qty").alias("total_quantity_sold"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
            F.round(F.avg("discount_pct"), 2).alias("avg_discount_pct"),
            F.min("order_date").alias("earliest_order_date"),
            F.max("order_date").alias("latest_order_date"),
            F.current_timestamp().alias("last_refreshed_at"),
        )
    )
