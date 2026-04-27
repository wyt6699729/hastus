from pyspark import pipelines as dp
import pyspark.sql.functions as F


@dp.table(
    name="dq_1_bronze.sales_bronze_clean",
    comment="Intermediate table - type casting only, no quality checks",
    table_properties={"quality": "bronze"},
)
def sales_bronze_clean():
    return (
        spark.readStream.table("dq_1_bronze.sales_bronze_raw")  # noqa: F821
        .select(
            F.col("subsidiary_id"),
            F.col("order_id"),
            F.expr("try_cast(order_timestamp AS TIMESTAMP)").alias("order_timestamp"),
            F.expr("try_cast(order_date AS DATE)").alias("order_date"),
            F.col("customer_id"),
            F.col("region"),
            F.col("country"),
            F.col("city"),
            F.col("channel"),
            F.col("sku"),
            F.col("category"),
            F.expr("try_cast(qty AS INT)").alias("qty"),
            F.expr("try_cast(unit_price AS DOUBLE)").alias("unit_price"),
            F.expr("try_cast(discount_pct AS DOUBLE)").alias("discount_pct"),
            F.expr("try_cast(total_amount AS DOUBLE)").alias("total_amount"),
            F.col("coupon_code"),
            F.col("order_status"),
            F.expr("try_cast(shipping_cost AS DOUBLE)").alias("shipping_cost"),
            F.col("source_file"),
        )
    )
