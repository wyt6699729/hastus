from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

_ORDERS_SILVER_SCHEMA = StructType(
    [
        StructField("subsidiary_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("order_timestamp", TimestampType(), True),
        StructField("order_date", DateType(), True),
        StructField("customer_id", StringType(), True),
        StructField("region", StringType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("sku", StringType(), True),
        StructField("category", StringType(), True),
        StructField("qty", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("discount_pct", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("coupon_code", StringType(), True),
    ]
)


@dp.table(
    name="multi_flow_2_silver.orders_silver_flows",
    comment="Clean and standardize data from the multiple-flow bronze table",
    schema=_ORDERS_SILVER_SCHEMA,
    cluster_by_auto=True,
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("qty_valid", "qty >= 0")
@dp.expect_or_drop("total_amount_valid", "total_amount >= 0")
@dp.expect_or_fail("timestamp_not_null", "order_timestamp IS NOT NULL")
def orders_silver_flows():
    return spark.readStream.table("multi_flow_1_bronze.orders_bronze_flows").select(  # noqa: F821
        F.col("subsidiary_id"),
        F.col("order_id"),
        F.expr("try_cast(order_timestamp as timestamp)").alias("order_timestamp"),
        F.expr("try_cast(order_date as date)").alias("order_date"),
        F.col("customer_id"),
        F.col("region"),
        F.col("country"),
        F.col("city"),
        F.col("channel"),
        F.col("sku"),
        F.col("category"),
        F.expr("try_cast(qty as int)").alias("qty"),
        F.expr("try_cast(unit_price as double)").alias("unit_price"),
        F.expr("try_cast(discount_pct as double)").alias("discount_pct"),
        F.expr("try_cast(total_amount as double)").alias("total_amount"),
        F.col("coupon_code"),
    )