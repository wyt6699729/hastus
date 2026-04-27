from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

_ORDERS_BRONZE_SCHEMA = StructType(
    [
        StructField("subsidiary_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("order_timestamp", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("region", StringType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("sku", StringType(), True),
        StructField("category", StringType(), True),
        StructField("qty", StringType(), True),
        StructField("unit_price", StringType(), True),
        StructField("discount_pct", StringType(), True),
        StructField("coupon_code", StringType(), True),
        StructField("total_amount", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("file_mod_time", TimestampType(), True),
    ]
)

dp.create_streaming_table(
    name="multi_flow_1_bronze.orders_bronze_flows",
    comment=(
        "Creates a single bronze streaming table with orders from all subsidiaries "
        "using multiple flows."
    ),
    schema=_ORDERS_BRONZE_SCHEMA,
    table_properties={
        "pipelines.reset.allowed": "false",
    },
)


def _transform_raw_to_bronze(raw: DataFrame) -> DataFrame:
    """Align raw CSV/JSON rows to orders_bronze_flows BY NAME (all STRING + metadata)."""
    return raw.select(
        F.col("subsidiary_id").cast(StringType()).alias("subsidiary_id"),
        F.col("order_id").cast(StringType()).alias("order_id"),
        F.col("order_timestamp").cast(StringType()).alias("order_timestamp"),
        F.col("customer_id").cast(StringType()).alias("customer_id"),
        F.col("region").cast(StringType()).alias("region"),
        F.col("country").cast(StringType()).alias("country"),
        F.col("city").cast(StringType()).alias("city"),
        F.col("channel").cast(StringType()).alias("channel"),
        F.col("sku").cast(StringType()).alias("sku"),
        F.col("category").cast(StringType()).alias("category"),
        F.col("qty").cast(StringType()).alias("qty"),
        F.col("unit_price").cast(StringType()).alias("unit_price"),
        F.col("discount_pct").cast(StringType()).alias("discount_pct"),
        F.col("coupon_code").cast(StringType()).alias("coupon_code"),
        F.col("total_amount").cast(StringType()).alias("total_amount"),
        F.col("order_date").cast(StringType()).alias("order_date"),
        F.col("_metadata.file_name").alias("source_file"),
        F.col("_metadata.file_modification_time").alias("file_mod_time"),
    )


@dp.append_flow(
    name="bright_home_orders_flow",
    target="multi_flow_1_bronze.orders_bronze_flows",
    comment="Read CSV files from the bright_home_orders volume",
)
def bright_home_orders_flow():
    path = spark.conf.get("bright_home_orders_source")  # noqa: F821
    raw = (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(path)
    )
    return _transform_raw_to_bronze(raw)


@dp.append_flow(
    name="lumia_sports_orders_flow",
    target="multi_flow_1_bronze.orders_bronze_flows",
    comment="Read CSV files from the lumia_sports_orders volume",
)
def lumia_sports_orders_flow():
    path = spark.conf.get("lumia_sports_orders_source")  # noqa: F821
    raw = (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(path)
    )
    return _transform_raw_to_bronze(raw)


@dp.append_flow(
    name="northstar_outfitters_orders_flow",
    target="multi_flow_1_bronze.orders_bronze_flows",
    comment="Read JSON files from the northstar_outfitters_orders volume",
)
def northstar_outfitters_orders_flow():
    path = spark.conf.get("northstar_outfitters_orders_source")  # noqa: F821
    raw = (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .load(path)
    )
    return _transform_raw_to_bronze(raw)
