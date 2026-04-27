from pyspark import pipelines as dp
import pyspark.sql.functions as F


@dp.table(
    name="dq_1_bronze.sales_bronze_raw",
    comment="Bronze layer raw sales ingestion from volume files",
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "false",
    },
)
def sales_bronze_raw():
    sales_source = spark.conf.get("sales_source")  # noqa: F821
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaHints", "order_status STRING, shipping_cost STRING")
        .load(sales_source)
        .select(
            F.col("subsidiary_id").cast("string").alias("subsidiary_id"),
            F.col("order_id").cast("string").alias("order_id"),
            F.col("order_timestamp").cast("string").alias("order_timestamp"),
            F.col("customer_id").cast("string").alias("customer_id"),
            F.col("region").cast("string").alias("region"),
            F.col("country").cast("string").alias("country"),
            F.col("city").cast("string").alias("city"),
            F.col("channel").cast("string").alias("channel"),
            F.col("sku").cast("string").alias("sku"),
            F.col("category").cast("string").alias("category"),
            F.col("qty").cast("string").alias("qty"),
            F.col("unit_price").cast("string").alias("unit_price"),
            F.col("discount_pct").cast("string").alias("discount_pct"),
            F.col("coupon_code").cast("string").alias("coupon_code"),
            F.col("total_amount").cast("string").alias("total_amount"),
            F.col("order_date").cast("string").alias("order_date"),
            F.col("order_status").cast("string").alias("order_status"),
            F.col("shipping_cost").cast("string").alias("shipping_cost"),
            F.col("_rescued_data").cast("string").alias("_rescued_data"),
            F.col("_metadata.file_name").alias("source_file"),
            F.col("_metadata.file_modification_time").alias("file_mod_time"),
        )
    )