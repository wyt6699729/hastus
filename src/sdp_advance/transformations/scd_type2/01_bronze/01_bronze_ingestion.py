from pyspark import pipelines as dp
import pyspark.sql.functions as F


@dp.table(
    name="sdp_cdc_1_bronze.bronze_customer_raw",
    comment="Raw data from customers CDC feed",
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "false",
    },
)
def bronze_customer_raw():
    source = spark.conf.get("source")  # noqa: F821
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .load(source)
        .select(
            "*",
            F.current_timestamp().alias("processing_time"),
            F.col("_metadata.file_name").alias("source_file"),
        )
    )
