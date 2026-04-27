from pyspark import pipelines as dp
import pyspark.sql.functions as F

@dp.table(
    name = 'multiplex_1_bronze.bronze_events',
    comment = 'Ingest bronze events from the volume',
    table_properties = {
        "pipelines.reset.allowed": "false",
        "delta.feature.variantType-preview": "supported"
    }
)
def bronze_events():
    source = spark.conf.get("business_events_source")  # noqa: F821
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "parquet")
        .load(source)
        .select(
            F.col("key").cast("string").alias("event_id"),
            F.parse_json(F.col("value").cast("string")).alias("event_data_variant"),
            F.col("topic").cast("string").alias("event_group"),
            F.col("partition").cast("string").alias("partition"),
            F.col("offset").cast("string").alias("offset"),
            F.col("_metadata.file_name").alias("source_file"),
            F.col("_metadata.file_modification_time").alias("file_mod_time"),
        )
    )