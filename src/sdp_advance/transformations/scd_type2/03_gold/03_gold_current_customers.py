from pyspark import pipelines as dp
import pyspark.sql.functions as F


@dp.materialized_view(
    name="sdp_cdc_3_gold.current_customers_gold",
    comment="Current updated list of active customers",
    table_properties={"quality": "gold"},
)
def current_customers_gold():
    return (
        spark.read.table("sdp_cdc_2_silver.customers_silver_scd2")  # noqa: F821
        .filter(F.col("__END_AT").isNull())
        .drop("processing_time")
        .withColumn("updated_at", F.current_timestamp())
    )
