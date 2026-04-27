from pyspark import pipelines as dp
import pyspark.sql.functions as F


@dp.materialized_view(
    name="sdp_cdc_3_gold.removed_customers_gold",
    comment="Latest removed customer records from SCD Type 2 history",
    table_properties={"quality": "gold"},
)
def removed_customers_gold():
    return (
        spark.read.table("sdp_cdc_2_silver.customers_silver_scd2")  # noqa: F821
        .groupBy("customer_id")
        .agg(
            F.expr("max_by(name, __START_AT)").alias("name"),
            F.expr("max_by(address, __START_AT)").alias("address"),
            F.expr("max_by(city, __START_AT)").alias("city"),
            F.expr("max_by(state, __START_AT)").alias("state"),
            F.expr("max_by(zip_code, __START_AT)").alias("zip_code"),
            F.expr("max_by(__START_AT, __START_AT)").alias("__START_AT"),
            F.expr("max_by(__END_AT, __START_AT)").alias("__END_AT"),
        )
        .filter(F.col("__END_AT").isNotNull())
    )
