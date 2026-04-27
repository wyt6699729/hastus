from pyspark import pipelines as dp
import pyspark.sql.functions as F


@dp.table(
    name="dq_2_silver.sales_silver_valid",
    comment="Clean records passing all 6 quality checks - ready for analytics",
    table_properties={"quality.layer": "silver_valid"},
)
def sales_silver_valid():
    return (
        spark.readStream.table("dq_2_silver.sales_silver_dq")  # noqa: F821
        .filter(F.col("is_quarantined") == F.lit(False))
        .drop("is_quarantined", "quarantine_reason")
    )


@dp.table(
    name="dq_2_silver.sales_silver_quarantined",
    comment="Invalid records with quality violations - requires remediation",
    table_properties={"quality.layer": "silver_quarantined"},
)
def sales_silver_quarantined():
    return (
        spark.readStream.table("dq_2_silver.sales_silver_dq")  # noqa: F821
        .filter(F.col("is_quarantined") == F.lit(True))
    )
