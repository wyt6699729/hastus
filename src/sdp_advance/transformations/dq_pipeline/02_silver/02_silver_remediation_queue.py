from pyspark import pipelines as dp
import pyspark.sql.functions as F


@dp.table(
    name="dq_2_silver.sales_quarantine_remediation_queue",
    comment="Quarantine queue with prioritized remediation actions",
    table_properties={"quality.layer": "silver_remediation"},
)
def sales_quarantine_remediation_queue():
    return (
        spark.readStream.table("dq_2_silver.sales_silver_quarantined")  # noqa: F821
        .withColumn("primary_reason", F.split(F.col("quarantine_reason"), "; ").getItem(0))
        .withColumn(
            "priority",
            F.when(
                F.col("primary_reason").isin("Missing subsidiary_id", "Missing customer_id", "Missing sku"),
                F.lit("P1"),
            )
            .when(F.col("primary_reason").isNotNull(), F.lit("P2"))
            .otherwise(F.lit("P3")),
        )
        .withColumn(
            "recommended_action",
            F.when(F.col("primary_reason") == "Missing subsidiary_id", F.lit("Backfill subsidiary mapping"))
            .when(F.col("primary_reason") == "Missing customer_id", F.lit("Join customer master and repair key"))
            .when(F.col("primary_reason") == "Missing sku", F.lit("Map SKU using product dimension"))
            .when(
                F.col("primary_reason") == "Invalid discount_pct (must be 0-100)",
                F.lit("Clamp discount to valid business range"),
            )
            .when(
                F.col("primary_reason") == "Invalid order_date (outside 4-year range)",
                F.lit("Repair source date parsing and timezone handling"),
            )
            .when(
                F.col("primary_reason") == "Invalid shipping_cost (must be 0-100)",
                F.lit("Recalculate shipping cost from carrier tariff"),
            )
            .otherwise(F.lit("Manual investigation required")),
        )
        .withColumn("detected_at", F.current_timestamp())
    )
