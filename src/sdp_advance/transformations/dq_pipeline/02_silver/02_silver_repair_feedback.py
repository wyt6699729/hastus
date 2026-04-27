from pyspark import pipelines as dp
import pyspark.sql.functions as F


@dp.table(
    name="dq_2_silver.sales_silver_repaired",
    comment="Records auto-repaired from quarantine and re-validated for consumption",
    table_properties={"quality.layer": "silver_repaired"},
)
def sales_silver_repaired():
    source = spark.readStream.table("dq_2_silver.sales_silver_quarantined")  # noqa: F821
    repaired = (
        source.withColumn(
            "discount_pct",
            F.when(F.col("discount_pct").isNull(), F.lit(None).cast("double"))
            .when(F.col("discount_pct") < 0, F.lit(0.0))
            .when(F.col("discount_pct") > 100, F.lit(100.0))
            .otherwise(F.col("discount_pct")),
        )
        .withColumn(
            "order_date",
            F.when(
                F.col("order_date").isNotNull()
                & (
                    (F.col("order_date") < F.date_sub(F.to_date(F.lit("2026-01-01")), 1460))
                    | (F.col("order_date") > F.to_date(F.lit("2026-01-01")))
                ),
                F.lit(None).cast("date"),
            ).otherwise(F.col("order_date")),
        )
        .withColumn(
            "shipping_cost",
            F.when(
                F.col("shipping_cost").isNotNull()
                & ((F.col("shipping_cost") <= 0) | (F.col("shipping_cost") >= 100)),
                F.lit(None).cast("double"),
            ).otherwise(F.col("shipping_cost")),
        )
    )
    passes_rules = (
        F.col("subsidiary_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("sku").isNotNull()
        & (F.col("discount_pct").isNull() | ((F.col("discount_pct") >= 0) & (F.col("discount_pct") <= 100)))
        & (
            F.col("order_date").isNull()
            | (
                (F.col("order_date") >= F.date_sub(F.to_date(F.lit("2026-01-01")), 1460))
                & (F.col("order_date") <= F.to_date(F.lit("2026-01-01")))
            )
        )
        & (F.col("shipping_cost").isNull() | ((F.col("shipping_cost") > 0) & (F.col("shipping_cost") < 100)))
    )
    return (
        repaired.filter(passes_rules)
        .drop("is_quarantined", "quarantine_reason")
        .withColumn("repair_applied_at", F.current_timestamp())
    )


@dp.table(
    name="dq_2_silver.sales_silver_curated",
    comment="Unified clean dataset: original valid records plus repaired records",
    table_properties={"quality.layer": "silver_curated"},
)
def sales_silver_curated():
    valid = spark.readStream.table("dq_2_silver.sales_silver_valid")  # noqa: F821
    repaired = spark.readStream.table("dq_2_silver.sales_silver_repaired")  # noqa: F821
    return (
        valid.withColumn("record_origin", F.lit("valid"))
        .unionByName(
            repaired.withColumn("record_origin", F.lit("repaired")),
            allowMissingColumns=True,
        )
    )
