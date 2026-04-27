from pyspark import pipelines as dp
import pyspark.sql.functions as F


@dp.materialized_view(
    name="dq_3_gold.sales_dq_monitoring",
    comment="DQ monitoring dashboard metrics by order_date",
    table_properties={"quality.layer": "gold_monitoring"},
)
def sales_dq_monitoring():
    return (
        spark.read.table("dq_2_silver.sales_silver_dq")  # noqa: F821
        .groupBy("order_date")
        .agg(
            F.count(F.lit(1)).alias("total_records"),
            F.sum(F.when(F.col("is_quarantined"), F.lit(1)).otherwise(F.lit(0))).alias(
                "quarantined_records"
            ),
            F.sum(F.when(~F.col("is_quarantined"), F.lit(1)).otherwise(F.lit(0))).alias("valid_records"),
        )
        .withColumn(
            "quarantine_rate_pct",
            F.round(
                F.when(F.col("total_records") > 0, F.col("quarantined_records") * 100.0 / F.col("total_records"))
                .otherwise(F.lit(0.0)),
                2,
            ),
        )
        .withColumn("last_refreshed_at", F.current_timestamp())
    )


@dp.materialized_view(
    name="dq_3_gold.sales_dq_rule_breakdown",
    comment="DQ violations grouped by rule for remediation prioritization",
    table_properties={"quality.layer": "gold_monitoring"},
)
def sales_dq_rule_breakdown():
    return (
        spark.read.table("dq_2_silver.sales_silver_quarantined")  # noqa: F821
        .withColumn("rule_name", F.explode(F.split(F.col("quarantine_reason"), "; ")))
        .groupBy("rule_name")
        .agg(F.count(F.lit(1)).alias("violations"))
        .orderBy(F.col("violations").desc(), F.col("rule_name").asc())
    )
