import pyspark.sql.functions as F
from pyspark import pipelines as dp

_CHECK_SUBSIDIARY_ID = "subsidiary_id IS NOT NULL"
_CHECK_CUSTOMER_ID = "customer_id IS NOT NULL"
_CHECK_SKU = "sku IS NOT NULL"
_VALID_DISCOUNT_RANGE = "discount_pct IS NULL OR (discount_pct >= 0 AND discount_pct <= 100)"
_VALID_DATE_RANGE = (
    "order_date IS NULL OR "
    "(order_date >= date_sub(date('2026-01-01'), 1460) AND order_date <= date('2026-01-01'))"
)
_VALID_SHIPPING_COST = "CASE WHEN shipping_cost IS NOT NULL THEN shipping_cost > 0 AND shipping_cost < 100 ELSE TRUE END"


dp.create_streaming_table(
    name="dq_2_silver.sales_silver_dq",
    comment="Quarantine table with 6 expectations - supports inverse logic pattern",
    table_properties={
        "quality.layer": "silver_quarantine",
        "quality.pattern": "inverse_logic",
    },
    schema="""
      subsidiary_id STRING,
      order_id STRING,
      order_timestamp TIMESTAMP,
      order_date DATE,
      customer_id STRING,
      region STRING,
      country STRING,
      city STRING,
      channel STRING,
      sku STRING,
      category STRING,
      qty INT,
      unit_price DOUBLE,
      discount_pct DOUBLE,
      total_amount DOUBLE,
      coupon_code STRING,
      order_status STRING,
      shipping_cost DOUBLE,
      source_file STRING,
      is_quarantined BOOLEAN,
      quarantine_reason STRING
    """,
)


@dp.expect("check_subsidiary_id", _CHECK_SUBSIDIARY_ID)
@dp.expect("check_customer_id", _CHECK_CUSTOMER_ID)
@dp.expect("check_sku", _CHECK_SKU)
@dp.expect("valid_discount_range", _VALID_DISCOUNT_RANGE)
@dp.expect("valid_date_range", _VALID_DATE_RANGE)
@dp.expect("valid_shipping_cost", _VALID_SHIPPING_COST)
@dp.temporary_view(name="sales_dq_validated_source")
def sales_dq_validated_source():
    return spark.readStream.table("dq_1_bronze.sales_bronze_clean")  # noqa: F821


@dp.append_flow(name="apply_inverse_logic_flow", target="dq_2_silver.sales_silver_dq")
def apply_inverse_logic_flow():
    source = spark.readStream.table("sales_dq_validated_source")  # noqa: F821
    all_rules_pass = (
        F.expr(_CHECK_SUBSIDIARY_ID)
        & F.expr(_CHECK_CUSTOMER_ID)
        & F.expr(_CHECK_SKU)
        & F.expr(_VALID_DISCOUNT_RANGE)
        & F.expr(_VALID_DATE_RANGE)
        & F.expr(_VALID_SHIPPING_COST)
    )
    return source.select(
        F.col("subsidiary_id"),
        F.col("order_id"),
        F.col("order_timestamp"),
        F.col("order_date"),
        F.col("customer_id"),
        F.col("region"),
        F.col("country"),
        F.col("city"),
        F.col("channel"),
        F.col("sku"),
        F.col("category"),
        F.col("qty"),
        F.col("unit_price"),
        F.col("discount_pct"),
        F.col("total_amount"),
        F.col("coupon_code"),
        F.col("order_status"),
        F.col("shipping_cost"),
        F.col("source_file"),
        (~all_rules_pass).alias("is_quarantined"),
        F.concat_ws(
            "; ",
            F.when(F.col("subsidiary_id").isNull(), F.lit("Missing subsidiary_id")),
            F.when(F.col("customer_id").isNull(), F.lit("Missing customer_id")),
            F.when(F.col("sku").isNull(), F.lit("Missing sku")),
            F.when(
                F.col("discount_pct").isNotNull()
                & ((F.col("discount_pct") < 0) | (F.col("discount_pct") > 100)),
                F.lit("Invalid discount_pct (must be 0-100)"),
            ),
            F.when(
                F.col("order_date").isNotNull()
                & (
                    (F.col("order_date") < F.date_sub(F.to_date(F.lit("2026-01-01")), 1460))
                    | (F.col("order_date") > F.to_date(F.lit("2026-01-01")))
                ),
                F.lit("Invalid order_date (outside 4-year range)"),
            ),
            F.when(
                F.col("shipping_cost").isNotNull()
                & ((F.col("shipping_cost") <= 0) | (F.col("shipping_cost") >= 100)),
                F.lit("Invalid shipping_cost (must be 0-100)"),
            ),
        ).alias("quarantine_reason"),
    )
