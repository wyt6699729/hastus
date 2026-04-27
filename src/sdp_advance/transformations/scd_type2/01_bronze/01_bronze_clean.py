from pyspark import pipelines as dp
import pyspark.sql.functions as F

# SQL: ON VIOLATION FAIL UPDATE -> fail if any row violates
# SQL: ON VIOLATION DROP ROW -> expect_or_drop
# SQL: no ON VIOLATION -> @dp.expect (warn, keep rows)

_VALID_EMAIL_SQL = (
    "(rlike(email, '^([a-zA-Z0-9_\\\\-\\\\.]+)@([a-zA-Z0-9_\\\\-\\\\.]+)\\\\.([a-zA-Z]{2,5})$')) "
    "OR operation = 'DELETE'"
)


@dp.table(
    name="sdp_cdc_1_bronze.bronze_customer_clean",
    comment="Clean raw bronze data and apply quality constraints",
)
@dp.expect_or_fail("valid_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid_operation", "operation IS NOT NULL")
@dp.expect("valid_name", "name IS NOT NULL OR operation = 'DELETE'")
@dp.expect(
    "valid_address",
    "(address IS NOT NULL AND city IS NOT NULL AND state IS NOT NULL AND zip_code IS NOT NULL) "
    "OR operation = 'DELETE'",
)
@dp.expect_or_drop("valid_email", _VALID_EMAIL_SQL)
def bronze_customer_clean():
    return (
        spark.readStream.table("sdp_cdc_1_bronze.bronze_customer_raw")  # noqa: F821
        .withColumn(
            "timestamp_datetime",
            F.from_unixtime(F.col("timestamp")).cast("timestamp"),
        )
    )
