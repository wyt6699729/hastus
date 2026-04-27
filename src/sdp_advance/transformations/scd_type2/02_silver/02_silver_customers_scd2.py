from pyspark import pipelines as dp
import pyspark.sql.functions as F

dp.create_streaming_table(
    name="sdp_cdc_2_silver.customers_silver_scd2",
    comment="SCD Type 2 Historical Customer Data",
    table_properties={"quality": "silver"},
)

dp.create_auto_cdc_flow(
    target="sdp_cdc_2_silver.customers_silver_scd2",
    source="sdp_cdc_1_bronze.bronze_customer_clean",
    keys=["customer_id"],
    apply_as_deletes=F.col("operation") == "DELETE",
    sequence_by=F.col("timestamp_datetime"),
    except_column_list=["timestamp", "_rescued_data", "operation"],
    stored_as_scd_type=2,
)
