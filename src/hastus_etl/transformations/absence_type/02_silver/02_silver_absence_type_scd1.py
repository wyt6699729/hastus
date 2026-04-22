from pyspark import pipelines as dp

dp.create_streaming_table(
    name="02_silver.absence_type_scd1",
    comment="Absence type SCD1 from cleaned bronze",
    table_properties={
        "quality": "silver",
        "delta.feature.timestampNtz": "supported",
    },
)

dp.create_auto_cdc_flow(
    target="02_silver.absence_type_scd1",
    source="01_bronze.absence_type_cleaned",
    keys=["absence_code"],
    sequence_by="absence_creation_datetime",
    stored_as_scd_type=1,
)
