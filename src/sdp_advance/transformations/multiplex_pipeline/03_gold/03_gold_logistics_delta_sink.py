# ------------------------------------------------------
#       GOLD: LOGISTICS (streaming table + Iceberg props)
# ------------------------------------------------------

from pyspark import pipelines as dp


@dp.table(
    name="multiplex_3_gold.logistics_delta_sink",
    comment=(
        "Gold logistics events from silver; Delta UniForm as Iceberg v3 "
        "(DBR 18+). See delta.enableIcebergCompatV3 + universalFormat."
    ),
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.enableIcebergCompatV3": "true",
        "delta.universalFormat.enabledFormats": "iceberg",
    },
)
def logistics_delta_sink():
    return spark.readStream.table("multiplex_2_silver.logistics_silver")  # noqa: F821
