from pyspark import pipelines as dp
import pyspark.sql.functions as F


@dp.table(
    name="01_bronze.sdp_advance_meta",
    comment="SDP Advance scaffold table (replace with real sources)",
    table_properties={
        "quality": "bronze",
        "delta.feature.timestampNtz": "supported",
    },
)
def sdp_advance_meta():
    return spark.createDataFrame(
        [("sdp_advance",)],
        ["project_name"],
    ).withColumn("created_at", F.current_timestamp())
