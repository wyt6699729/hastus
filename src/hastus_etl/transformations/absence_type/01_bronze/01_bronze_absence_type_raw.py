from pyspark import pipelines as dp
import pyspark.sql.functions as F
from utils.convert_time import convert_to_vancouver_time
from utils.hastus_utils import get_hastus_source, read_hastus_xml

@dp.table(
    name="01_bronze.absence_type_raw",
    comment="Ingest absence_type_raw data from the volume",
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "false",
        "delta.feature.timestampNtz": "supported",
    },
)
def absence_type_raw():
    hastus_source = get_hastus_source(spark)
    return (
        read_hastus_xml(
            spark=spark,
            source_root=hastus_source,
            dataset="absence_type",
            row_tag="absence_type",
        )
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn(
            "dw_load_date",
            convert_to_vancouver_time("_ingest_ts", source_timezone="UTC"),
        )
        .withColumn(
            "dw_change_date",
            convert_to_vancouver_time("_ingest_ts", source_timezone="UTC"),
        )
        .drop("_ingest_ts")
        .select("*", "_metadata.file_name")
    )
