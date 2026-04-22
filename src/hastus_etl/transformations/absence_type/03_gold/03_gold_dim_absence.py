import pyspark.sql.functions as F
from pyspark import pipelines as dp
from pyspark.sql.window import Window


@dp.materialized_view(
    name="03_gold.dim_absence",
    comment="Dimension table for absence (integer surrogate by first-seen business time)",
    table_properties={
        "quality": "gold",
        "delta.feature.timestampNtz": "supported",
    },
)
def dim_absence():
    s = dp.read("02_silver.absence_type_scd1")
    first_seen = s.groupBy("absence_code").agg(
        F.min("absence_creation_datetime").alias("_first_seen"),
    )
    key_window = Window.orderBy(
        F.col("_first_seen").asc_nulls_last(),
        F.col("absence_code").asc_nulls_last(),
    )
    keys = first_seen.withColumn(
        "absence_type_key",
        F.row_number().over(key_window),
    )
    return (
        s.join(keys, on="absence_code", how="inner")
        .select(
            "absence_type_key",
            "absence_code",
            "absence_description",
        )
    )
