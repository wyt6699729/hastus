import importlib.util
import sys
import types
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock

from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def _load_flow_ingestion_module():
    module_path = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "sdp_advance"
        / "transformations"
        / "ingest_multiple_flows"
        / "flow_ingestion.py"
    )

    pipelines_stub = types.ModuleType("pyspark.pipelines")
    pipelines_stub.create_streaming_table = Mock(return_value=None)
    pipelines_stub.append_flow = lambda **kwargs: (lambda fn: fn)
    sys.modules["pyspark.pipelines"] = pipelines_stub

    spec = importlib.util.spec_from_file_location("flow_ingestion_test_module", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_transform_raw_to_bronze_aligns_schema_and_metadata(spark):
    module = _load_flow_ingestion_module()

    schema = StructType(
        [
            StructField("subsidiary_id", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("order_timestamp", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("region", StringType(), True),
            StructField("country", StringType(), True),
            StructField("city", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("sku", StringType(), True),
            StructField("category", StringType(), True),
            StructField("qty", StringType(), True),
            StructField("unit_price", StringType(), True),
            StructField("discount_pct", StringType(), True),
            StructField("coupon_code", StringType(), True),
            StructField("total_amount", StringType(), True),
            StructField("order_date", StringType(), True),
            StructField(
                "_metadata",
                StructType(
                    [
                        StructField("file_name", StringType(), True),
                        StructField("file_modification_time", TimestampType(), True),
                    ]
                ),
                True,
            ),
        ]
    )

    raw = spark.createDataFrame(
        [
            (
                "sub-01",
                "ord-01",
                "2026-01-01 10:00:00",
                "cust-01",
                "bc",
                "ca",
                "vancouver",
                "online",
                "sku-01",
                "shoes",
                "2",
                "100.5",
                "10",
                "NEW10",
                "181.0",
                "2026-01-01",
                ("orders_01.csv", datetime(2026, 1, 1, 10, 1, 0)),
            )
        ],
        schema=schema,
    )

    out = module._transform_raw_to_bronze(raw)
    row = out.first()

    assert out.columns == [
        "subsidiary_id",
        "order_id",
        "order_timestamp",
        "customer_id",
        "region",
        "country",
        "city",
        "channel",
        "sku",
        "category",
        "qty",
        "unit_price",
        "discount_pct",
        "coupon_code",
        "total_amount",
        "order_date",
        "source_file",
        "file_mod_time",
    ]
    assert row["subsidiary_id"] == "sub-01"
    assert row["source_file"] == "orders_01.csv"
    assert str(out.schema["qty"].dataType) == "StringType()"
