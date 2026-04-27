import importlib.util
import sys
import types
from datetime import date, datetime
from pathlib import Path
from unittest.mock import Mock

from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def _load_dq_transformation_module():
    module_path = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "sdp_advance"
        / "transformations"
        / "dq_pipeline"
        / "02_silver"
        / "02_silver_transformation.py"
    )

    pipelines_stub = types.ModuleType("pyspark.pipelines")
    pipelines_stub.create_streaming_table = Mock(return_value=None)
    pipelines_stub.expect = lambda *args, **kwargs: (lambda fn: fn)
    pipelines_stub.temporary_view = lambda *args, **kwargs: (lambda fn: fn)
    pipelines_stub.append_flow = lambda *args, **kwargs: (lambda fn: fn)
    sys.modules["pyspark.pipelines"] = pipelines_stub

    spec = importlib.util.spec_from_file_location("dq_transformation_test_module", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_apply_inverse_logic_flow_flags_bad_records(spark):
    module = _load_dq_transformation_module()

    schema = StructType(
        [
            StructField("subsidiary_id", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("order_timestamp", TimestampType(), True),
            StructField("order_date", DateType(), True),
            StructField("customer_id", StringType(), True),
            StructField("region", StringType(), True),
            StructField("country", StringType(), True),
            StructField("city", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("sku", StringType(), True),
            StructField("category", StringType(), True),
            StructField("qty", StringType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("discount_pct", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("coupon_code", StringType(), True),
            StructField("order_status", StringType(), True),
            StructField("shipping_cost", DoubleType(), True),
            StructField("source_file", StringType(), True),
        ]
    )

    source_df = spark.createDataFrame(
        [
            (
                "sub-1",
                "ord-1",
                datetime(2026, 1, 1, 12, 0, 0),
                date(2025, 12, 31),
                "cust-1",
                "bc",
                "ca",
                "vancouver",
                "online",
                "sku-1",
                "cat",
                "1",
                10.0,
                20.0,
                8.0,
                None,
                "completed",
                5.0,
                "f1.csv",
            ),
            (
                None,
                "ord-2",
                datetime(2026, 1, 1, 12, 0, 0),
                date(2020, 1, 1),
                None,
                "bc",
                "ca",
                "vancouver",
                "online",
                None,
                "cat",
                "1",
                10.0,
                120.0,
                8.0,
                None,
                "completed",
                -1.0,
                "f2.csv",
            ),
        ],
        schema=schema,
    )

    class _ReadStreamStub:
        def table(self, _name):
            return source_df

    class _SparkStub:
        readStream = _ReadStreamStub()

    module.spark = _SparkStub()
    out = module.apply_inverse_logic_flow().orderBy("order_id").collect()

    assert out[0]["order_id"] == "ord-1"
    assert out[0]["is_quarantined"] is False
    assert out[0]["quarantine_reason"] == ""

    assert out[1]["order_id"] == "ord-2"
    assert out[1]["is_quarantined"] is True
    assert "Missing subsidiary_id" in out[1]["quarantine_reason"]
    assert "Invalid discount_pct (must be 0-100)" in out[1]["quarantine_reason"]
