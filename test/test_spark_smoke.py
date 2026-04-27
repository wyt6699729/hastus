def test_spark_session_smoke(spark):
    assert spark.range(1).count() == 1
