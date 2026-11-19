def test_spark_session_starts(spark):
    df = spark.range(3)
    assert df.count() == 3