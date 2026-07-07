"""
Tests for Data Quality framework.
"""
import pytest
from pyspark.sql import SparkSession, Row
from src.quality import DataQuality, QualityCheck


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("test_quality") \
        .getOrCreate()


@pytest.fixture
def clean_df(spark):
    return spark.createDataFrame([
        Row(time="2024-06-01T00:00:00", latitude=-8.5, longitude=115.2,
            mag=4.5, depth=20.0),
        Row(time="2024-06-15T00:00:00", latitude=-7.3, longitude=109.8,
            mag=3.2, depth=10.0),
    ])


@pytest.fixture
def dirty_df(spark):
    return spark.createDataFrame([
        Row(time=None, latitude=-8.5, longitude=115.2,
            mag=4.5, depth=20.0),
        Row(time="2024-06-15T00:00:00", latitude=200.0, longitude=109.8,
            mag=3.2, depth=-5.0),
        Row(time="2024-06-20T00:00:00", latitude=-7.3, longitude=109.8,
            mag=None, depth=10.0),
    ])


class TestDataQuality:
    def test_not_null_pass(self, spark, clean_df):
        dq = DataQuality(spark)
        check = dq.check_not_null(clean_df, "test", "time")
        assert check.status == "PASS"

    def test_not_null_fail(self, spark, dirty_df):
        dq = DataQuality(spark)
        check = dq.check_not_null(dirty_df, "test", "time")
        assert check.status == "FAIL"

    def test_range_pass(self, spark, clean_df):
        dq = DataQuality(spark)
        check = dq.check_range(clean_df, "test", "latitude", -90, 90)
        assert check.status == "PASS"

    def test_range_fail(self, spark, dirty_df):
        dq = DataQuality(spark)
        check = dq.check_range(dirty_df, "test", "latitude", -90, 90)
        assert check.status == "FAIL"

    def test_schema_missing(self, spark, clean_df):
        dq = DataQuality(spark)
        check = dq.check_schema(clean_df, "test",
                                ["time", "latitude", "longitude", "mag", "depth", "extra_col"])
        assert check.status == "FAIL"
        assert "extra_col" in check.detail

    def test_to_row_includes_timestamp(self):
        check = QualityCheck("test_check", "bronze", "mag",
                             "not_null", "PASS", "All good")
        row = check.to_row()
        assert row["check_name"] == "test_check"
        assert row["status"] == "PASS"
        assert "checked_at" in row
