"""
Tests for Silver transformer layer.
"""
import pytest
from pyspark.sql import SparkSession, Row
from src.silver import SilverTransformer


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("test_silver") \
        .getOrCreate()


@pytest.fixture
def raw_df(spark):
    return spark.createDataFrame([
        Row(time="2024-01-15T10:30:00", latitude=-8.5, longitude=115.2,
            mag=4.5, depth=20.0, place="25 km S of Denpasar, Indonesia",
            type="earthquake", status="reviewed", gap=120.0, dmin=1.5,
            rms=0.8, nst=45),
        Row(time="2024-01-16T11:00:00", latitude=-7.3, longitude=109.8,
            mag=3.2, depth=10.0, place="15 km SW of Purwokerto, Indonesia",
            type="earthquake", status="automatic", gap=150.0, dmin=2.1,
            rms=0.5, nst=20),
        # Duplicate (same time, lat, lon, mag)
        Row(time="2024-01-16T11:00:00", latitude=-7.3, longitude=109.8,
            mag=3.2, depth=10.0, place="15 km SW of Purwokerto, Indonesia",
            type="earthquake", status="automatic", gap=150.0, dmin=2.1,
            rms=0.5, nst=20),
        # Invalid row (null magnitude)
        Row(time="2024-01-17T12:00:00", latitude=-6.5, longitude=110.0,
            mag=None, depth=30.0, place="Java Sea",
            type="earthquake", status="reviewed", gap=90.0, dmin=1.0,
            rms=0.3, nst=10),
    ])


class TestSilverTransformer:
    def test_transform_deduplicates(self, raw_df):
        st = SilverTransformer("/tmp/bronze", "/tmp/silver")
        result = st.transform(raw_df)
        # Should remove the duplicate AND the null-mag row
        assert result.count() == 2

    def test_magnitude_classification(self, raw_df):
        st = SilverTransformer("/tmp/bronze", "/tmp/silver")
        result = st.transform(raw_df)
        classes = [r.mag_class for r in result.select("mag_class").collect()]
        assert "MODERATE" in classes  # mag=4.5
        assert "LIGHT" in classes  # mag=3.2

    def test_region_extraction(self, raw_df):
        st = SilverTransformer("/tmp/bronze", "/tmp/silver")
        result = st.transform(raw_df)
        regions = result.select("region").distinct().collect()
        assert any("Indonesia" in r.region for r in regions)

    def test_time_partitions(self, raw_df):
        st = SilverTransformer("/tmp/bronze", "/tmp/silver")
        result = st.transform(raw_df)
        row = result.select("year", "month").first()
        assert row.year == 2024
        assert row.month == 1
