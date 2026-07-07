"""
Gold Layer — Business-level aggregations.
Reads Silver Delta → produces analytics-ready Delta tables.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    count, avg, max as spark_max, min as spark_min,
    round as spark_round, col, sum, when, date_format
)
import logging

logger = logging.getLogger(__name__)


class GoldAggregator:
    """Build daily, monthly, and categorical aggregations."""

    def __init__(self, source_path: str, output_path: str):
        self.source_path = source_path
        self.output_path = output_path

    def build_aggregations(self, df: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:
        """Produce daily, monthly, and magnitude-distribution tables."""
        daily = self._daily_agg(df)
        monthly = self._monthly_agg(df)
        mag_dist = self._mag_distribution(df)
        return daily, monthly, mag_dist

    def _daily_agg(self, df: DataFrame) -> DataFrame:
        return df.groupBy(
            col("year"), col("month"),
            date_format(col("time"), "yyyy-MM-dd").alias("date")
        ).agg(
            count("*").alias("event_count"),
            spark_round(avg("mag"), 2).alias("avg_magnitude"),
            spark_max("mag").alias("max_magnitude"),
            spark_round(avg("depth_km"), 1).alias("avg_depth_km"),
            spark_round(avg("gap"), 0).alias("avg_gap"),
            count(when(col("mag") >= 5.0, 1)).alias("major_events")
        ).orderBy("date")

    def _monthly_agg(self, df: DataFrame) -> DataFrame:
        return df.groupBy("year", "month").agg(
            count("*").alias("event_count"),
            spark_round(avg("mag"), 2).alias("avg_magnitude"),
            spark_max("mag").alias("max_magnitude"),
            spark_min("mag").alias("min_magnitude"),
            spark_round(avg("depth_km"), 1).alias("avg_depth_km"),
            spark_round(avg("rms"), 3).alias("avg_rms"),
            count(when(col("mag") >= 5.0, 1)).alias("major_events"),
            countDistinctApprox("region").alias("regions_active")
        ).orderBy("year", "month")

    def _mag_distribution(self, df: DataFrame) -> DataFrame:
        return df.groupBy("mag_class", "region").agg(
            count("*").alias("count"),
            spark_round(avg("depth_km"), 1).alias("avg_depth"),
            spark_round(avg("mag"), 2).alias("avg_magnitude")
        ).orderBy(col("count").desc())

    def load_all(self, daily: DataFrame, monthly: DataFrame,
                 mag_dist: DataFrame) -> dict:
        """Write each aggregation to its own Delta path."""
        paths = {}

        for name, df in [
            ("daily_stats", daily),
            ("monthly_stats", monthly),
            ("magnitude_distribution", mag_dist),
        ]:
            path = f"{self.output_path}/{name}"
            df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .save(path)
            paths[name] = path
            count = SparkSession.active().read.format("delta").load(path).count()
            logger.info("Gold %s: %s rows at %s", name, count, path)

        return paths
