"""
Silver Layer — Cleaning, deduplication, enrichment, validation.
Reads Bronze Delta → produces clean Silver Delta.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, year, month, when, regexp_extract,
    row_number, count, round as spark_round, lit, coalesce
)
from pyspark.sql.window import Window
import logging

logger = logging.getLogger(__name__)


class SilverTransformer:
    """Clean, deduplicate, enrich raw earthquake data."""

    def __init__(self, source_path: str, output_path: str):
        self.source_path = source_path
        self.output_path = output_path

    def transform(self, df: DataFrame) -> DataFrame:
        """Apply cleaning pipeline."""
        df = self._cast_types(df)
        df = self._add_time_partitions(df)
        df = self._classify_magnitude(df)
        df = self._extract_region(df)
        df = self._filter_valid(df)
        df = self._deduplicate(df)
        df = self._add_silver_metadata(df)
        return df

    def _cast_types(self, df: DataFrame) -> DataFrame:
        return df \
            .withColumn("time", to_timestamp(col("time"))) \
            .withColumn("depth_km", col("depth").cast("double")) \
            .withColumn("mag", col("mag").cast("double")) \
            .withColumn("latitude", col("latitude").cast("double")) \
            .withColumn("longitude", col("longitude").cast("double")) \
            .withColumn("gap", col("gap").cast("double")) \
            .withColumn("dmin", col("dmin").cast("double")) \
            .withColumn("rms", col("rms").cast("double")) \
            .withColumn("nst", col("nst").cast("int"))

    def _add_time_partitions(self, df: DataFrame) -> DataFrame:
        return df \
            .withColumn("year", year(col("time"))) \
            .withColumn("month", month(col("time")))

    def _classify_magnitude(self, df: DataFrame) -> DataFrame:
        return df.withColumn("mag_class",
            when(col("mag") >= 5.0, "MAJOR")
            .when(col("mag") >= 4.0, "MODERATE")
            .when(col("mag") >= 3.0, "LIGHT")
            .otherwise("MINOR"))

    def _extract_region(self, df: DataFrame) -> DataFrame:
        return df.withColumn("region",
            regexp_extract(col("place"), r",\s*(.+)$", 1))

    def _filter_valid(self, df: DataFrame) -> DataFrame:
        """Drop rows with null critical fields."""
        before = df.count()
        df = df.filter(
            col("time").isNotNull() &
            col("mag").isNotNull() &
            col("latitude").isNotNull() &
            col("longitude").isNotNull() &
            col("mag").between(0, 10) &
            col("latitude").between(-90, 90) &
            col("longitude").between(-180, 180)
        )
        after = df.count()
        dropped = before - after
        if dropped:
            logger.warning("Silver dropped %s invalid rows", dropped)
        return df

    def _deduplicate(self, df: DataFrame) -> DataFrame:
        """Remove duplicates based on event identity."""
        window = Window.partitionBy("time", "latitude", "longitude", "mag") \
                       .orderBy(col("rms").asc_nulls_last(), col("nst").desc_nulls_last())
        return df.withColumn("_rn", row_number().over(window)) \
                 .filter(col("_rn") == 1) \
                 .drop("_rn")

    def _add_silver_metadata(self, df: DataFrame) -> DataFrame:
        from pyspark.sql.functions import current_timestamp
        return df.withColumn("_silver_processed_at", current_timestamp())

    def load(self, df: DataFrame) -> None:
        """Write clean Delta partitioned by year/month."""
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("year", "month") \
            .option("overwriteSchema", "true") \
            .save(self.output_path)

        spark = SparkSession.active()
        count = spark.read.format("delta").load(self.output_path).count()
        logger.info("Silver load complete: %s rows at %s", count, self.output_path)

    def get_schema(self) -> str:
        """Return expected output schema for docs."""
        return """
    time, latitude, longitude, depth_km, mag, mag_class,
    place, region, type, status, gap, dmin, rms, nst,
    year, month, _silver_processed_at
    """
