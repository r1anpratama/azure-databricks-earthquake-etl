"""
Bronze Layer — Raw ingestion from USGS earthquake API.
Minimal transformation, schema-on-read, stored as Delta.
"""

import logging
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp

logger = logging.getLogger(__name__)


class BronzeIngestion:
    """Ingest raw USGS CSV with schema inference and minimal type casting."""

    def __init__(self, source_url: str, output_path: str):
        self.source_url = source_url
        self.output_path = output_path

    def extract(self) -> DataFrame:
        """Pull CSV from USGS API into Spark DataFrame."""
        spark = SparkSession.active()
        try:
            df = spark.read.format("csv") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load(self.source_url)
            logger.info("Extracted %s raw records", df.count())
            return df
        except Exception as e:
            logger.error("Extraction failed: %s", e)
            raise

    def validate_raw(self, df: DataFrame) -> None:
        """Assert critical columns exist."""
        required = {"time", "latitude", "longitude", "mag", "depth", "place"}
        actual = set(df.columns)
        missing = required - actual
        if missing:
            raise ValueError(
                f"Bronze schema missing columns: {missing}. "
                f"Available: {actual}"
            )

    def load(self, df: DataFrame) -> str:
        """Write raw data as Delta table (Bronze)."""
        df_with_meta = df \
            .withColumn("_ingested_at", current_timestamp()) \
            .withColumn("_source", col("_source").otherwise("usgs"))

        df_with_meta.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(self.output_path)

        spark = SparkSession.active()
        rows = spark.read.format("delta").load(self.output_path).count()
        logger.info("Bronze load complete: %s rows at %s", rows, self.output_path)
        return self.output_path

    def summarize(self, df: DataFrame) -> dict[str, Any]:
        """Print quick stats."""
        stats = {
            "total_raw": df.count(),
            "columns": len(df.columns),
            "date_range": df.agg(
                df["time"].cast("date").min().alias("from"),
                df["time"].cast("date").max().alias("to")
            ).collect()[0].asDict(),
            "source": self.source_url.split("/")[2]
        }
        print(stats)
        return stats
