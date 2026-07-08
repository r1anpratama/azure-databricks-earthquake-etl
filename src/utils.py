"""
Shared utilities: config loader, schema definitions, metrics helpers.
"""

import logging
from typing import Any

import yaml
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def load_config(path: str = "config/config.yaml") -> dict[str, Any]:
    """Load YAML configuration."""
    with open(path) as f:
        cfg = yaml.safe_load(f)
    logger.info("Config loaded from %s", path)
    return cfg  # type: ignore[no-any-return]


def create_catalog(spark: SparkSession, catalog_name: str) -> None:
    """Create Unity Catalog catalog if not exists (Databricks only)."""
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
        spark.sql(f"USE CATALOG {catalog_name}")
        logger.info("Catalog %s ready", catalog_name)
    except Exception:
        logger.warning(
            "Could not create catalog %s — not a Databricks UC environment?",
            catalog_name
        )


BRONZE_SCHEMA = [
    "time", "latitude", "longitude", "mag", "depth", "place",
    "type", "status", "gap", "dmin", "rms", "nst",
    "magType", "net", "id", "updated", "horizontalError",
    "depthError", "magNst", "locationSource", "magSource",
    "_ingested_at", "_source"
]

SILVER_SCHEMA = [
    "time", "latitude", "longitude", "depth_km", "mag",
    "mag_class", "place", "region", "type", "status",
    "gap", "dmin", "rms", "nst", "year", "month",
    "_silver_processed_at"
]

GOLD_METRICS = [
    "event_count", "avg_magnitude", "max_magnitude",
    "avg_depth_km", "avg_gap", "major_events"
]


def human_size(num_rows: int) -> str:
    """Format row count for display."""
    if num_rows >= 1_000_000:
        return f"{num_rows / 1_000_000:.1f}M"
    if num_rows >= 1_000:
        return f"{num_rows / 1_000:.1f}K"
    return str(num_rows)
