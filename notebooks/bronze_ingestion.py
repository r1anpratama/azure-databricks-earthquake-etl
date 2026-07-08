# Databricks notebook source
# DBTITLE 1, Bronze Layer — Raw Ingestion (Community Edition)
# Self-contained: no src.* imports, no DBFS, uses UC Volume for storage
# Run on: Serverless Starter Warehouse

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bronze Layer
# MAGIC Ingest USGS earthquake CSV → raw Delta table in UC Volume
# MAGIC - Schema enforcement on load
# MAGIC - Data quality checks
# MAGIC - No DBFS dependency (Community Edition compatible)

# COMMAND ----------
import pandas as pd
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, LongType
)

# COMMAND ----------
# Config (Community Edition — no config.yaml access)
SOURCE_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&minmagnitude=2.5&orderby=time"
VOLUME_PATH = "/Volumes/main/default/earthquake_analytics"

# Create volume directory via SQL (UC) - try multiple catalog options
try:
    spark.sql("CREATE VOLUME IF NOT EXISTS hive_metastore.default.earthquake_analytics")
    VOLUME_PATH = "/Volumes/hive_metastore/default/earthquake_analytics"
except:
    try:
        spark.sql("CREATE VOLUME IF NOT EXISTS main.default.earthquake_analytics")
        VOLUME_PATH = "/Volumes/main/default/earthquake_analytics"
    except:
        # Fallback: use workspace files (always available)
        VOLUME_PATH = "/Workspace/Pipelines/earthquake_analytics"
        import os
        for p in [
            f"{VOLUME_PATH}/bronze/events",
            f"{VOLUME_PATH}/silver/events",
            f"{VOLUME_PATH}/gold/daily_stats",
            f"{VOLUME_PATH}/gold/monthly_stats", 
            f"{VOLUME_PATH}/gold/magnitude_distribution"
        ]:
            os.makedirs(p, exist_ok=True)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Extract: Download CSV via pandas

# COMMAND ----------
df_pd = pd.read_csv(SOURCE_URL)
print(f"Downloaded {len(df_pd):,} records from USGS")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Schema enforcement

# COMMAND ----------
schema = StructType([
    StructField("time", TimestampType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("depth", DoubleType(), True),
    StructField("mag", DoubleType(), True),
    StructField("magType", StringType(), True),
    StructField("nst", LongType(), True),
    StructField("gap", DoubleType(), True),
    StructField("dmin", DoubleType(), True),
    StructField("rms", DoubleType(), True),
    StructField("net", StringType(), True),
    StructField("id", StringType(), True),
    StructField("updated", TimestampType(), True),
    StructField("place", StringType(), True),
    StructField("type", StringType(), True),
    StructField("horizontalError", DoubleType(), True),
    StructField("depthError", DoubleType(), True),
    StructField("magError", DoubleType(), True),
    StructField("magNst", LongType(), True),
    StructField("status", StringType(), True),
    StructField("locationSource", StringType(), True),
    StructField("magSource", StringType(), True),
])

df = spark.createDataFrame(df_pd, schema=schema)

# Add ingestion metadata
df = df.withColumn("ingestion_time", current_timestamp()) \
       .withColumn("source", lit("USGS_FDSN"))

print(f"Bronze DataFrame: {df.count():,} rows, {len(df.columns)} columns")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Data Quality Checks (inline, no src.quality import)

# COMMAND ----------
quality_results = []

# Check 1: no null time
null_time = df.filter("time IS NULL").count()
quality_results.append(("no_null_time", null_time == 0, null_time))

# Check 2: no null magnitude
null_mag = df.filter("mag IS NULL").count()
quality_results.append(("no_null_magnitude", null_mag == 0, null_mag))

# Check 3: valid latitude range
bad_lat = df.filter("latitude < -90 OR latitude > 90").count()
quality_results.append(("valid_latitude", bad_lat == 0, bad_lat))

# Check 4: valid longitude range
bad_lon = df.filter("longitude < -180 OR longitude > 180").count()
quality_results.append(("valid_longitude", bad_lon == 0, bad_lon))

# Check 5: valid magnitude range (0-10)
bad_mag = df.filter("mag < 0 OR mag > 10").count()
quality_results.append(("valid_magnitude_range", bad_mag == 0, bad_mag))

print("=== BRONZE DATA QUALITY REPORT ===")
print(f"{'Check':<30} {'Status':<10} {'Failures':<10}")
print("-" * 50)
for name, passed, failures in quality_results:
    status = "✅ PASS" if passed else "❌ FAIL"
    print(f"{name:<30} {status:<10} {failures:<10}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Load: Write to Delta table in UC Volume

# COMMAND ----------
bronze_path = f"{VOLUME_PATH}/bronze/events"

df.write \
  .format("delta") \
  .mode("overwrite") \
  .save(bronze_path)

print(f"✅ Bronze layer written to: {bronze_path}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------
print(f"\n{'='*50}")
print(f"BRONZE LAYER SUMMARY")
print(f"{'='*50}")
print(f"Records loaded:    {df.count():,}")
print(f"Columns:           {len(df.columns)}")
print(f"Storage:           Delta at {bronze_path}")
print(f"Quality checks:    {sum(1 for _,p,_ in quality_results if p)}/{len(quality_results)} passed")

display(df.limit(100))
