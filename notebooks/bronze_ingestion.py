# Databricks notebook source
# DBTITLE 1, Bronze Layer — Raw Ingestion (Community Edition)
# Self-contained: no src.* imports, no DBFS, no Volume write needed
# Run on: Serverless Starter Warehouse
# Output: in-memory temp view "bronze_events" + display

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bronze Layer
# MAGIC Ingest USGS earthquake CSV → Spark DataFrame → temp view
# MAGIC - Schema enforcement on load
# MAGIC - Data quality checks
# MAGIC - No persistent storage needed (Community Edition compatible)

# COMMAND ----------
import pandas as pd
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, LongType
)

# COMMAND ----------
# Config (Community Edition)
SOURCE_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&minmagnitude=2.5&orderby=time"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Extract: Download CSV via pandas

# COMMAND ----------
df_pd = pd.read_csv(SOURCE_URL)
print(f"Downloaded {len(df_pd):,} records from USGS")

# Parse timestamp columns in pandas first (avoid Arrow conversion error)
if "time" in df_pd.columns:
    df_pd["time"] = pd.to_datetime(df_pd["time"], utc=True)
if "updated" in df_pd.columns:
    df_pd["updated"] = pd.to_datetime(df_pd["updated"], utc=True)

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
# MAGIC ## Data Quality Checks (inline)

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
# MAGIC ## Create Temp View (in-memory, no file write)

# Create Global Temp View (shared across notebooks on same warehouse)
df.createOrReplaceGlobalTempView("bronze_events")
print("✅ Bronze global temp view created: global_temp.bronze_events")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------
print(f"\n{'='*50}")
print(f"BRONZE LAYER SUMMARY")
print(f"{'='*50}")
print(f"Records loaded:    {df.count():,}")
print(f"Columns:           {len(df.columns)}")
print(f"Storage:           In-memory temp view (bronze_events)")
print(f"Quality checks:    {sum(1 for _,p,_ in quality_results if p)}/{len(quality_results)} passed")

display(df.limit(100))