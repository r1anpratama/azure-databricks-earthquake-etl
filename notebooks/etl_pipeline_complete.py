# Databricks notebook source
# DBTITLE 1, Earthquake ETL — Bronze → Silver → Gold (Community Edition)
# Zero SQL, zero temp views, zero metastore — pure DataFrame API
# Run on: Serverless Starter Warehouse
# Single notebook, single session, no Hive Metastore dependency

# COMMAND ----------
# MAGIC %md
# MAGIC ## Imports & Config

# COMMAND ----------
import pandas as pd
from pyspark.sql.functions import (
    current_timestamp, lit, col, year, month, date_format, desc,
    when, trim, row_number, count as _count, avg, max as _max,
    min as _min, round as _round
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, LongType
)
from pyspark.sql.window import Window

# COMMAND ----------
SOURCE_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&minmagnitude=2.5&orderby=time"

# COMMAND ----------
# MAGIC %md
# MAGIC ## BRONZE — Raw Ingestion

# COMMAND ----------
df_pd = pd.read_csv(SOURCE_URL)
print(f"Downloaded {len(df_pd):,} records from USGS")

if "time" in df_pd.columns:
    df_pd["time"] = pd.to_datetime(df_pd["time"], utc=True)
if "updated" in df_pd.columns:
    df_pd["updated"] = pd.to_datetime(df_pd["updated"], utc=True)

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

bronze = spark.createDataFrame(df_pd, schema=schema)
bronze = bronze.withColumn("ingestion_time", current_timestamp()).withColumn("source", lit("USGS_FDSN"))

print(f"Bronze: {bronze.count():,} rows, {len(bronze.columns)} columns")

# Bronze quality checks
q = []
q.append(("no_null_time", bronze.filter("time IS NULL").count() == 0))
q.append(("no_null_mag", bronze.filter("mag IS NULL").count() == 0))
q.append(("valid_lat", bronze.filter("latitude < -90 OR latitude > 90").count() == 0))
q.append(("valid_lon", bronze.filter("longitude < -180 OR longitude > 180").count() == 0))
q.append(("valid_mag_range", bronze.filter("mag < 0 OR mag > 10").count() == 0))
print("=== BRONZE QUALITY ===")
for n, p in q: print(f"  {n}: {'✅' if p else '❌'}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## SILVER — Cleaning & Enrichment (DataFrame API, no SQL)

# COMMAND ----------
print(f"Silver input: {bronze.count():,} rows")

# Deduplicate by event ID (keep latest)
window = Window.partitionBy("id").orderBy(desc("updated"))
silver = bronze.withColumn("rn", row_number().over(window)).filter("rn = 1").drop("rn")

# Drop null critical fields
silver = silver.na.drop(subset=["time", "latitude", "longitude", "mag"])

# Enrich
silver = silver \
    .withColumn("event_year", year(col("time"))) \
    .withColumn("event_month", month(col("time"))) \
    .withColumn("mag_category", when(col("mag") < 3, "minor")
        .when(col("mag") < 6, "moderate")
        .when(col("mag") < 7, "strong")
        .otherwise("major")) \
    .withColumn("place", trim(col("place")))

print(f"Silver output: {silver.count():,} rows")

# Silver quality
q = []
q.append(("no_duplicates", silver.groupBy("id").count().filter("count > 1").count() == 0))
q.append(("valid_lat", silver.filter("latitude < -90 OR latitude > 90").count() == 0))
q.append(("valid_lon", silver.filter("longitude < -180 OR longitude > 180").count() == 0))
q.append(("valid_mag", silver.filter("mag < 0 OR mag > 10").count() == 0))
print("=== SILVER QUALITY ===")
for n, p in q: print(f"  {n}: {'✅' if p else '❌'}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## GOLD — Business Analytics (DataFrame API)

# COMMAND ----------
print(f"Gold input: {silver.count():,} rows")

# 1. Daily stats
daily = silver.groupBy(date_format(col("time"), "yyyy-MM-dd").alias("date")) \
    .agg(
        _count("*").alias("event_count"),
        _round(avg("mag"), 2).alias("avg_magnitude"),
        _round(_max("mag"), 2).alias("max_magnitude"),
        _round(avg("depth"), 1).alias("avg_depth")
    ).orderBy(desc("date"))

print(f"Daily stats: {daily.count():,} rows")
display(daily.limit(20))

# 2. Monthly stats
monthly = silver.groupBy(year(col("time")).alias("year"), month(col("time")).alias("month")) \
    .agg(
        _count("*").alias("event_count"),
        _max("mag").alias("max_magnitude"),
        _round(avg("mag"), 2).alias("avg_magnitude"),
        _round(avg("depth"), 1).alias("avg_depth")
    ).orderBy(col("year").desc(), col("month").desc())

print(f"Monthly stats: {monthly.count():,} rows")
display(monthly)

# 3. Magnitude distribution
mag_dist = silver.groupBy(col("mag_category")) \
    .agg(
        _count("*").alias("count"),
        _min("mag").alias("min_mag"),
        _max("mag").alias("max_mag"),
        _round(avg("depth"), 1).alias("avg_depth")
    ).orderBy(desc("max_mag"))

print(f"Magnitude distribution: {mag_dist.count():,} categories")
display(mag_dist)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------
print(f"\n{'='*60}")
print(f"PIPELINE COMPLETE — Bronze → Silver → Gold")
print(f"{'='*60}")
print(f"Bronze records:      {bronze.count():,}")
print(f"Silver records:      {silver.count():,}")
print(f"Daily stats rows:    {daily.count():,}")
print(f"Monthly stats rows:  {monthly.count():,}")
print(f"Mag categories:      {mag_dist.count():,}")
print(f"\n--- Top 5 Most Active Days ---")
display(daily.orderBy(desc("event_count")).limit(5))
print(f"\n--- Top 5 Strongest Events ---")
display(silver.select("time", "place", "mag", "depth").orderBy(desc("mag")).limit(5))

print("\n✅ Pipeline completed successfully — zero SQL, zero metastore!")