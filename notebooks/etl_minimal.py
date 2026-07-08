# Databricks notebook source
# DBTITLE 1,Earthquake ETL - Community Edition (Single Notebook, Zero Hive Metastore)
# Run on: Serverless Starter Warehouse
# No DBFS, no Volume, no global_temp, no Delta write, no metastore access

# COMMAND ----------
import pandas as pd
from pyspark.sql.functions import (
    current_timestamp, lit, col, year, month, date_format, desc,
    when, trim, row_number, count as _count, avg, max as _max,
    min as _min, round as _round
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, LongType
from pyspark.sql.window import Window

# COMMAND ----------
SOURCE_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&minmagnitude=2.5&orderby=time"

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. BRONZE - Ingest & Validate

# COMMAND ----------
df_pd = pd.read_csv(SOURCE_URL)
print(f"Downloaded {len(df_pd):,} records from USGS")

# Parse timestamps in pandas (avoid Arrow conversion error)
for c in ("time", "updated"):
    if c in df_pd.columns:
        df_pd[c] = pd.to_datetime(df_pd[c], utc=True)

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

bronze = spark.createDataFrame(df_pd, schema=schema) \
    .withColumn("ingestion_time", current_timestamp()) \
    .withColumn("source", lit("USGS_FDSN"))

print(f"Bronze: {bronze.count():,} rows, {len(bronze.columns)} cols")

# Quality checks (in-memory, no metastore)
checks = {
    "no_null_time": bronze.filter("time IS NULL").count() == 0,
    "no_null_mag": bronze.filter("mag IS NULL").count() == 0,
    "valid_lat": bronze.filter("latitude < -90 OR latitude > 90").count() == 0,
    "valid_lon": bronze.filter("longitude < -180 OR longitude > 180").count() == 0,
    "valid_mag": bronze.filter("mag < 0 OR mag > 10").count() == 0,
}
print("=== BRONZE QUALITY ===")
for k, v in checks.items():
    print(f"  {k}: {'✅' if v else '❌'}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. SILVER - Clean & Enrich (in-memory only)

# COMMAND ----------
silver = bronze

# Deduplicate by event ID (keep latest)
win = Window.partitionBy("id").orderBy(desc("updated"))
silver = silver.withColumn("rn", row_number().over(win)).filter("rn = 1").drop("rn")

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

print(f"Silver: {silver.count():,} rows")

checks = {
    "no_duplicates": silver.groupBy("id").count().filter("count > 1").count() == 0,
    "valid_lat": silver.filter("latitude < -90 OR latitude > 90").count() == 0,
    "valid_lon": silver.filter("longitude < -180 OR longitude > 180").count() == 0,
    "valid_mag": silver.filter("mag < 0 OR mag > 10").count() == 0,
}
print("=== SILVER QUALITY ===")
for k, v in checks.items():
    print(f"  {k}: {'✅' if v else '❌'}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. GOLD - Aggregations (display only, no write)

# COMMAND ----------
# Daily stats
daily = silver.groupBy(date_format(col("time"), "yyyy-MM-dd").alias("date")) \
    .agg(
        _count("*").alias("event_count"),
        _round(avg("mag"), 2).alias("avg_magnitude"),
        _round(_max("mag"), 2).alias("max_magnitude"),
        _round(avg("depth"), 1).alias("avg_depth")
    ).orderBy(desc("date"))

print(f"Daily stats: {daily.count():,} rows")
display(daily.limit(20))

# COMMAND ----------
# Monthly stats
monthly = silver.groupBy(year(col("time")).alias("year"), month(col("time")).alias("month")) \
    .agg(
        _count("*").alias("event_count"),
        _max("mag").alias("max_magnitude"),
        _round(avg("mag"), 2).alias("avg_magnitude"),
        _round(avg("depth"), 1).alias("avg_depth")
    ).orderBy(col("year").desc(), col("month").desc())

print(f"Monthly stats: {monthly.count():,} rows")
display(monthly)

# COMMAND ----------
# Magnitude distribution
mag_dist = silver.groupBy(col("mag_category")) \
    .agg(
        _count("*").alias("count"),
        _min("mag").alias("min_mag"),
        _max("mag").alias("max_mag"),
        _round(avg("depth"), 1).alias("avg_depth")
    ).orderBy(desc("max_mag"))

print(f"Mag categories: {mag_dist.count():,}")
display(mag_dist)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------
print(f"\n{'='*50}")
print("PIPELINE COMPLETE")
print(f"{'='*50}")
print(f"Bronze:      {bronze.count():,} rows")
print(f"Silver:      {silver.count():,} rows")
print(f"Daily stats: {daily.count():,} rows")
print(f"Monthly:     {monthly.count():,} rows")
print(f"Mag dist:    {mag_dist.count():,} categories")

print("\n--- Top 5 Most Active Days ---")
display(daily.orderBy(desc("event_count")).limit(5))

print("\n--- Top 5 Strongest Events ---")
display(silver.select("time", "place", "mag", "depth").orderBy(desc("mag")).limit(5))

print("\n✅ All done - zero metastore, zero file I/O, pure in-memory Spark")