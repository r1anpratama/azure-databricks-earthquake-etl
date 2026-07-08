# Databricks notebook source
# DBTITLE 1, Silver Layer — Cleaning & Validation (Community Edition)
# Self-contained: reads Bronze Delta table from Volume, writes cleaned Silver layer
# Run AFTER bronze_ingestion.py
# Run on: Serverless Starter Warehouse

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver Layer
# MAGIC Read Bronze → Deduplicate → Enrich → Write Silver Delta
# MAGIC - Deduplicate by event ID
# MAGIC - Enrich with derived columns (year, month, mag_category)
# MAGIC - Partition by year + month

# COMMAND ----------
from pyspark.sql.functions import (
    year, month, col, when, trim, to_timestamp,
    date_format, count as _count, desc, row_number
)
from pyspark.sql.window import Window

# COMMAND ----------
# Config (Community Edition) - try UC Volume, fallback to Workspace
try:
    spark.sql("DESCRIBE VOLUME hive_metastore.default.earthquake_analytics")
    VOLUME_PATH = "/Volumes/hive_metastore/default/earthquake_analytics"
except:
    try:
        spark.sql("DESCRIBE VOLUME main.default.earthquake_analytics")
        VOLUME_PATH = "/Volumes/main/default/earthquake_analytics"
    except:
        VOLUME_PATH = "/Workspace/Pipelines/earthquake_analytics"

bronze_path = f"{VOLUME_PATH}/bronze/events"
silver_path = f"{VOLUME_PATH}/silver/events"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Read Bronze

# COMMAND ----------
df_bronze = spark.read.format("delta").load(bronze_path)
print(f"Read {df_bronze.count():,} rows from Bronze")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Transform: Clean → Enrich → Partition

# COMMAND ----------
# Deduplicate by event ID (keep latest)
window = Window.partitionBy("id").orderBy(desc("updated"))
df_dedup = df_bronze.withColumn("rn", row_number().over(window)) \
    .filter("rn = 1") \
    .drop("rn")

# Drop rows with null critical fields
df_clean = df_dedup.na.drop(subset=["time", "latitude", "longitude", "mag"])

# Enrich: add year, month for partitioning
df_silver = df_clean \
    .withColumn("event_year", year(col("time"))) \
    .withColumn("event_month", month(col("time"))) \
    .withColumn("mag_category", when(col("mag") < 3, "minor")
        .when(col("mag") < 6, "moderate")
        .when(col("mag") < 7, "strong")
        .otherwise("major")) \
    .withColumn("place", trim(col("place")))

print(f"Silver after cleaning: {df_silver.count():,} rows")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Quality Checks

# COMMAND ----------
quality_results = []
quality_results.append(("no_duplicates", df_silver.groupBy("id").count().filter("count > 1").count() == 0))
quality_results.append(("valid_lat", df_silver.filter("latitude < -90 OR latitude > 90").count() == 0))
quality_results.append(("valid_lon", df_silver.filter("longitude < -180 OR longitude > 180").count() == 0))
quality_results.append(("valid_mag", df_silver.filter("mag < 0 OR mag > 10").count() == 0))

print("=== SILVER DATA QUALITY REPORT ===")
for name, passed in quality_results:
    print(f"  {name}: {'✅ PASS' if passed else '❌ FAIL'}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Load: Write to Delta (partitioned by year+month)

# COMMAND ----------
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("event_year", "event_month") \
    .save(silver_path)

print(f"✅ Silver layer written to: {silver_path}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------
print(f"\n{'='*50}")
print(f"SILVER LAYER SUMMARY")
print(f"{'='*50}")
print(f"Bronze input:      {df_bronze.count():,}")
print(f"Silver output:      {df_silver.count():,}")
print(f"Rows dropped:       {df_bronze.count() - df_silver.count():,}")
print(f"Storage:            Delta at {silver_path}")

display(df_silver.limit(100))
