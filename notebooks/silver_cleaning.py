# Databricks notebook source
# DBTITLE 1, Silver Layer — Cleaning & Validation (Community Edition)
# Self-contained: reads Bronze temp view, creates Silver temp view
# Run AFTER bronze_ingestion.py
# Run on: Serverless Starter Warehouse

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver Layer
# MAGIC Read Bronze temp view → Deduplicate → Enrich → Silver temp view
# MAGIC - Deduplicate by event ID
# MAGIC - Enrich with derived columns (year, month, mag_category)
# MAGIC - No persistent storage needed

# COMMAND ----------
from pyspark.sql.functions import (
    year, month, col, when, trim, desc, row_number
)
from pyspark.sql.window import Window

# COMMAND ----------
# MAGIC %md
# MAGIC ## Read Bronze (from global temp view)

# COMMAND ----------
df_bronze = spark.sql("SELECT * FROM global_temp.bronze_events")
print(f"Read {df_bronze.count():,} rows from Bronze global temp view")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Transform: Clean → Enrich

# COMMAND ----------
# Deduplicate by event ID (keep latest)
window = Window.partitionBy("id").orderBy(desc("updated"))
df_dedup = df_bronze.withColumn("rn", row_number().over(window)) \
    .filter("rn = 1") \
    .drop("rn")

# Drop rows with null critical fields
df_clean = df_dedup.na.drop(subset=["time", "latitude", "longitude", "mag"])

# Enrich: add year, month, mag_category
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
# MAGIC ## Create Temp View (in-memory)

# Create Global Temp View (shared across notebooks)
df_silver.createOrReplaceGlobalTempView("silver_events")
print("✅ Silver global temp view created: global_temp.silver_events")

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
print(f"Storage:            In-memory temp view (silver_events)")

display(df_silver.limit(100))