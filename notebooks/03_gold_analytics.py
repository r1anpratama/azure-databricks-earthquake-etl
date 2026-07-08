# Databricks notebook source
# DBTITLE 1, Gold Layer — Multi-Grain Analytics & ML-Ready Feature Store
# Medallion Architecture · Layer 3 of 3
# Platform: Databricks Community Edition (Serverless SQL Warehouse)
# Input:  main.default.silver_earthquake_events (Delta, UC)
# Target: Multiple Gold Delta tables (UC managed tables)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🥇 Gold Layer
# MAGIC
# MAGIC **Purpose:** Produce analytics-ready, multi-grain aggregations for BI, ML, and reporting.
# MAGIC
# MAGIC **Gold Tables:**
# MAGIC | Table | Grain | Use Case |
# MAGIC |-------|-------|----------|
# MAGIC | `gold_daily_stats`       | Daily              | Time-series, anomaly detection |
# MAGIC | `gold_monthly_stats`     | Monthly            | Trend analysis, seasonality |
# MAGIC | `gold_regional_stats`    | Region × Month     | Geographic risk comparison |
# MAGIC | `gold_mag_distribution`  | Magnitude Category | Statistical distribution |
# MAGIC | `gold_ml_features`       | Event-level         | ML model features (lagged features, rolling stats) |

# COMMAND ----------
from pyspark.sql.functions import (
    col, year, month, quarter, date_format, desc, asc,
    count as _count, avg, max as _max, min as _min,
    round as _round, sum as _sum, when, lit, expr,
    lag, avg as spark_avg, stddev, percentile_approx,
    datediff, to_date, max as spark_max, coalesce
)
from pyspark.sql.window import Window

# COMMAND ----------
# Configuration
CATALOG = "main"
SCHEMA = "default"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_earthquake_events"
GOLD_PREFIX = f"{CATALOG}.{SCHEMA}"

print(f"Input: {SILVER_TABLE}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Read Silver

# COMMAND ----------
silver_df = spark.table(SILVER_TABLE)
silver_count = silver_df.count()
print(f"Read {silver_count:,} rows from {SILVER_TABLE}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Daily Stats — Time-Series Grain
# MAGIC
# MAGIC Per-day aggregations for anomaly detection and forecasting.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Daily Stats — Time-Series Grain
# MAGIC
# MAGIC Per-day aggregations with rolling windows for trend analysis and anomaly detection.

# COMMAND ----------
daily_stats = (silver_df
    .groupBy(date_format(col("time"), "yyyy-MM-dd").alias("event_date"))
    .agg(
        _count("*").alias("event_count"),
        _round(avg("mag"), 2).alias("avg_magnitude"),
        _max("mag").alias("max_magnitude"),
        _min("mag").alias("min_magnitude"),
        _round(stddev("mag"), 3).alias("std_magnitude"),
        _round(avg("depth"), 1).alias("avg_depth"),
        _max("depth").alias("max_depth"),
        _count(when(col("mag") >= 5.0, 1)).alias("significant_events"),
        _count(when(col("mag") >= 7.0, 1)).alias("major_events")
    )
    .orderBy(desc("event_date"))
)

# Add 7-day and 30-day rolling averages
w7 = Window.orderBy(col("event_date")).rowsBetween(-6, 0)
w30 = Window.orderBy(col("event_date")).rowsBetween(-29, 0)

daily_stats = (daily_stats
    .withColumn("rolling_7d_avg_mag", _round(spark_avg("avg_magnitude").over(w7), 2))
    .withColumn("rolling_30d_avg_mag", _round(spark_avg("avg_magnitude").over(w30), 2))
    .withColumn("rolling_7d_event_count", _sum("event_count").over(w7))
    .withColumn("rolling_30d_event_count", _sum("event_count").over(w30))
)

# COMMAND ----------
daily_stats.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_PREFIX}.gold_daily_stats")

print(f"✅ gold_daily_stats: {daily_stats.count():,} rows")
display(daily_stats.limit(20))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Monthly Stats — Trend Analysis Grain

# COMMAND ----------
monthly_stats = (silver_df
    .groupBy(
        year(col("time")).alias("event_year"),
        month(col("time")).alias("event_month")
    )
    .agg(
        _count("*").alias("event_count"),
        _max("mag").alias("max_magnitude"),
        _round(avg("mag"), 2).alias("avg_magnitude"),
        _round(stddev("mag"), 3).alias("std_magnitude"),
        _round(avg("depth"), 1).alias("avg_depth"),
        _min("depth").alias("min_depth"),
        _max("depth").alias("max_depth"),
        _count(when(col("mag") >= 5.0, 1)).alias("significant_events"),
        _count(when(col("mag") >= 7.0, 1)).alias("major_events"),
        _count(when(col("depth_class") == "shallow", 1)).alias("shallow_count"),
        _count(when(col("depth_class") == "intermediate", 1)).alias("intermediate_count"),
        _count(when(col("depth_class") == "deep", 1)).alias("deep_count")
    )
    .orderBy(col("event_year").desc(), col("event_month").desc())
)

# Month-over-month change
monthly_window = Window.orderBy(col("event_year").desc(), col("event_month").desc())
monthly_stats = monthly_stats.withColumn(
    "mom_event_count_delta",
    col("event_count") - lag("event_count", 1).over(monthly_window)
)

monthly_stats.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_PREFIX}.gold_monthly_stats")

print(f"✅ gold_monthly_stats: {monthly_stats.count():,} rows")
display(monthly_stats)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Regional Stats — Geographic Risk Comparison

# COMMAND ----------
regional_stats = (silver_df
    .groupBy(
        col("region"),
        year(col("time")).alias("event_year"),
        month(col("time")).alias("event_month")
    )
    .agg(
        _count("*").alias("event_count"),
        _max("mag").alias("max_magnitude"),
        _round(avg("mag"), 2).alias("avg_magnitude"),
        _round(avg("depth"), 1).alias("avg_depth"),
        _count(when(col("mag") >= 5.0, 1)).alias("significant_events"),
        _count(when(col("mag") >= 7.0, 1)).alias("major_events")
    )
    .orderBy(col("event_year").desc(), col("event_month").desc(), desc("event_count"))
)

regional_stats.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_PREFIX}.gold_regional_stats")

print(f"✅ gold_regional_stats: {regional_stats.count():,} rows")
display(regional_stats.limit(30))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Magnitude Distribution — Statistical Summary

# COMMAND ----------
mag_distribution = (silver_df
    .groupBy(col("mag_category"))
    .agg(
        _count("*").alias("count"),
        _min("mag").alias("min_mag"),
        _max("mag").alias("max_mag"),
        _round(avg("mag"), 2).alias("avg_mag"),
        _round(stddev("mag"), 3).alias("std_mag"),
        _round(avg("depth"), 1).alias("avg_depth"),
        _round(stddev("depth"), 3).alias("std_depth"),
        _count("*").alias("total_events")
    )
    .orderBy(col("min_mag"))
)

mag_distribution.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_PREFIX}.gold_mag_distribution")

print(f"✅ gold_mag_distribution: {mag_distribution.count():,} categories")
display(mag_distribution)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. ML Feature Store — Event-Level with Lagged Features
# MAGIC
# MAGIC Per-event features with temporal and spatial lag for ML models.
# MAGIC Includes rolling statistics and time-since-last-event per region.

# COMMAND ----------
# Regional lag features: time since last event in same region
region_time_window = Window.partitionBy("region").orderBy(col("time"))

ml_features = (silver_df
    .select(
        col("id"),
        col("time"),
        col("place"),
        col("latitude"),
        col("longitude"),
        col("depth"),
        col("mag"),
        col("magType"),
        col("mag_category"),
        col("depth_class"),
        col("region"),
        col("event_year"),
        col("event_month"),
        col("event_quarter"),
        col("nst"),
        col("gap"),
        col("dmin"),
        col("rms"),
        col("status")
    )
    # Lag features within region
    .withColumn("prev_event_mag", lag("mag").over(region_time_window))
    .withColumn("prev_event_depth", lag("depth").over(region_time_window))
    .withColumn("time_since_prev_event", datediff(col("time"), lag("time").over(region_time_window)))
    
    # Rolling 10-event average magnitude within region
    .withColumn(
        "rolling_10_event_avg_mag",
        _round(spark_avg("mag").over(region_time_window.rowsBetween(-9, 0)), 2)
    )
    
    # Rolling 10-event max magnitude within region
    .withColumn(
        "rolling_10_event_max_mag",
        _max("mag").over(region_time_window.rowsBetween(-9, 0))
    )
    
    # Rolling 10-event count (events in last 10 at this region)
    .withColumn(
        "rolling_10_event_count",
        _count("*").over(region_time_window.rowsBetween(-9, 0))
    )
    
    # Percentile of magnitude within region (95th)
    .withColumn(
        "mag_95th_percentile_region",
        percentile_approx("mag", 0.95).over(region_time_window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))
    )
)

# Write ML feature table
ml_features.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_PREFIX}.gold_ml_features")

print(f"✅ gold_ml_features: {ml_features.count():,} rows (event-level with lagged features)")
display(ml_features.limit(20))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Pipeline Summary

# COMMAND ----------
print(f"\n{'='*70}")
print(f"  GOLD LAYER — PIPELINE COMPLETE")
print(f"{'='*70}")
print(f"  Silver input:              {silver_count:,} rows")
print(f"  gold_daily_stats:          {daily_stats.count():,} rows (daily grain, rolling features)")
print(f"  gold_monthly_stats:        {monthly_stats.count():,} rows (monthly, MoM deltas)")
print(f"  gold_regional_stats:       {regional_stats.count():,} rows (region × month)")
print(f"  gold_mag_distribution:     {mag_distribution.count():,} categories")
print(f"  gold_ml_features:           {ml_features.count():,} rows (event-level, lagged)")
print(f"{'='*70}")
print(f"\n  Gold Tables in Unity Catalog:")

tables = [
    f"{GOLD_PREFIX}.gold_daily_stats",
    f"{GOLD_PREFIX}.gold_monthly_stats",
    f"{GOLD_PREFIX}.gold_regional_stats",
    f"{GOLD_PREFIX}.gold_mag_distribution",
    f"{GOLD_PREFIX}.gold_ml_features",
]
for t in tables:
    cnt = spark.table(t).count()
    print(f"    {t}: {cnt:,} rows")

print(f"\n{'='*70}")
print(f"  ✅ Bronze → Silver → Gold pipeline complete!")
print(f"{'='*70}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Sample Analytics — Top 5 Strongest Events

# COMMAND ----------
print("\n--- Top 5 Strongest Events ---")
display(
    ml_features.select("time", "place", "mag", "depth", "mag_category", "region")
    .orderBy(desc("mag")).limit(5)
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Sample Analytics — Regional Risk Ranking (by events)

# COMMAND ----------
print("\n--- Regional Risk Ranking ---")
display(
    silver_df.groupBy("region")
    .agg(
        _count("*").alias("total_events"),
        _max("mag").alias("max_mag"),
        _round(avg("mag"), 2).alias("avg_mag"),
        _count(when(col("mag") >= 5.0, 1)).alias("significant_events")
    )
    .orderBy(desc("total_events"))
)