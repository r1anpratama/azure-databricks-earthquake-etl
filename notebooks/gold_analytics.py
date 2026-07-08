# Databricks notebook source
# DBTITLE 1, Gold Layer — Business Analytics (Community Edition)
# Self-contained: reads Silver temp view, produces Gold aggregations
# Run AFTER silver_cleaning.py
# Run on: Serverless Starter Warehouse

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold Layer
# MAGIC Aggregate Silver → analytics-ready results (display only, no file write)
# MAGIC 1. Daily stats (count, avg/max magnitude, avg depth)
# MAGIC 2. Monthly stats (count, avg/max magnitude)
# MAGIC 3. Magnitude distribution (by category)

# COMMAND ----------
from pyspark.sql.functions import (
    col, year, month, date_format, count as _count,
    avg, max as _max, min as _min, round as _round, desc
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Read Silver (from temp view)

# COMMAND ----------
df_silver = spark.sql("SELECT * FROM silver_events")
print(f"Read {df_silver.count():,} rows from Silver temp view")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Aggregation 1: Daily Stats
# MAGIC Events per day, avg/max magnitude, avg depth

# COMMAND ----------
daily = df_silver.groupBy(date_format(col("time"), "yyyy-MM-dd").alias("date")) \
    .agg(
        _count("*").alias("event_count"),
        _round(avg("mag"), 2).alias("avg_magnitude"),
        _round(_max("mag"), 2).alias("max_magnitude"),
        _round(avg("depth"), 1).alias("avg_depth")
    ) \
    .orderBy(desc("date"))

print(f"✅ Daily stats: {daily.count():,} rows")
display(daily.limit(20))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Aggregation 2: Monthly Stats
# MAGIC Monthly trends for time-series analysis

# COMMAND ----------
monthly = df_silver.groupBy(
    year(col("time")).alias("year"),
    month(col("time")).alias("month")
) \
    .agg(
        _count("*").alias("event_count"),
        _max("mag").alias("max_magnitude"),
        _round(avg("mag"), 2).alias("avg_magnitude"),
        _round(avg("depth"), 1).alias("avg_depth")
    ) \
    .orderBy(col("year").desc(), col("month").desc())

print(f"✅ Monthly stats: {monthly.count():,} rows")
display(monthly)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Aggregation 3: Magnitude Distribution

# COMMAND ----------
mag_dist = df_silver.groupBy(col("mag_category")) \
    .agg(
        _count("*").alias("count"),
        _min("mag").alias("min_mag"),
        _max("mag").alias("max_mag"),
        _round(avg("depth"), 1).alias("avg_depth")
    ) \
    .orderBy(desc("max_mag"))

print(f"✅ Magnitude distribution: {mag_dist.count():,} categories")
display(mag_dist)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold Layer Summary

# COMMAND ----------
print(f"\n{'='*60}")
print(f"GOLD LAYER SUMMARY")
print(f"{'='*60}")
print(f"Silver source rows:   {df_silver.count():,}")
print(f"Daily stats rows:     {daily.count():,}")
print(f"Monthly stats rows:   {monthly.count():,}")
print(f"Mag categories:       {mag_dist.count():,}")
print(f"Storage:              In-memory results (display only)")

print(f"\n--- Top 5 Most Active Days ---")
display(daily.orderBy(desc("event_count")).limit(5))
print(f"\n--- Top 5 Strongest Events ---")
display(df_silver.select("time", "place", "mag", "depth").orderBy(desc("mag")).limit(5))

# Create temp views for downstream use
daily.createOrReplaceTempView("gold_daily_stats")
monthly.createOrReplaceTempView("gold_monthly_stats")
mag_dist.createOrReplaceTempView("gold_mag_distribution")
print("\n✅ Gold temp views created: gold_daily_stats, gold_monthly_stats, gold_mag_distribution")