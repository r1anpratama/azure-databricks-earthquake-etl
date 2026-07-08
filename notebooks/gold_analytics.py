# spark=3.5
# Databricks notebook source

# MAGIC %md
# MAGIC # Gold Layer — Business Analytics
# MAGIC Aggregate Silver into analytics-ready views
# MAGIC Output: Delta tables for BI / ML consumption

# COMMAND ----------
# Setup widgets (Community Edition)
dbutils.widgets.text("silver_path", "/tmp/earthquake_analytics/silver/events")
dbutils.widgets.text("gold_path", "/tmp/earthquake_analytics/gold")

# COMMAND ----------
from src.gold import GoldAggregator

# COMMAND ----------
gold = GoldAggregator(
    source_path=dbutils.widgets.get("silver_path") or "/mnt/earthquake_analytics/silver/events",
    output_path=dbutils.widgets.get("gold_path") or "/mnt/earthquake_analytics/gold"
)

df_silver = spark.read.format("delta").load(gold.source_path)
daily, monthly, mag_dist = gold.build_aggregations(df_silver)
gold.load_all(daily, monthly, mag_dist)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold Layer Tables

# COMMAND ----------
print(f"Daily:    {daily.count():,} rows")
print(f"Monthly:  {monthly.count():,} rows")
print(f"Mag Dist: {mag_dist.count():,} rows")

display(monthly)
