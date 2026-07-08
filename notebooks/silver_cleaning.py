# spark=3.5
# Databricks notebook source

# MAGIC %md
# MAGIC # Silver Layer — Cleaning & Validation
# MAGIC Read Bronze → Dedup → Validate → Enrich → Write Silver Delta
# MAGIC Output: Cleaned, partitioned Delta table

# COMMAND ----------
# Setup widgets (Community Edition)
dbutils.widgets.text("bronze_path", "/tmp/earthquake_analytics/bronze/events")
dbutils.widgets.text("silver_path", "/tmp/earthquake_analytics/silver/events")

# COMMAND ----------
from src.silver import SilverTransformer

# COMMAND ----------
silver = SilverTransformer(
    source_path=dbutils.widgets.get("bronze_path") or "/mnt/earthquake_analytics/bronze/events",
    output_path=dbutils.widgets.get("silver_path") or "/mnt/earthquake_analytics/silver/events"
)

df_bronze = spark.read.format("delta").load(silver.source_path)
df_silver = silver.transform(df_bronze)
silver.load(df_silver)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Validation Summary

# COMMAND ----------
from src.quality import DataQuality

dq = DataQuality(spark)
report = dq.report_all("silver").filter("status != 'PASS'")
display(report if report.count() > 0 else "✅ All checks passed")
