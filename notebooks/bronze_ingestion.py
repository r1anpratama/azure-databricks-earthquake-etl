# spark=3.5
# Databricks notebook source

# MAGIC %md
# MAGIC # Bronze Layer — Raw Ingestion
# MAGIC Ingest USGS earthquake CSV with minimal schema enforcement.
# MAGIC Output: Delta table (ACID, schema-on-read, raw format preserved)

# COMMAND ----------
from src.bronze import BronzeIngestion

# COMMAND ----------
bronze = BronzeIngestion(
    source_url=(
        "https://earthquake.usgs.gov/fdsnws/event/1/query"
        "?format=csv&minmagnitude=2.5&orderby=time"
    ),
    output_path=dbutils.widgets.get("output_path") or "/mnt/earthquake_analytics/bronze/events"
)

df = bronze.extract()
bronze.validate_raw(df)
bronze.load(df)
bronze.summarize(df)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Data Quality Report
# MAGIC
# MAGIC Run quality checks on raw data:

# COMMAND ----------
from src.quality import DataQuality

dq = DataQuality(spark)
dq.report_all("bronze").show(20, truncate=False)
