# Databricks notebook source
# DBTITLE 1, Silver Layer — Cleansing, Deduplication & Enrichment
# Medallion Architecture · Layer 2 of 3
# Platform: Databricks Community Edition (Serverless SQL Warehouse)
# Input:  main.default.bronze_earthquake_events (Delta, UC)
# Target: main.default.silver_earthquake_events (Delta, UC managed table)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🥈 Silver Layer
# MAGIC
# MAGIC **Purpose:** Transform raw Bronze data into analysis-ready, validated, enriched records.
# MAGIC
# MAGIC **Transformations:**
# MAGIC 1. **Deduplication** — Window-based, keep latest `updated` per event ID
# MAGIC 2. **Type casting & validation** — Enforce typed columns, drop invalid
# MAGIC 3. **Geographic enrichment** — Derive `region` from lat/lon bounding boxes
# MAGIC 4. **Magnitude classification** — `mag_category` (minor/moderate/strong/major/great)
# MAGIC 5. **Temporal enrichment** — `event_year`, `event_month`, `event_quarter`
# MAGIC 6. **Depth classification** — Shallow/intermediate/deep
# MAGIC
# MAGIC **Target:** `main.default.silver_earthquake_events`
# MAGIC **Partitioning:** `event_year`, `event_month` (for partition pruning)

# COMMAND ----------
from pyspark.sql.functions import (
    col, when, trim, upper, lower, year, month, quarter,
    row_number, desc, count, isnull, isnan, lit
)
from pyspark.sql.window import Window

# COMMAND ----------
# Configuration
CATALOG = "main"
SCHEMA = "default"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_earthquake_events"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_earthquake_events"

print(f"Input:  {BRONZE_TABLE}")
print(f"Output: {SILVER_TABLE}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Read Bronze

# COMMAND ----------
bronze_df = spark.table(BRONZE_TABLE)
bronze_count = bronze_df.count()
print(f"Read {bronze_count:,} rows from {BRONZE_TABLE}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Deduplication (SCD-like — Keep Latest)
# MAGIC
# MAGIC USGS events are updated over time (automatic → reviewed). We keep
# MAGIC the version with the latest `updated` timestamp for each event ID.

# COMMAND ----------
dedup_window = Window.partitionBy("id").orderBy(desc("updated"))

silver_df = (bronze_df
    .withColumn("_row_num", row_number().over(dedup_window))
    .filter(col("_row_num") == 1)
    .drop("_row_num")
)

after_dedup = silver_df.count()
print(f"After dedup: {after_dedup:,} rows ({bronze_count - after_dedup:,} duplicates removed)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Null Critical Field Filtering
# MAGIC
# MAGIC Rows missing `time`, `latitude`, `longitude`, or `mag` cannot be used
# MAGIC for spatial or temporal analysis — they are dropped.

# COMMAND ----------
critical_fields = ["time", "latitude", "longitude", "mag"]

silver_df = silver_df.na.drop(subset=critical_fields)
after_null_filter = silver_df.count()
print(f"After null filter: {after_null_filter:,} rows ({after_dedup - after_null_filter:,} dropped)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Geographic Enrichment — Region Classification
# MAGIC
# MAGIC Derive a `region` column from lat/lon using bounding boxes for
# MAGIC major tectonic zones (Sunda Arc, Pacific Ring of Fire, etc.)

# COMMAND ----------
silver_df = silver_df.withColumn(
    "region",
    when((col("latitude").between(-10, 6)) & (col("longitude").between(94, 120)), "Sunda Arc")
    .when((col("latitude").between(-47, -30)) & (col("longitude").between(165, 180)), "New Zealand")
    .when((col("latitude").between(30, 55)) & (col("longitude").between(130, 155)), "Japan")
    .when((col("latitude").between(-35, 5)) & (col("longitude").between(-82, -65)), "South America")
    .when((col("latitude").between(30, 55)) & (col("longitude").between(-135, -115)), "California")
    .when((col("latitude").between(55, 72)) & (col("longitude").between(-170, -130)), "Alaska")
    .when((col("latitude").between(-90, 90)) & (col("longitude").between(-180, 180)), "Other")
    .otherwise("Unknown")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Magnitude Classification
# MAGIC
# MAGIC | Range  | Category  |
# MAGIC |--------|-----------|
# MAGIC | < 3.0  | micro     |
# MAGIC | 3.0–4.9 | minor    |
# MAGIC | 4.0–4.9 | light    |
# MAGIC | 5.0–5.9 | moderate  |
# MAGIC | 6.0–6.9 | strong    |
# MAGIC | 7.0–7.9 | major     |
# MAGIC | ≥ 8.0  | great     |

# COMMAND ----------
silver_df = silver_df.withColumn(
    "mag_category",
    when(col("mag") < 3.0, "micro")
    .when(col("mag").between(3.0, 3.9), "minor")
    .when(col("mag").between(4.0, 4.9), "light")
    .when(col("mag").between(5.0, 5.9), "moderate")
    .when(col("mag").between(6.0, 6.9), "strong")
    .when(col("mag").between(7.0, 7.9), "major")
    .otherwise("great")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Depth Classification
# MAGIC
# MAGIC | Range (km) | Classification |
# MAGIC |-----------|----------------|
# MAGIC | 0–70      | shallow        |
# MAGIC | 70–300    | intermediate   |
# MAGIC | > 300     | deep           |

# COMMAND ----------
silver_df = silver_df.withColumn(
    "depth_class",
    when(col("depth") <= 70, "shallow")
    .when(col("depth").between(71, 300), "intermediate")
    .otherwise("deep")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Temporal Enrichment
# MAGIC
# MAGIC Derived columns for partition pruning and time-series analysis.

# COMMAND ----------
silver_df = (silver_df
    .withColumn("event_year", year(col("time")))
    .withColumn("event_month", month(col("time")))
    .withColumn("event_quarter", quarter(col("time")))
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. String Cleanup

# COMMAND ----------
silver_df = silver_df.withColumn("place", trim(col("place")))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver Quality Checks

# COMMAND ----------
checks = [
    ("no_duplicates",      silver_df.groupBy("id").count().filter("count > 1").count() == 0),
    ("no_null_critical",   silver_df.filter(
        col("time").isNull() | col("latitude").isNull() | col("longitude").isNull() | col("mag").isNull()
    ).count() == 0),
    ("valid_lat",          silver_df.filter((col("latitude") < -90) | (col("latitude") > 90)).count() == 0),
    ("valid_lon",          silver_df.filter((col("longitude") < -180) | (col("longitude") > 180)).count() == 0),
    ("valid_mag",          silver_df.filter((col("mag") < 0) | (col("mag") > 10)).count() == 0),
    ("valid_depth",        silver_df.filter(col("depth") < 0).count() == 0),
    ("region_classified",  silver_df.filter(col("region") == "Unknown").count() == 0),
]

print(f"\n{'='*60}")
print(f"  SILVER LAYER QUALITY REPORT")
print(f"{'='*60}")
print(f"  {'Check':<25} {'Status':<10}")
print(f"  {'-'*35}")
fail_count = 0
for name, passed in checks:
    icon = "✅" if passed else "❌"
    print(f"  {name:<25} {icon}")
    if not passed:
        fail_count += 1
print(f"  {'-'*35}")
print(f"  {len(checks) - fail_count}/{len(checks)} checks passed")
print(f"{'='*60}\n")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Write to Delta — UC Managed Table (Partitioned)

# COMMAND ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# Write partitioned Delta table for efficient pruning
silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("event_year", "event_month") \
    .saveAsTable(SILVER_TABLE)

print(f"✅ Silver table written: {SILVER_TABLE}")
print(f"   Rows: {silver_df.count():,}")
print(f"   Columns: {len(silver_df.columns)}")
print(f"   Partitions: event_year, event_month")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary Statistics

# COMMAND ----------
print(f"\n{'='*60}")
print(f"  SILVER LAYER SUMMARY")
print(f"{'='*60}")
print(f"  Bronze input:       {bronze_count:,}")
print(f"  Silver output:      {silver_df.count():,}")
print(f"  Rows dropped:        {bronze_count - silver_df.count():,}")
print(f"  Dedup removed:       {bronze_count - after_dedup:,}")
print(f"  Null-filter removed: {after_dedup - after_null_filter:,}")
print(f"\n  By Region:")

silver_df.groupBy("region").count().orderBy(desc("count")).show(truncate=False)

print(f"\n  By Magnitude Category:")
silver_df.groupBy("mag_category").count().orderBy("mag_category").show(truncate=False)

print(f"\n  By Depth Class:")
silver_df.groupBy("depth_class").count().orderBy("depth_class").show(truncate=False)

print(f"\n{'='*60}")