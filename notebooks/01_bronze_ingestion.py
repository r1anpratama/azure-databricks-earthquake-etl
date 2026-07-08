# Databricks notebook source
# DBTITLE 1, Bronze Layer — Raw Ingestion with Schema Enforcement
# Medallion Architecture · Layer 1 of 3
# Platform: Databricks Community Edition (Serverless SQL Warehouse)
# Source: USGS FDSNWS Earthquake API (CSV)
# Target: Delta Lake managed table in Unity Catalog

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🥉 Bronze Layer
# MAGIC
# MAGIC **Purpose:** Ingest raw earthquake event data from USGS FDSNWS API with schema enforcement and ingestion metadata.
# MAGIC
# MAGIC **Key Features:**
# MAGIC - Explicit schema definition (schema-on-read, not inference)
# MAGIC - Data quality framework with pass/fail/warn severity
# MAGIC - Idempotent ingestion (overwrite mode)
# MAGIC - Ingestion audit columns (`_ingested_at`, `_source`, `_batch_id`)
# MAGIC
# MAGIC **Target:** `main.default.bronze_earthquake_events` (Delta managed table, UC)

# COMMAND ----------
# Imports
import pandas as pd
import hashlib
from datetime import datetime, timezone
from pyspark.sql.functions import (
    current_timestamp, lit, col, count, when, isnull, isnan,
    min as spark_min, max as spark_max, desc
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, LongType, IntegerType
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------
# USGS FDSNWS API endpoint
SOURCE_URL = (
    "https://earthquake.usgs.gov/fdsnws/event/1/query"
    "?format=csv&minmagnitude=2.5&orderby=time"
)

# Unity Catalog target (Community Edition: `workspace` catalog is provisioned)
CATALOG = "workspace"
SCHEMA = "default"
TABLE_NAME = "bronze_earthquake_events"
FULL_TABLE = f"{CATALOG}.{SCHEMA}.{TABLE_NAME}"

print(f"Source:  {SOURCE_URL}")
print(f"Target:  {FULL_TABLE}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Schema Definition (Explicit, No Inference)
# MAGIC
# MAGIC Enforcing a strict schema at ingestion prevents downstream corruption
# MAGIC from silent API changes. Every column is explicitly typed.

# COMMAND ----------
EARTHQUAKE_SCHEMA = StructType([
    StructField("time",          TimestampType(),  True),   # Event origin time (UTC)
    StructField("latitude",      DoubleType(),     True),   # Decimal degrees
    StructField("longitude",     DoubleType(),     True),   # Decimal degrees
    StructField("depth",         DoubleType(),     True),   # Depth in km
    StructField("mag",           DoubleType(),     True),   # Magnitude
    StructField("magType",       StringType(),     True),   # Magnitude type (ml, mb, mw, etc.)
    StructField("nst",           IntegerType(),    True),   # Number of stations
    StructField("gap",           DoubleType(),     True),   # Azimuthal gap
    StructField("dmin",          DoubleType(),     True),   # Horizontal distance to nearest station
    StructField("rms",           DoubleType(),     True),   # Root-mean-square travel time residual
    StructField("net",           StringType(),     True),   # Network contributor
    StructField("id",            StringType(),     False),  # Unique event identifier (NOT NULL)
    StructField("updated",       TimestampType(),  True),   # Last update time
    StructField("place",         StringType(),     True),   # Human-readable location
    StructField("type",          StringType(),     True),   # Event type (earthquake, quarry blast, etc.)
    StructField("horizontalError", DoubleType(),   True),
    StructField("depthError",    DoubleType(),     True),
    StructField("magError",      DoubleType(),     True),
    StructField("magNst",        IntegerType(),    True),
    StructField("status",       StringType(),     True),   # automatic / reviewed
    StructField("locationSource", StringType(),    True),
    StructField("magSource",     StringType(),     True),
])

# COMMAND ----------
# MAGIC %md
# MAGIC ## Data Quality Framework
# MAGIC
# MAGIC Each check produces a row in a quality report DataFrame.
# MAGIC Severity: `FAIL` → drop rows, `WARN` → log only, `INFO` → statistics

# COMMAND ----------
def run_quality_checks(df, layer="bronze"):
    """Run rule-based quality checks and return a report DataFrame."""
    checks = []

    # --- Critical (FAIL severity) ---
    checks.append(("DQ001", "no_null_id",           "id",          "not_null",   "FAIL", df.filter(col("id").isNull()).count()))
    checks.append(("DQ002", "no_null_time",         "time",        "not_null",   "FAIL", df.filter(col("time").isNull()).count()))
    checks.append(("DQ003", "valid_latitude",       "latitude",   "[-90, 90]",  "FAIL", df.filter((col("latitude") < -90) | (col("latitude") > 90)).count()))
    checks.append(("DQ004", "valid_longitude",     "longitude",  "[-180, 180]","FAIL", df.filter((col("longitude") < -180) | (col("longitude") > 180)).count()))
    checks.append(("DQ005", "valid_magnitude",     "mag",         "[0, 10]",    "FAIL", df.filter((col("mag") < 0) | (col("mag") > 10)).count()))

    # --- Warning (WARN severity) ---
    checks.append(("DQ006", "no_null_magnitude",   "mag",         "not_null",   "WARN", df.filter(col("mag").isNull()).count()))
    checks.append(("DQ007", "no_null_depth",       "depth",       "not_null",   "WARN", df.filter(col("depth").isNull()).count()))
    checks.append(("DQ008", "no_null_place",       "place",       "not_null",   "WARN", df.filter(col("place").isNull()).count()))

    # Build report
    report = spark.createDataFrame(
        [(rule_id, name, column, rule, severity, failures, layer,
          datetime.now(timezone.utc).isoformat())
         for rule_id, name, column, rule, severity, failures in checks],
        schema="rule_id STRING, check_name STRING, column STRING, rule STRING, severity STRING, failures LONG, layer STRING, checked_at STRING"
    )
    return report


def print_quality_report(report_df, layer="bronze"):
    """Pretty-print the quality report."""
    print(f"\n{'='*70}")
    print(f"  DATA QUALITY REPORT — {layer.upper()} LAYER")
    print(f"{'='*70}")
    print(f"  {'Rule':<8} {'Check':<25} {'Column':<15} {'Severity':<10} {'Failures':<10}")
    print(f"  {'-'*68}")
    rows = report_df.collect()
    fail_count = 0
    warn_count = 0
    for r in rows:
        icon = "❌" if r.severity == "FAIL" and r.failures > 0 else "⚠️" if r.severity == "WARN" and r.failures > 0 else "✅"
        print(f"  {r.rule_id:<8} {r.check_name:<25} {r.column:<15} {r.severity:<10} {r.failures:<10} {icon}")
        if r.severity == "FAIL" and r.failures > 0:
            fail_count += 1
        if r.severity == "WARN" and r.failures > 0:
            warn_count += 1
    print(f"  {'-'*68}")
    print(f"  Summary: {fail_count} FAIL, {warn_count} WARN, {len(rows) - fail_count - warn_count} PASS")
    print(f"{'='*70}\n")
    return fail_count == 0  # True if no FAIL checks

# COMMAND ----------
# MAGIC %md
# MAGIC ## Extract — Download from USGS API via Pandas
# MAGIC
# MAGIC We use pandas for the HTTP fetch (handling redirects, compression)
# MAGIC then convert to Spark DataFrame with explicit schema.

# COMMAND ----------
df_pd = pd.read_csv(SOURCE_URL)
print(f"Downloaded {len(df_pd):,} records from USGS FDSNWS")

# Pre-parse timestamps in pandas (avoids Arrow conversion issues)
for col_name in ("time", "updated"):
    if col_name in df_pd.columns:
        df_pd[col_name] = pd.to_datetime(df_pd[col_name], utc=True)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Schema Enforcement & DataFrame Creation

# COMMAND ----------
bronze_df = spark.createDataFrame(df_pd, schema=EARTHQUAKE_SCHEMA)

# Add ingestion audit columns
batch_id = hashlib.md5(
    f"{SOURCE_URL}:{datetime.now(timezone.utc).isoformat()}".encode()
).hexdigest()[:12]

bronze_df = (bronze_df
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source", lit("USGS_FDSNWS"))
    .withColumn("_batch_id", lit(batch_id))
)

total_rows = bronze_df.count()
print(f"Bronze DataFrame: {total_rows:,} rows × {len(bronze_df.columns)} columns")
print(f"Batch ID: {batch_id}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Data Quality — Bronze Layer

# COMMAND ----------
dq_report = run_quality_checks(bronze_df, layer="bronze")
all_passed = print_quality_report(dq_report, layer="bronze")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Load — Write to Delta Managed Table (Unity Catalog)

# COMMAND ----------
# Create schema if not exists (UC)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# Write as Delta managed table (idempotent — overwrite per batch)
bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(FULL_TABLE)

print(f"✅ Bronze table written: {FULL_TABLE}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Schema Registration (for downstream notebooks)

# COMMAND ----------
# Store schema info for Silver notebook to read via df.schema
bronze_schema_json = bronze_df.schema.jsonValue()

# Also verify table is readable
verify_df = spark.table(FULL_TABLE)
print(f"Verification: {verify_df.count():,} rows in {FULL_TABLE}")
print(f"Columns: {len(verify_df.columns)}")

display(verify_df.limit(20))