# Databricks notebook source
# DBTITLE 1, Debug — Test Spark operations on Community Edition
# Run cells ONE BY ONE, stop at first error

# COMMAND ----------
# Test 1: Basic Spark session
print("Spark version:", spark.version)
print("✅ Test 1 passed: Spark session works")

# COMMAND ----------
# Test 2: Simple DataFrame creation (no pandas)
from pyspark.sql import Row
df = spark.createDataFrame([Row(id=1, val="a"), Row(id=2, val="b")])
print("Count:", df.count())
print("✅ Test 2 passed: createDataFrame + count works")

# COMMAND ----------
# Test 3: DataFrame transformations
df2 = df.withColumn("val2", df.val + "_x")
print("Transform count:", df2.count())
print("✅ Test 3 passed: withColumn works")

# COMMAND ----------
# Test 4: Aggregation
df3 = df2.groupBy("val2").count()
print("Agg count:", df3.count())
print("✅ Test 4 passed: groupBy + agg works")

# COMMAND ----------
# Test 5: Window function
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
w = Window.partitionBy("val2").orderBy("id")
df4 = df2.withColumn("rn", row_number().over(w))
print("Window count:", df4.count())
print("✅ Test 5 passed: Window function works")

# COMMAND ----------
# Test 6: Pandas -> Spark DataFrame
import pandas as pd
pdf = pd.DataFrame({"a": [1,2,3], "b": [4,5,6]})
df5 = spark.createDataFrame(pdf)
print("Pandas->Spark count:", df5.count())
print("✅ Test 6 passed: pandas to Spark works")

# COMMAND ----------
# Test 7: CSV read from URL (if supported)
# Try: spark.read.csv("https://...") - usually fails on HTTPS
# print("✅ Test 7 passed: CSV read works")

# COMMAND ----------
# Test 8: display()
display(df.limit(5))
print("✅ Test 8 passed: display works")

# COMMAND ----------
print("\n=== ALL TESTS PASSED ===")
print("If you see this, full pipeline should work!")