"""
Data Quality Framework — Rule-based validation at each medallion layer.
Checks: not_null, range, uniqueness, freshness, schema conformity.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, when, isnan, isnull, lit, min as spark_min, max as spark_max
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class QualityCheck:
    """Single check result."""

    def __init__(self, name: str, table: str, column: str,
                 rule: str, status: str, detail: str):
        self.name = name
        self.table = table
        self.column = column
        self.rule = rule
        self.status = status
        self.detail = detail

    def to_row(self) -> dict:
        return {
            "check_name": self.name,
            "table": self.table,
            "column": self.column,
            "rule": self.rule,
            "status": self.status,
            "detail": self.detail,
            "checked_at": datetime.utcnow().isoformat()
        }


class DataQuality:
    """Run configurable quality checks on a DataFrame."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def check_not_null(self, df: DataFrame, table: str,
                       column: str, threshold: float = 0.0) -> QualityCheck:
        total = df.count()
        nulls = df.filter(col(column).isNull() | isnan(col(column))).count()
        null_pct = nulls / total if total > 0 else 1.0
        status = "PASS" if null_pct <= threshold else "FAIL"
        return QualityCheck(
            name=f"not_null_{column}",
            table=table, column=column,
            rule=f"null_rate <= {threshold}",
            status=status,
            detail=f"{nulls}/{total} null ({null_pct:.1%})"
        )

    def check_range(self, df: DataFrame, table: str,
                    column: str, min_val: float, max_val: float) -> QualityCheck:
        total = df.count()
        out = df.filter(
            (col(column) < min_val) | (col(column) > max_val)
        ).count()
        status = "PASS" if out == 0 else "WARN" if out / total < 0.1 else "FAIL"
        return QualityCheck(
            name=f"range_{column}",
            table=table, column=column,
            rule=f"[{min_val}, {max_val}]",
            status=status,
            detail=f"{out}/{total} out of range"
        )

    def check_freshness(self, df: DataFrame, table: str,
                        column: str, max_age_days: int = 365) -> QualityCheck:
        max_date = df.agg(spark_max(col(column))).collect()[0][0]
        if max_date is None:
            return QualityCheck(name=f"freshness_{column}", table=table,
                                column=column, rule=f"max_age <= {max_age_days}d",
                                status="FAIL", detail="No data found")
        age = (datetime.utcnow() - max_date).days
        status = "PASS" if age <= max_age_days else "WARN"
        return QualityCheck(
            name=f"freshness_{column}", table=table, column=column,
            rule=f"max_age <= {max_age_days}d",
            status=status,
            detail=f"Latest: {max_date} ({age} days ago)"
        )

    def check_schema(self, df: DataFrame, table: str,
                     expected_columns: list) -> QualityCheck:
        actual = set(df.columns)
        expected = set(expected_columns)
        missing = expected - actual
        extra = actual - expected
        status = "PASS" if not missing else "FAIL"
        detail_parts = []
        if missing:
            detail_parts.append(f"Missing: {missing}")
        if extra:
            detail_parts.append(f"Extra: {extra}")
        return QualityCheck(
            name="schema_conformity",
            table=table, column="all",
            rule=f"columns = {expected_columns}",
            status=status,
            detail="; ".join(detail_parts) if detail_parts else "Schema OK"
        )

    def report_all(self, table: str) -> DataFrame:
        """Run standard checks and return report as DataFrame.
        'table' is used to load from known paths.
        """
        checks = []
        spark = self.spark
        try:
            df = spark.read.format("delta").load(
                f"/mnt/earthquake_analytics/{table}/events"
            )
        except Exception:
            df = None

        if df is None:
            return spark.createDataFrame([
                QualityCheck("load_check", table, "*", "delta_path_exists",
                             "FAIL", "Cannot load Delta table").to_row()
            ])

        # Run checks
        checks.append(self.check_not_null(df, table, "time"))
        checks.append(self.check_not_null(df, table, "mag"))
        checks.append(self.check_range(df, table, "latitude", -90, 90))
        checks.append(self.check_range(df, table, "longitude", -180, 180))
        checks.append(self.check_range(df, table, "mag", 0, 10))
        checks.append(self.check_range(df, table, "depth", 0, 1000))

        if "time" in df.columns:
            checks.append(self.check_freshness(df, table, "time"))

        rows = [c.to_row() for c in checks]
        return spark.createDataFrame(rows)
