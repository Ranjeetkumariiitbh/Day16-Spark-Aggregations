"""Reusable aggregation logic: groupBy + count/sum/avg/min/max and HAVING-like filters."""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

REVENUE_SCHEMA = StructType(
    [
        StructField("visit_id", StringType(), False),
        StructField("visit_date", DateType(), True),
        StructField("branch", StringType(), False),
        StructField("department", StringType(), False),
        StructField("service_type", StringType(), False),
        StructField("patients", IntegerType(), True),
        StructField("revenue", DoubleType(), True),
        StructField("cost", DoubleType(), True),
    ]
)


def load_revenue(spark: SparkSession, path: str) -> DataFrame:
    """Load the CSV with an explicit schema and add a derived `profit` column."""
    df = spark.read.option("header", True).schema(REVENUE_SCHEMA).csv(path)
    return df.withColumn("profit", F.round(F.col("revenue") - F.col("cost"), 2))


def _stats(group_cols: list[str], df: DataFrame) -> DataFrame:
    """groupBy(group_cols) with the five core aggregates on revenue."""
    return (
        df.groupBy(*group_cols)
        .agg(
            F.count("*").alias("visit_count"),
            F.round(F.sum("revenue"), 2).alias("total_revenue"),
            F.round(F.avg("revenue"), 2).alias("avg_revenue"),
            F.round(F.min("revenue"), 2).alias("min_revenue"),
            F.round(F.max("revenue"), 2).alias("max_revenue"),
        )
    )


def department_stats(df: DataFrame) -> DataFrame:
    """Single-column grouping: department."""
    return _stats(["department"], df).orderBy(F.desc("total_revenue"))


def department_branch_stats(df: DataFrame) -> DataFrame:
    """Two-column grouping: department + branch."""
    return _stats(["department", "branch"], df).orderBy("department", F.desc("total_revenue"))


def department_branch_service_stats(df: DataFrame) -> DataFrame:
    """Three-column grouping: department + branch + service_type."""
    return _stats(["department", "branch", "service_type"], df).orderBy(
        "department", "branch", "service_type"
    )


def department_financials(df: DataFrame) -> DataFrame:
    """Multi-metric aggregation across revenue, cost, profit and patients."""
    return (
        df.groupBy("department")
        .agg(
            F.sum("patients").alias("total_patients"),
            F.round(F.sum("revenue"), 2).alias("total_revenue"),
            F.round(F.sum("cost"), 2).alias("total_cost"),
            F.round(F.sum("profit"), 2).alias("total_profit"),
            F.round(F.avg("profit"), 2).alias("avg_profit_per_visit"),
        )
        .withColumn("revenue_per_patient", F.round(F.col("total_revenue") / F.col("total_patients"), 2))
        .withColumn("profit_margin_pct", F.round(F.col("total_profit") / F.col("total_revenue") * 100, 1))
        .orderBy(F.desc("total_profit"))
    )


def having_filter(stats: DataFrame, min_total_revenue: float, min_visits: int = 1) -> DataFrame:
    """HAVING-like filtering.

    SQL's HAVING filters *after* aggregation. In the DataFrame API this is simply
    `.filter()` applied to the aggregated DataFrame (as opposed to a WHERE-style
    filter applied to the raw rows *before* groupBy).
    """
    return stats.filter(
        (F.col("total_revenue") >= min_total_revenue) & (F.col("visit_count") >= min_visits)
    )


def where_then_aggregate(df: DataFrame, service_type: str) -> DataFrame:
    """WHERE-like filtering: rows are filtered *before* aggregation."""
    return department_stats(df.filter(F.col("service_type") == service_type))


def department_stats_sql(spark: SparkSession, df: DataFrame, min_total_revenue: float) -> DataFrame:
    """The same department report expressed in Spark SQL with a real HAVING clause."""
    df.createOrReplaceTempView("hospital_revenue")
    return spark.sql(
        f"""
        SELECT department,
               COUNT(*)                  AS visit_count,
               ROUND(SUM(revenue), 2)    AS total_revenue,
               ROUND(AVG(revenue), 2)    AS avg_revenue,
               ROUND(MIN(revenue), 2)    AS min_revenue,
               ROUND(MAX(revenue), 2)    AS max_revenue
        FROM hospital_revenue
        GROUP BY department
        HAVING SUM(revenue) >= {float(min_total_revenue)}
        ORDER BY total_revenue DESC
        """
    )
