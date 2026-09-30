"""
hospital_aggregations.py
-------------------------
Day16-Spark-Aggregations

Practices Spark DataFrame aggregation (groupBy, count, sum, avg, min, max),
multi-column grouping, HAVING-like post-aggregation filtering, and inspects
partitions / shuffle behavior / execution plans along the way.

Dataset: data/hospital_billing.csv
    transaction_id, patient_id, department, doctor_name, bill_amount,
    payment_mode, city, admission_date, discharge_date

Run:
    spark-submit src/hospital_aggregations.py
    (or) python src/hospital_aggregations.py
"""

import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DATA_PATH = str(Path(__file__).resolve().parent.parent / "data" / "hospital_billing.csv")


def line(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("Day16-Spark-Aggregations")
        # small shuffle partition count so the effect is visible on a laptop-sized dataset
        .config("spark.sql.shuffle.partitions", "4")
        .master("local[*]")
        .getOrCreate()
    )


def load_data(spark: SparkSession):
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(DATA_PATH)
    )
    return df


def show_partition_info(df, label: str) -> None:
    num_partitions = df.rdd.getNumPartitions()
    print(f"[{label}] number of partitions: {num_partitions}")
    per_partition_counts = df.rdd.glom().map(len).collect()
    print(f"[{label}] rows per partition: {per_partition_counts}")


def single_column_grouping(df):
    line("1) Single-column grouping: revenue by department")

    dept_summary = (
        df.groupBy("department")
        .agg(
            F.count("*").alias("num_transactions"),
            F.sum("bill_amount").alias("total_revenue"),
            F.round(F.avg("bill_amount"), 2).alias("avg_bill"),
            F.min("bill_amount").alias("min_bill"),
            F.max("bill_amount").alias("max_bill"),
        )
        .orderBy(F.desc("total_revenue"))
    )
    dept_summary.show(truncate=False)
    return dept_summary


def multi_column_grouping(df):
    line("2) Multi-column grouping: department x payment_mode")

    dept_payment_summary = (
        df.groupBy("department", "payment_mode")
        .agg(
            F.count("*").alias("num_transactions"),
            F.sum("bill_amount").alias("total_revenue"),
            F.round(F.avg("bill_amount"), 2).alias("avg_bill"),
        )
        .orderBy("department", F.desc("total_revenue"))
    )
    dept_payment_summary.show(30, truncate=False)
    return dept_payment_summary


def having_like_filtering(dept_summary):
    line("3) HAVING-like filtering: departments earning above overall average revenue")

    # groupBy + agg produces a normal DataFrame, so a post-aggregation filter
    # on an aggregated column behaves exactly like SQL's HAVING clause.
    avg_total_revenue = dept_summary.agg(F.avg("total_revenue")).first()[0]
    print(f"Average total_revenue across departments: {avg_total_revenue:,.2f}")

    high_earning_depts = dept_summary.filter(F.col("total_revenue") > avg_total_revenue)
    high_earning_depts.show(truncate=False)

    line("3b) HAVING-like filtering: departments with >= 15 transactions")
    frequent_depts = dept_summary.filter(F.col("num_transactions") >= 15)
    frequent_depts.show(truncate=False)

    return high_earning_depts


def city_department_pivot(df):
    line("4) Multi-column grouping: department x city, pivoted for a readable summary")

    pivot_df = (
        df.groupBy("department")
        .pivot("city")
        .agg(F.round(F.sum("bill_amount"), 2))
        .orderBy("department")
    )
    pivot_df.show(truncate=False)
    return pivot_df


def transformations_vs_actions_demo(df):
    line("5) Transformations vs actions")

    # Transformations (lazy, build up a plan, nothing executes yet)
    filtered = df.filter(F.col("bill_amount") > 50000)
    grouped = filtered.groupBy("department").agg(F.sum("bill_amount").alias("total_revenue"))
    print("Transformations chained (filter -> groupBy -> agg): no job has run yet.")

    # Action (triggers execution)
    start = time.time()
    result_count = grouped.count()
    elapsed = time.time() - start
    print(f"Action .count() triggered execution -> {result_count} groups in {elapsed:.3f}s")


def partitioning_and_shuffle_demo(df):
    line("6) Partitions and shuffle behavior")

    show_partition_info(df, "original read")

    # repartition triggers a full shuffle across the cluster/executors
    repartitioned = df.repartition(8, "department")
    show_partition_info(repartitioned, "after repartition(8, 'department')")

    # coalesce avoids a full shuffle when reducing partition count
    coalesced = repartitioned.coalesce(2)
    show_partition_info(coalesced, "after coalesce(2)")

    line("6b) Execution plan for a grouped aggregation (look for the Exchange/shuffle step)")
    agg_plan = df.groupBy("department").agg(F.sum("bill_amount"))
    agg_plan.explain(mode="formatted")


def performance_comparison(df):
    line("7) Execution performance: repeated aggregation with and without cache")

    heavy = df.withColumn("bill_amount_adj", F.col("bill_amount") * 1.18)  # add GST, simulate work

    start = time.time()
    heavy.groupBy("department").agg(F.sum("bill_amount_adj")).count()
    heavy.groupBy("payment_mode").agg(F.avg("bill_amount_adj")).count()
    uncached_time = time.time() - start
    print(f"Two aggregations without cache: {uncached_time:.3f}s")

    heavy_cached = heavy.cache()
    heavy_cached.count()  # materialize the cache

    start = time.time()
    heavy_cached.groupBy("department").agg(F.sum("bill_amount_adj")).count()
    heavy_cached.groupBy("payment_mode").agg(F.avg("bill_amount_adj")).count()
    cached_time = time.time() - start
    print(f"Two aggregations with cache: {cached_time:.3f}s")

    heavy_cached.unpersist()


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    df = load_data(spark)
    print(f"Loaded {df.count()} rows from {DATA_PATH}")
    df.printSchema()

    dept_summary = single_column_grouping(df)
    multi_column_grouping(df)
    having_like_filtering(dept_summary)
    city_department_pivot(df)
    transformations_vs_actions_demo(df)
    partitioning_and_shuffle_demo(df)
    performance_comparison(df)

    line("Done")
    spark.stop()


if __name__ == "__main__":
    main()
