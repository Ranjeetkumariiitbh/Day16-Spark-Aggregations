"""Day 16 - Spark Aggregations: hospital department revenue analysis.

Run from the project root:
    python -m src.main
    python -m src.main --data data/hospital_revenue.csv --output output
"""
import argparse
import time
from pathlib import Path

from pyspark.sql import DataFrame

from src import aggregations as agg
from src.session import create_spark

DEPT_MIN_REVENUE = 20_000.0        # HAVING SUM(revenue) >= 20000 (department level)
BRANCH_MIN_REVENUE = 5_000.0       # HAVING SUM(revenue) >= 5000  (department + branch level)
BRANCH_MIN_VISITS = 2              # HAVING COUNT(*)     >= 2


def banner(title: str) -> None:
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")


def timed_action(label: str, df: DataFrame) -> None:
    """Trigger an action (count) and report wall-clock time."""
    start = time.perf_counter()
    rows = df.count()
    elapsed = (time.perf_counter() - start) * 1000
    print(f"{label:<45} rows={rows:<4} time={elapsed:8.1f} ms")


def save(df: DataFrame, output_dir: Path, name: str) -> None:
    """Write a small result as a single CSV file (coalesce(1) is fine for tiny outputs)."""
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(output_dir / name))


def main() -> None:
    parser = argparse.ArgumentParser(description="Day 16 Spark aggregations demo")
    parser.add_argument("--data", default="data/hospital_revenue.csv")
    parser.add_argument("--output", default="output")
    args = parser.parse_args()
    output_dir = Path(args.output)

    spark = create_spark()
    df = agg.load_revenue(spark, args.data)

    banner("1. Raw data")
    df.printSchema()
    df.show(5, truncate=False)
    print(f"Total rows: {df.count()}")

    banner("2. groupBy(department): count / sum / avg / min / max")
    dept = agg.department_stats(df)
    dept.show(truncate=False)

    banner("3. groupBy(department, branch): multi-column grouping")
    dept_branch = agg.department_branch_stats(df)
    dept_branch.show(30, truncate=False)

    banner("4. groupBy(department, branch, service_type): three columns")
    agg.department_branch_service_stats(df).show(60, truncate=False)

    banner("5. Financial summary per department (revenue, cost, profit, margin)")
    financials = agg.department_financials(df)
    financials.show(truncate=False)

    banner(f"6. HAVING-like filter: departments with total_revenue >= {DEPT_MIN_REVENUE:,.0f}")
    high_revenue = agg.having_filter(dept, DEPT_MIN_REVENUE)
    high_revenue.show(truncate=False)

    banner(
        f"7. HAVING-like filter on dept+branch: total_revenue >= {BRANCH_MIN_REVENUE:,.0f} "
        f"AND visit_count >= {BRANCH_MIN_VISITS}"
    )
    agg.having_filter(dept_branch, BRANCH_MIN_REVENUE, BRANCH_MIN_VISITS).show(30, truncate=False)

    banner("8. WHERE (before groupBy) vs HAVING (after groupBy)")
    print("WHERE service_type = 'Procedure', then aggregate:")
    agg.where_then_aggregate(df, "Procedure").show(truncate=False)

    banner("9. Same report in Spark SQL with a real HAVING clause")
    sql_result = agg.department_stats_sql(spark, df, DEPT_MIN_REVENUE)
    sql_result.show(truncate=False)
    same = sorted(map(tuple, high_revenue.collect())) == sorted(map(tuple, sql_result.collect()))
    print(f"DataFrame filter() result == SQL HAVING result: {same}")

    banner("10. Transformations vs actions (lazy evaluation)")
    start = time.perf_counter()
    lazy = agg.department_branch_stats(df)          # transformation(s) only
    print(f"Building the groupBy plan took {(time.perf_counter() - start) * 1000:.1f} ms (nothing executed yet)")
    timed_action("count() action triggers the job", lazy)
    timed_action("second count() (no caching, recomputed)", lazy)

    banner("11. Partitioning and shuffle behaviour")
    print(f"Input DataFrame partitions          : {df.rdd.getNumPartitions()}")
    print(f"After groupBy (shuffle partitions)  : {dept_branch.rdd.getNumPartitions()}")
    print("Physical plan (look for HashAggregate -> Exchange hashpartitioning -> HashAggregate):")
    dept_branch.explain()

    banner("12. Execution performance: cache() for repeated aggregations")
    cached = df.cache()
    cached.count()  # materialise the cache
    timed_action("dept stats on cached data", agg.department_stats(cached))
    timed_action("dept+branch stats on cached data", agg.department_branch_stats(cached))
    cached.unpersist()

    banner(f"Saving reports to {output_dir}/")
    save(dept, output_dir, "department_stats")
    save(dept_branch, output_dir, "department_branch_stats")
    save(financials, output_dir, "department_financials")
    save(high_revenue, output_dir, "high_revenue_departments")
    print("Done.")

    spark.stop()


if __name__ == "__main__":
    main()
