"""Verify Spark results against a plain-Python oracle computed from the same CSV."""
import csv
from collections import defaultdict
from pathlib import Path

import pytest

from src import aggregations as agg

DATA = str(Path(__file__).resolve().parent.parent / "data" / "hospital_revenue.csv")


@pytest.fixture(scope="module")
def df(spark):
    return agg.load_revenue(spark, DATA)


@pytest.fixture(scope="module")
def oracle():
    """department -> list of revenue values."""
    groups = defaultdict(list)
    with open(DATA, newline="") as f:
        for row in csv.DictReader(f):
            groups[row["department"]].append(float(row["revenue"]))
    return groups


def test_department_count_sum_avg_min_max(df, oracle):
    result = {r["department"]: r for r in agg.department_stats(df).collect()}
    assert set(result) == set(oracle)
    for dept, values in oracle.items():
        row = result[dept]
        assert row["visit_count"] == len(values)
        assert row["total_revenue"] == pytest.approx(sum(values), abs=0.01)
        assert row["avg_revenue"] == pytest.approx(sum(values) / len(values), abs=0.01)
        assert row["min_revenue"] == pytest.approx(min(values), abs=0.01)
        assert row["max_revenue"] == pytest.approx(max(values), abs=0.01)


def test_multi_column_group_counts_add_up(df):
    total_rows = df.count()
    grouped = agg.department_branch_stats(df)
    assert grouped.agg({"visit_count": "sum"}).collect()[0][0] == total_rows


def test_having_filter_removes_small_departments(df, oracle):
    stats = agg.department_stats(df)
    kept = {r["department"] for r in agg.having_filter(stats, 20_000).collect()}
    expected = {d for d, v in oracle.items() if sum(v) >= 20_000}
    assert kept == expected
    assert "Pediatrics" not in kept


def test_sql_having_matches_dataframe_filter(spark, df):
    from_df = sorted(map(tuple, agg.having_filter(agg.department_stats(df), 20_000).collect()))
    from_sql = sorted(map(tuple, agg.department_stats_sql(spark, df, 20_000).collect()))
    assert from_df == from_sql


def test_where_before_groupby_only_uses_matching_rows(df):
    result = agg.where_then_aggregate(df, "Procedure")
    total = result.agg({"visit_count": "sum"}).collect()[0][0]
    assert total == df.filter(df.service_type == "Procedure").count()
