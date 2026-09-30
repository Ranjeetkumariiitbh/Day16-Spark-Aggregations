# Day 16 - Spark Aggregations (PySpark)

Practice `groupBy()` with `count`, `sum`, `avg`, `min` and `max` on a **hospital department revenue** dataset, including multi-column grouping and HAVING-like filtering after aggregation.

## What it covers

| Topic | Where |
|---|---|
| `count / sum / avg / min / max` with `groupBy("department")` | `department_stats` |
| Multi-column grouping (`department, branch` and `department, branch, service_type`) | `department_branch_stats`, `department_branch_service_stats` |
| Revenue, cost, profit, margin and revenue-per-patient per department | `department_financials` |
| HAVING-like filtering (`filter()` after `agg()`) | `having_filter` |
| WHERE (before `groupBy`) vs HAVING (after) | `where_then_aggregate` |
| Same report in Spark SQL with a real `HAVING` clause | `department_stats_sql` |
| Transformations vs actions, shuffle partitions, `explain()`, `cache()` timing | `src/main.py` sections 10-12 |

## Project structure

```
Day16-Spark-Aggregations/
├── data/hospital_revenue.csv     # 47 sample visits (6 departments x 3 branches)
├── src/
│   ├── session.py                # SparkSession factory (local[*], 4 shuffle partitions, AQE off)
│   ├── aggregations.py           # schema, loader and all aggregation functions
│   └── main.py                   # runnable demo, prints 12 sections and writes CSV reports
├── tests/                        # pytest checks against a plain-Python oracle
├── requirements.txt
├── pytest.ini
└── .gitignore
```

## Dataset

`data/hospital_revenue.csv`

| column | type | description |
|---|---|---|
| visit_id | string | unique visit-batch id |
| visit_date | date | August 2026 |
| branch | string | North / South / East |
| department | string | Cardiology, Orthopedics, Neurology, Pediatrics, Emergency, Radiology |
| service_type | string | Consultation / Procedure / Diagnostics |
| patients | int | patients covered by the record |
| revenue | double | billed revenue |
| cost | double | operating cost |

`profit = revenue - cost` is derived at load time.

## Execution steps

Requirements: Python 3.9+, Java 17 or 21 (needed by Spark).

```bash
# 1. Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run the demo from the project root
python -m src.main

# 4. Run the tests
pytest -v
```

CSV reports are written to `output/` (git-ignored).

## Expected department result (section 2)

| department | visit_count | total_revenue | avg_revenue | min_revenue | max_revenue |
|---|---|---|---|---|---|
| Cardiology | 9 | 52,418.80 | 5,824.31 | 1,841.00 | 10,521.60 |
| Neurology | 8 | 33,218.40 | 4,152.30 | 866.00 | 8,432.20 |
| Orthopedics | 8 | 28,350.70 | 3,543.84 | 1,033.50 | 8,065.80 |
| Emergency | 8 | 25,114.60 | 3,139.32 | 738.40 | 5,218.70 |
| Radiology | 8 | 8,609.10 | 1,076.14 | 270.00 | 2,990.40 |
| Pediatrics | 6 | 3,991.40 | 665.23 | 144.00 | 1,346.40 |

With `total_revenue >= 20000` (the HAVING-like step) only Cardiology, Neurology, Orthopedics and Emergency remain.

## Key takeaways

- **HAVING = `filter()` after `agg()`.** Filtering the aggregated DataFrame on `total_revenue` or `visit_count` is the DataFrame equivalent of SQL `HAVING`. Filtering raw rows before `groupBy` is `WHERE`, and it shrinks the data that gets shuffled.
- **`groupBy` is a wide transformation.** The plan shows `HashAggregate -> Exchange hashpartitioning -> HashAggregate` (partial aggregation, shuffle, final aggregation). The post-shuffle partition count follows `spark.sql.shuffle.partitions`.
- **Nothing runs until an action.** Building the aggregation is instant; `count()`, `show()` or `collect()` triggers the job.
- **Cache when reusing.** Several reports over the same DataFrame benefit from `cache()` on large inputs; on this tiny dataset the timing difference is small.
