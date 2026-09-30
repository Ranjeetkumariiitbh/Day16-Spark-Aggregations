"""SparkSession factory for the Day 16 aggregation exercises."""
from pyspark.sql import SparkSession


def create_spark(app_name: str = "Day16-Spark-Aggregations", shuffle_partitions: int = 4) -> SparkSession:
    """Create a local SparkSession tuned for a small demo dataset.

    - shuffle partitions are lowered from the default 200 to keep the
      groupBy shuffle small and easy to reason about.
    - Adaptive Query Execution is disabled so the shuffle partition count
      shown in the demo is the one we configured (AQE would coalesce it).
    """
    spark = (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
