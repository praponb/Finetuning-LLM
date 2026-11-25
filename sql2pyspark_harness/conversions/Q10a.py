"""Q10a — UNPIVOT with value aliases (reference conversion).

DataFrame.unpivot() (Spark 3.4+) has no alias list and no EXCLUDE NULLS, so the
columns are renamed to the SQL aliases first and nulls are filtered afterwards.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def build(spark: SparkSession) -> DataFrame:
    m = F.month("order_ts")
    monthly = (
        spark.table("orders")
        .groupBy("customer_id")
        .agg(
            F.sum(F.when(m.between(1, 3), 1).otherwise(0)).alias("Q1"),
            F.sum(F.when(m.between(4, 6), 1).otherwise(0)).alias("Q2"),
            F.sum(F.when(m.between(7, 9), 1).otherwise(0)).alias("Q3"),
        )
    )
    return (
        monthly.unpivot("customer_id", ["Q1", "Q2", "Q3"], "quarter", "n_orders")
        .where(F.col("n_orders").isNotNull())   # EXCLUDE NULLS
        .where(F.col("n_orders") > 0)
        .orderBy("customer_id", "quarter")
    )
