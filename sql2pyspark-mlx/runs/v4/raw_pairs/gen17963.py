from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4143000, "high").when(F.col("budget") >= 1224000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
        (F.max("budget") - F.min("budget")).alias("budget_range"),
    )
)
