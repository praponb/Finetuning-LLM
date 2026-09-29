from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 3723000, "high").when(F.col("budget") >= 1935000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.count(F.when(F.col("dept_name") == "Finance", 1)).alias("finance_count"),
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
    )
    .orderBy("stddev_budget")
)
