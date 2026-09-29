from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") == "Finance")
    .groupBy(
        F.when(F.col("budget") >= 3708000, "high").when(F.col("budget") >= 1486000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.round(F.stddev("budget"), 2).alias("stddev_budget"))
    .filter(F.col("stddev_budget") >= 63)
    .select("budget_bucket", "stddev_budget")
    .orderBy("stddev_budget", F.col("budget_bucket").desc())
    .limit(20)
)
