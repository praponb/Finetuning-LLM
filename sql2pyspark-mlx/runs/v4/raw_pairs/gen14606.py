from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(1537000, 1692000))
    .groupBy(
        F.when(F.col("budget") >= 3191000, "high").when(F.col("budget") >= 1849000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum("budget").alias("total_budget"),
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
    )
    .filter(F.col("total_budget") < 18574000)
    .select("budget_bucket", "total_budget", "stddev_budget")
    .orderBy(F.col("total_budget").desc())
)
