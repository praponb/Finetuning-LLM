from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3151000, "high").when(F.col("budget") >= 981000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum("budget").alias("total_budget"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.min("budget").alias("min_budget"),
    )
    .orderBy(F.col("budget_bucket").desc())
    .limit(5)
)
