from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3902000, "high").when(F.col("budget") >= 1057000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(
        F.min("budget").alias("min_budget"),
        F.max("budget").alias("max_budget"),
        F.avg("budget").alias("avg_budget"),
    )
    .filter(F.col("max_budget") >= 1613000)
)
