from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4228000, "high").when(F.col("budget") >= 732000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.avg("budget").alias("avg_budget"),
        F.sum("budget").alias("total_budget"),
    )
    .filter(F.col("avg_budget") > 2184000)
    .orderBy("budget_bucket")
)
