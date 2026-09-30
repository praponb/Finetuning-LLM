from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") < 3084000)
    .groupBy(
        F.when(F.col("budget") >= 3172000, "high").when(F.col("budget") >= 622000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.max("budget").alias("max_budget"),
        F.avg("budget").alias("avg_budget"),
    )
)
