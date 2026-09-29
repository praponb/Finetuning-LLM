from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") > 3949000)
    .groupBy(
        F.when(F.col("budget") >= 3281000, "high").when(F.col("budget") >= 1938000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        (F.max("budget") - F.min("budget")).alias("budget_range"),
    )
)
