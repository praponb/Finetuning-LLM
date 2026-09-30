from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~(F.col("dept_name").isNotNull() & (F.col("budget") <= 2258000)))
    .groupBy(
        F.when(F.col("budget") >= 3181000, "high").when(F.col("budget") >= 1826000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count("*").alias("total_count"),
        F.avg("budget").alias("avg_budget"),
        F.round(((100 * F.sum(F.when(F.col("location") == "Berlin", 1).otherwise(0))) / F.count("*")), 1).alias("pct_berlin"),
    )
    .filter(F.col("pct_berlin") > 69)
)
