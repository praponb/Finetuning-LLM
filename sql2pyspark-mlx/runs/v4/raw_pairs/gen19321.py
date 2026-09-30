from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").isNull())
    .groupBy(
        F.when(F.col("budget") >= 3798000, "high").when(F.col("budget") >= 1542000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("location") == "New York", F.col("budget")).otherwise(0)).alias("new_york_budget"),
        F.count("budget").alias("budget_count"),
        F.round(((100 * F.sum(F.when(F.col("location") == "Singapore", 1).otherwise(0))) / F.count("*")), 1).alias("pct_singapore"),
    )
)
