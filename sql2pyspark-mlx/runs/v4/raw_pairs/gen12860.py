from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 4107000, "high").when(F.col("budget") >= 946000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(F.stddev("budget"), 2).alias("stddev_budget"),
        F.sum(F.when(F.col("location") == "Singapore", F.col("budget")).otherwise(0)).alias("singapore_budget"),
        F.count("budget").alias("budget_count"),
    )
    .filter(F.col("budget_count") >= 5)
    .drop("budget_count")
)
