from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 3813000, "high").when(F.col("budget") >= 906000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count("budget").alias("budget_count"),
        F.avg("budget").alias("avg_budget"),
        F.sum(F.when(F.col("location") == "London", F.col("budget")).otherwise(0)).alias("london_budget"),
    )
    .filter(F.col("london_budget") >= 30)
    .drop("london_budget")
    .orderBy(F.col("budget_count").desc())
)
