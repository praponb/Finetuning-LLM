from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4309000, "high").when(F.col("budget") >= 1423000, "mid").otherwise("low").alias("budget_bucket"),
        "dept_name",
        "location",
    )
    .agg(
        F.avg("budget").alias("avg_budget"),
        F.count("budget").alias("budget_count"),
    )
    .orderBy("avg_budget", "location", "budget_bucket", "dept_name")
    .limit(10)
)
