from pyspark.sql import functions as F

recent_departments = df_departments.filter(F.col("location").isin(["Chiang Mai", "Singapore"]))

agg = (
    recent_departments
    .groupBy(
        F.when(F.col("budget") >= 4125000, "high").when(F.col("budget") >= 1889000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("location") == "Chiang Mai", F.col("budget")).otherwise(0)).alias("chiang_mai_budget"),
        F.avg("budget").alias("avg_budget"),
    )
)

result = (
    agg
    .filter(F.col("avg_budget") < 1834000)
    .select("budget_bucket", "chiang_mai_budget", "avg_budget")
    .orderBy("budget_bucket")
    .limit(10)
)
