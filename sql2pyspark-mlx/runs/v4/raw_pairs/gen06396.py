from pyspark.sql import functions as F

recent_departments = (
    df_departments
    .filter(((F.col("dept_name") != "Engineering") & (F.col("location") == "Singapore")) | (F.col("location") == "Tokyo"))
)

summary = (
    recent_departments
    .groupBy(
        F.when(F.col("budget") >= 4351000, "high").when(F.col("budget") >= 1577000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.avg("budget").alias("avg_budget"),
        F.max("budget").alias("max_budget"),
    )
)

result = (
    summary
    .select("budget_bucket", "avg_budget")
    .orderBy(F.col("budget_bucket").desc())
)
