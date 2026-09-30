from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") <= 1920000)
    .groupBy(
        "dept_name",
        F.when(F.col("budget") >= 3491000, "high").when(F.col("budget") >= 1121000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.min("budget").alias("min_budget"),
        F.count(F.when(F.col("location") == "Bangkok", 1)).alias("bangkok_count"),
    )
    .filter(F.col("bangkok_count") < 8)
    .select("dept_name", "budget_bucket", "min_budget", "bangkok_count")
    .orderBy(F.col("min_budget").desc(), F.col("budget_bucket").desc(), "dept_name")
    .limit(50)
)
