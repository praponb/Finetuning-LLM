from pyspark.sql import functions as F

filtered_departments = df_departments.filter(F.col("dept_name") != "Legal")

summary = (
    filtered_departments
    .groupBy(
        F.when(F.col("budget") >= 4484000, "high").when(F.col("budget") >= 1144000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum("budget").alias("total_budget"),
        F.round(((100 * F.sum(F.when(F.col("dept_name") == "Marketing", 1).otherwise(0))) / F.count("*")), 1).alias("pct_marketing"),
    )
)

result = (
    summary
    .select("budget_bucket", "total_budget", "pct_marketing")
    .orderBy("pct_marketing", "budget_bucket")
    .limit(5)
)
