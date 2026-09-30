from pyspark.sql import functions as F

valid_departments = df_departments.filter(F.col("budget").between(1597000, 3697000))

summary = (
    valid_departments
    .groupBy(
        "location",
        F.when(F.col("budget") >= 4246000, "high").when(F.col("budget") >= 950000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.min("budget").alias("min_budget"))
)

result = (
    summary
    .select("location", "budget_bucket")
    .orderBy(F.col("location").desc(), "budget_bucket")
    .limit(20)
)
