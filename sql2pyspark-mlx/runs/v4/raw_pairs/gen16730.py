from pyspark.sql import functions as F

valid_departments = df_departments.filter(F.col("location") == "New York")

departments_stats = (
    valid_departments
    .groupBy(
        "location",
        F.when(F.col("budget") >= 3770000, "high").when(F.col("budget") >= 1053000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.max("budget").alias("max_budget"))
)

result = (
    departments_stats
    .filter(F.col("max_budget") > 1365000)
    .orderBy("budget_bucket", "max_budget")
)
