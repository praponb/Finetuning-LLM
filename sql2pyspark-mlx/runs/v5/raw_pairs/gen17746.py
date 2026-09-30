from pyspark.sql import functions as F

recent_departments = df_departments.filter(F.col("budget").between(1340000, 1492000))

departments_stats = (
    recent_departments
    .groupBy(
        F.when(F.col("budget") >= 3519000, "high").when(F.col("budget") >= 1045000, "mid").otherwise("low").alias("budget_bucket"),
        F.when(F.col("location").isin(["London", "New York"]), "priority").otherwise("other").alias("location_group"),
    )
    .agg(F.min("budget").alias("min_budget"))
)

result = (
    departments_stats
    .select("budget_bucket", "location_group", "min_budget")
    .orderBy("min_budget", "budget_bucket", "location_group")
    .limit(20)
)
