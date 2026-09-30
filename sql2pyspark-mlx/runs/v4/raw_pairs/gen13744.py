from pyspark.sql import functions as F

base = (
    df_departments
    .filter((F.col("location") != "Singapore") & F.col("location").isin(["New York", "Bangkok"]))
)

departments_stats = (
    base
    .groupBy(
        F.when(F.col("budget") >= 3168000, "high").when(F.col("budget") >= 1436000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.avg("budget").alias("avg_budget"),
        F.max("budget").alias("max_budget"),
    )
)

result = (
    departments_stats
    .filter(F.col("avg_budget") < 2419000)
    .select("budget_bucket", "avg_budget", "max_budget")
    .orderBy("avg_budget", "budget_bucket")
)
