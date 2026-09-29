from pyspark.sql import functions as F

base = df_departments.filter(F.col("dept_name") == "HR")

departments_stats = (
    base
    .groupBy(
        F.when(F.col("budget") >= 4151000, "high").when(F.col("budget") >= 1899000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count(F.when(F.col("location") == "Chiang Mai", 1)).alias("chiang_mai_count"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
    )
)

result = (
    departments_stats
    .select("budget_bucket", "chiang_mai_count")
    .orderBy("budget_bucket")
    .limit(50)
)
