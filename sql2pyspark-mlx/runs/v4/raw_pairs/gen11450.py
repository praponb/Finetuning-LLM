from pyspark.sql import functions as F

base = (
    df_departments
    .filter(F.col("dept_name") == "HR")
    .withColumn("location_group", F.when(F.col("location").isin(["Chiang Mai", "Berlin"]), "Group A").otherwise("Other"))
)

departments_stats = (
    base
    .groupBy(
        F.when(F.col("budget") >= 4238000, "high").when(F.col("budget") >= 1678000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum("budget").alias("total_budget"),
        F.min("budget").alias("min_budget"),
    )
)

result = departments_stats.select("budget_bucket", "total_budget", "min_budget")
