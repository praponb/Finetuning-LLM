from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "New York")
    .groupBy(
        F.when(F.col("budget") >= 3425000, "high").when(F.col("budget") >= 2054000, "mid").otherwise("low").alias("budget_bucket"),
        "location",
    )
    .agg(
        F.count(F.when(F.col("location") == "Berlin", 1)).alias("berlin_count"),
        F.max("budget").alias("max_budget"),
    )
    .filter(F.col("max_budget") >= 2138000)
    .select("budget_bucket", "location", "berlin_count", "max_budget")
    .orderBy(F.col("berlin_count").desc(), "budget_bucket")
)
