from pyspark.sql import functions as F

base = df_departments.filter(~F.col("location").isin(["Bangkok", "Chiang Mai"]))

summary = (
    base
    .groupBy(
        F.when(F.col("budget") >= 3252000, "high").when(F.col("budget") >= 1203000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count(F.when(F.col("location") == "Singapore", 1)).alias("singapore_count"),
        F.min("budget").alias("min_budget"),
    )
)

result = (
    summary
    .filter(F.col("singapore_count") > 7)
    .select("budget_bucket", "singapore_count", "min_budget")
    .orderBy(F.col("min_budget").desc(), "budget_bucket")
)
