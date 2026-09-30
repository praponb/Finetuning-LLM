from pyspark.sql import functions as F

recent_departments = (
    df_departments
    .filter(~F.col("location").isin(["London", "Chiang Mai", "Singapore"]))
)

departments_stats = (
    recent_departments
    .groupBy(
        "location",
        F.when(F.col("budget") >= 4321000, "high").when(F.col("budget") >= 1783000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("location") == "Bangkok", 1).otherwise(0))) / F.count("*")), 1).alias("pct_bangkok"),
    )
)

result = (
    departments_stats
    .filter(F.col("pct_bangkok") > 22)
    .select("location", "budget_bucket", "pct_bangkok")
    .orderBy("location", "budget_bucket")
    .limit(20)
)
