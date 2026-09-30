from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("location").isin(["Bangkok", "Chiang Mai"]), "tier_1").otherwise("other").alias("location_group"),
        F.when(F.col("budget") >= 4243000, "high").when(F.col("budget") >= 739000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(F.avg("budget").alias("avg_budget"))
    .filter(F.col("avg_budget") > 1524000)
    .select("location_group", "budget_bucket", "avg_budget")
    .orderBy(F.col("budget_bucket").desc())
)
