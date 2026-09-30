from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3640000, "high").when(F.col("budget") >= 1091000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count("budget").alias("budget_count"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") > 6)
    .select("budget_bucket", "budget_count", "cnt")
)
