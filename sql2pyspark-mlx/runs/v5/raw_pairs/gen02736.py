from pyspark.sql import functions as F

src = df_departments.filter((F.col("budget") <= 2220000) & (F.col("budget") < 1354000))

departments_stats = (
    src
    .groupBy(
        F.when(F.col("budget") >= 3343000, "high").when(F.col("budget") >= 1976000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("location") == "Chiang Mai", 1).otherwise(0)).alias("chiang_mai_count"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
    )
)

result = departments_stats
