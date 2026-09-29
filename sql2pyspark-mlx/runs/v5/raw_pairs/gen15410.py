from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        "location",
        F.when(F.col("budget") >= 4466000, "high").when(F.col("budget") >= 1076000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("location") == "Chiang Mai", 1).otherwise(0))) / F.count("*")), 1).alias("pct_chiang_mai"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.round(((100 * F.sum(F.when(F.col("location") == "New York", 1).otherwise(0))) / F.count("*")), 1).alias("pct_new_york"),
    )
    .filter(F.col("pct_new_york") < 76)
)
