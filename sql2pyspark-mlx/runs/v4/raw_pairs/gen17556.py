from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "Singapore")
    .groupBy(
        F.when(F.col("budget") >= 3278000, "high").when(F.col("budget") >= 1484000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("location") == "New York", F.col("budget")).otherwise(0)).alias("new_york_budget"),
        F.round(((100 * F.sum(F.when(F.col("location") == "Berlin", 1).otherwise(0))) / F.count("*")), 1).alias("pct_berlin"),
    )
    .orderBy("pct_berlin")
)
