from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4413000, "high").when(F.col("budget") >= 1446000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count("budget").alias("budget_count"),
        F.round(((100 * F.sum(F.when(F.col("location") == "New York", 1).otherwise(0))) / F.count("*")), 1).alias("pct_new_york"),
    )
    .filter(F.col("pct_new_york") >= 52)
    .orderBy("budget_bucket")
)
