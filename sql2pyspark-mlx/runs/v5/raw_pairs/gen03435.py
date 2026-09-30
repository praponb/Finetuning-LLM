from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        "location",
        F.when(F.col("budget") >= 3549000, "high").when(F.col("budget") >= 760000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.count("budget").alias("budget_count"),
        F.round(((100 * F.sum(F.when(F.col("dept_name") == "Finance", 1).otherwise(0))) / F.count("*")), 1).alias("pct_finance"),
    )
    .orderBy("budget_bucket")
)
