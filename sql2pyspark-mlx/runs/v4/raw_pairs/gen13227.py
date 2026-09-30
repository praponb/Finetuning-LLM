from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4241000, "PREMIUM").otherwise("STANDARD").alias("budget_tier"),
    )
    .agg(
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
        F.sum(F.when(F.col("dept_name") == "Sales", F.col("budget")).otherwise(0)).alias("sales_budget"),
        F.count(F.when(F.col("location") == "Singapore", 1)).alias("singapore_rows"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
    )
)
