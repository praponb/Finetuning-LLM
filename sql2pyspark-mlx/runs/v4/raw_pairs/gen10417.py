from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 4504000, "PREMIUM").otherwise("STANDARD").alias("budget_tier"),
    )
    .agg(
        F.max(F.when(F.col("dept_name") == "Marketing", F.col("budget"))).alias("marketing_max_budget"),
        F.percentile_approx("budget", 0.95).alias("budget_p95"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("budget_p95") < 0)
)
