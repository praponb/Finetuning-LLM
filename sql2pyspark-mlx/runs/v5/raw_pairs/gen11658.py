from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(
        F.percentile_approx("budget", 0.95).alias("budget_p95"),
        F.count_if(F.col("dept_name") == "Finance").alias("finance_events"),
        F.stddev_samp("budget").alias("budget_stddev"),
    )
    .orderBy("budget_p95")
)
