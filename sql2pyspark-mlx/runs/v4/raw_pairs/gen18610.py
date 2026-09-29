from pyspark.sql import functions as F

kept = (
    df_departments
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.salary").between(64000, 112000)), "left_anti")
)

result = (
    kept
    .groupBy(
        F.when(F.col("budget") >= 4063000, "LARGE").otherwise("SMALL").alias("budget_tier"),
    )
    .agg(
        F.sum(F.when(F.col("location") == "Bangkok", F.col("budget"))).alias("bangkok_total_budget"),
        F.min("budget").alias("min_budget"),
        F.percentile_approx("budget", 0.9).alias("budget_p90"),
        F.stddev_samp("budget").alias("budget_stddev"),
    )
)
