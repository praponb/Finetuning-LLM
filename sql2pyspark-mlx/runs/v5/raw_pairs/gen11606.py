from pyspark.sql import functions as F

kept = (
    df_departments
    .alias("s")
    .join(df_employees.alias("x"), (F.col("x.dept_id") == F.col("s.id")) & (F.col("x.job_title") == "Director"), "left_anti")
)

result = (
    kept
    .withColumn("budget_tier", F.when(F.col("budget") >= 3218000, "PREMIUM").otherwise("STANDARD"))
    .rollup("location", "budget_tier")
    .agg(
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.percentile_approx("budget", 0.5).alias("budget_p50"),
        F.count(F.when(F.col("location") == "New York", 1)).alias("new_york_rows"),
        F.countDistinct("dept_name").alias("distinct_dept_names"),
        F.count(F.when(F.col("dept_name") == "HR", 1)).alias("hr_rows"),
        F.grouping_id().alias("grouping_level"),
    )
)
