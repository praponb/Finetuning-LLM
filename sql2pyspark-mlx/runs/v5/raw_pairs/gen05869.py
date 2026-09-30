from pyspark.sql import functions as F

eligible = (
    df_departments
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.hire_date").between("2024-05-23", "2025-10-01")), "left_semi")
)

result = (
    eligible
    .groupBy(
        F.when(F.col("budget") >= 3989000, "LARGE").otherwise("SMALL").alias("budget_tier"),
        "location",
    )
    .agg(
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
        F.stddev_samp("budget").alias("budget_stddev"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
    )
    .filter(F.col("avg_budget") >= 0)
    .orderBy(F.col("num_rows").asc_nulls_first())
)
