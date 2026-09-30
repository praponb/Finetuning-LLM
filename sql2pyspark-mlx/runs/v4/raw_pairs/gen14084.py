from pyspark.sql import functions as F

remaining_keys = (
    df_departments
    .select("dept_name", "id")
    .intersect(
        df_departments
        .filter(F.col("location") == "Chiang Mai")
        .select("dept_name", "id")
    )
)

verified = (
    df_departments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("budget") >= 3395000, "HIGH").otherwise("LOW").alias("budget_tier"),
    )
    .agg(
        F.stddev_samp("budget").alias("budget_stddev"),
        F.count_if(F.col("location") == "Tokyo").alias("tokyo_events"),
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
        F.percentile_approx("budget", 0.95).alias("budget_p95"),
        F.percentile_approx("budget", 0.95).alias("budget_p95_2"),
        F.size(F.collect_set("location")).alias("num_locations"),
    )
    .orderBy("budget_tier")
)
