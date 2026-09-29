from pyspark.sql import functions as F

kept_keys = (
    df_departments
    .select("id", "dept_name")
    .exceptAll(
        df_departments
        .filter(F.col("budget").between(1088000, 1329000))
        .select("id", "dept_name")
    )
)

verified = (
    df_departments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.dept_name") == F.col("k.dept_name")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("location")
    .agg(
        F.stddev_samp("budget").alias("budget_stddev"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
    )
)
