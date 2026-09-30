from pyspark.sql import functions as F

matched_keys = (
    df_departments
    .select("dept_name", "location")
    .subtract(
        df_departments
        .filter(F.col("location") == "Bangkok")
        .select("dept_name", "location")
    )
)

reconciled = (
    df_departments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")) & (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("budget") >= 4161000, "HIGH").otherwise("LOW").alias("budget_tier"),
    )
    .agg(
        F.stddev_samp("budget").alias("budget_stddev"),
        F.countDistinct("dept_name").alias("distinct_dept_names"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
    )
)
