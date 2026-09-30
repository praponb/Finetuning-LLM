from pyspark.sql import functions as F

kept_keys = (
    df_departments
    .select("dept_name")
    .intersect(
        df_departments
        .filter(F.col("dept_name").isin(["Marketing", "Engineering", "Legal"]))
        .select("dept_name")
    )
)

verified = (
    df_departments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.dept_name") == F.col("k.dept_name")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        F.when(F.col("budget") >= 4033000, "HIGH").otherwise("LOW").alias("budget_tier"),
    )
    .agg(
        F.stddev_samp("budget").alias("budget_stddev"),
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        F.count(F.when(F.col("location") == "Tokyo", 1)).alias("tokyo_count"),
        F.sum(F.when(F.col("location") == "London", F.col("budget")).otherwise(0)).alias("london_budget"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("num_dept_names") > 1)
    .orderBy(F.col("event_count").desc_nulls_first())
)
