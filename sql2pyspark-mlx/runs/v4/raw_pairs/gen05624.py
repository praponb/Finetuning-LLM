from pyspark.sql import functions as F

matched_keys = (
    df_departments
    .select("dept_name", "location")
    .exceptAll(
        df_departments
        .filter(F.col("budget").between(1806000, 2370000))
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
    .groupBy("location")
    .agg(
        F.stddev_samp("budget").alias("budget_stddev"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.round(F.sum("budget") / F.nullif(F.count(F.when(F.col("dept_name") == "Marketing", 1)), F.lit(0)), 2).alias("budget_per_marketing"),
    )
)
