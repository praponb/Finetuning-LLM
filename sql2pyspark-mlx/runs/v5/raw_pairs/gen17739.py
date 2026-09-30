from pyspark.sql import functions as F

kept_keys = (
    df_departments
    .select("id", "location")
    .exceptAll(
        df_departments
        .filter(F.col("budget") >= 2097000)
        .select("id", "location")
    )
)

verified = (
    df_departments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.location") == F.col("k.location")), "inner")
    .select("s.*")
)

result = (
    verified
    .cube(
        F.when(F.col("budget") >= 3910000, "HIGH").otherwise("LOW").alias("budget_tier"),
        "location",
    )
    .agg(
        F.size(F.collect_set("dept_name")).alias("num_dept_names"),
        F.percentile_approx("budget", 0.5).alias("budget_p50"),
    )
    .orderBy(F.col("location").asc_nulls_first())
)
