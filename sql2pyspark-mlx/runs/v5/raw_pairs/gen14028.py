from pyspark.sql import functions as F

matched_keys = (
    df_employees
    .select("id")
    .exceptAll(
        df_employees
        .filter(F.col("level").isin(["L1", "L3"]))
        .select("id")
    )
)

kept_rows = (
    df_employees
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .cube(
        "job_title",
        F.date_trunc("week", "hire_date").alias("week_start"),
        "manager_id",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("salary", [0.25, 0.5, 0.75]).alias("salary_quartiles"),
        F.count_if(F.col("level") == "L5").alias("l5_events"),
    )
    .orderBy(F.col("manager_id").desc())
)
