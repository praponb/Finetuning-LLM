from pyspark.sql import functions as F

kept_keys = (
    df_employees
    .select("job_title")
    .intersectAll(
        df_employees
        .filter(F.col("manager_id") == 23)
        .select("job_title")
    )
)

reconciled = (
    df_employees
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.job_title") == F.col("k.job_title")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(F.date_trunc("QUARTER", "hire_date").alias("quarter_start"))
    .agg(
        F.countDistinct("manager_id").alias("distinct_manager_ids"),
        F.count("*").alias("cnt"),
        F.count("*").alias("num_rows"),
    )
)
