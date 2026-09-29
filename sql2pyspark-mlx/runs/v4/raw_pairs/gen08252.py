from pyspark.sql import functions as F

remaining_keys = (
    df_employees
    .select("id")
    .subtract(
        df_employees
        .filter(F.col("full_name").like("%Mia%"))
        .select("id")
    )
)

reconciled = (
    df_employees
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("full_name", "email", "dept_id", "job_title", "level")
    .orderBy(F.col("job_title").desc_nulls_first(), "dept_id")
)
