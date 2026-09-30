from pyspark.sql import functions as F

remaining_keys = (
    df_employees
    .select("manager_id", "id")
    .subtract(
        df_employees
        .filter(~F.col("job_title").isin(["Designer", "Analyst"]))
        .select("manager_id", "id")
    )
)

verified = (
    df_employees
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("id", "email", "dept_id", "job_title", "salary", "hire_date", "level")
    .orderBy(F.col("id").desc_nulls_last())
)
