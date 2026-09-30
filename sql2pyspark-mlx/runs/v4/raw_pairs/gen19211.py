from pyspark.sql import functions as F

remaining_keys = (
    df_employees
    .select("job_title", "id")
    .subtract(
        df_employees
        .filter(F.col("dept_id") == 40)
        .select("job_title", "id")
    )
)

kept_rows = (
    df_employees
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.job_title") == F.col("k.job_title")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("full_name", "email", "manager_id", "hire_date")
    .orderBy("manager_id")
)
