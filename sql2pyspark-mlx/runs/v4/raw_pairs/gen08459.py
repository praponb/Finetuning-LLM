from pyspark.sql import functions as F

remaining_keys = (
    df_employees
    .select("dept_id")
    .intersectAll(
        df_employees
        .filter(F.col("manager_id") == 19)
        .select("dept_id")
    )
)

kept_rows = (
    df_employees
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.dept_id") == F.col("k.dept_id")), "inner")
    .select("s.*")
)

result = kept_rows.select("full_name", "email", "job_title", "salary", "hire_date")
