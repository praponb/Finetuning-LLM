from pyspark.sql import functions as F

matched_keys = (
    df_employees
    .select("dept_id")
    .intersectAll(
        df_employees
        .filter(~F.col("is_remote"))
        .select("dept_id")
    )
)

reconciled = (
    df_employees
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.dept_id") == F.col("k.dept_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("full_name", "job_title", "salary", "hire_date", "is_remote")
    .orderBy(F.col("salary").desc())
)
