from pyspark.sql import functions as F

kept_keys = (
    df_employees
    .select("job_title")
    .subtract(
        df_employees
        .filter(F.col("dept_id").isin([14, 19, 20, 25]))
        .select("job_title")
    )
)

kept_rows = (
    df_employees
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.job_title") == F.col("k.job_title")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("full_name", "email", "dept_id", "manager_id", "salary", "level")
    .orderBy(F.col("level").desc(), "manager_id")
)
