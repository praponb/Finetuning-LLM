from pyspark.sql import functions as F

kept_keys = (
    df_employees
    .select("level", "manager_id")
    .subtract(
        df_employees
        .filter(F.col("email").isNotNull())
        .select("level", "manager_id")
    )
)

kept_rows = (
    df_employees
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.manager_id") == F.col("k.manager_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "level",
    )
    .orderBy(F.col("level").desc_nulls_first())
)
