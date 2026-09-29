from pyspark.sql import functions as F

matched_keys = (
    df_employees
    .select("level", "job_title")
    .subtract(
        df_employees
        .filter(~F.col("email").like("%@startup.io"))
        .select("level", "job_title")
    )
)

kept_rows = (
    df_employees
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.level") == F.col("k.level")) & (F.col("s.job_title") == F.col("k.job_title")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "is_remote",
        "level",
    )
)
