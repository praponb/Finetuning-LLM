from pyspark.sql import functions as F

base = (
    df_employees
    .filter(F.col("job_title").isNotNull() & (F.col("hire_date") > F.lit("2023-05-18").cast("date")))
    .select(
        "id",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "is_remote",
        F.concat_ws("-", "full_name", "email").alias("full_name_email_key"),
        F.coalesce("job_title", F.lit("DIRECT")).alias("job_title_filled"),
    )
)

result = (
    base
    .select("dept_id", "manager_id")
    .intersect(
        df_employees
        .filter(F.col("level").isNotNull())
        .select("dept_id", "manager_id")
    )
)
