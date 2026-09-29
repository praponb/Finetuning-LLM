from pyspark.sql import functions as F

valid = (
    df_employees
    .filter(F.col("hire_date") >= F.lit("2023-06-22").cast("date"))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "level",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
        F.date_trunc("MONTH", "hire_date").alias("hire_date_period"),
        F.coalesce("job_title", F.lit("UNKNOWN")).alias("job_title_filled"),
    )
)

result = (
    valid
    .select("job_title", "level")
    .subtract(
        df_employees
        .filter(F.lower(F.trim("full_name")) == "jack taylor")
        .select("job_title", "level")
    )
)
