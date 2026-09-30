from pyspark.sql import functions as F

with_dims = (
    df_employees
    .alias("a")
    .join(df_employees.alias("d"), F.col("a.manager_id") == F.col("d.id"), "inner")
    .select(
        "a.*",
        F.col("d.level").alias("employee_level"),
        F.col("d.job_title").alias("employee_job_title"),
        F.col("d.salary").alias("employee_salary"),
    )
)

result = (
    with_dims
    .select(
        "id",
        "email",
        "job_title",
        "is_remote",
        "employee_level",
        "employee_job_title",
    )
    .orderBy(F.col("employee_level").asc_nulls_last(), "job_title")
)
