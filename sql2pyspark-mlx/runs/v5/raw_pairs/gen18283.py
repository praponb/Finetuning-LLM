from pyspark.sql import functions as F

valid = (
    df_employees
    .filter(F.col("is_remote"))
    .select(
        "id",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
        F.regexp_replace("email", " ", "-").alias("email_slug"),
    )
)

result = (
    valid
    .alias("e")
    .join(df_employees.alias("d"), F.col("e.manager_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.manager_id",
        "e.job_title",
        "e.salary",
        "e.hire_date",
        "e.is_remote",
        "e.level",
        "e.email_slug",
        F.col("d.salary").alias("employee_salary"),
        F.col("d.hire_date").alias("employee_hire_date"),
    )
)
