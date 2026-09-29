from pyspark.sql import functions as F

valid = (
    df_employees
    .filter(F.col("hire_date") > F.lit("2023-04-04").cast("date"))
    .select(
        "*",
        F.datediff(F.current_date(), "hire_date").alias("days_since_hire_date"),
    )
)

result = (
    valid
    .alias("e")
    .join(df_employees.alias("d"), F.col("e.manager_id") == F.col("d.id"), "left")
    .filter(F.col("d.hire_date").between("2024-04-23", "2024-07-07"))
    .select(
        "e.id",
        "e.full_name",
        "e.email",
        "e.dept_id",
        "e.manager_id",
        "e.salary",
        "e.is_remote",
        "e.level",
        "e.days_since_hire_date",
        F.col("d.job_title").alias("employee_job_title"),
    )
)
