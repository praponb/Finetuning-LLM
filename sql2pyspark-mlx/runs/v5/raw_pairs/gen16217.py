from pyspark.sql import functions as F

result = (
    df_employees
    .alias("a")
    .join(df_employees.alias("d"), F.col("a.manager_id") == F.col("d.id"), "left")
    .filter(F.col("d.job_title") == "Intern")
    .select(
        "a.id",
        "a.manager_id",
        "a.level",
        F.col("d.salary").alias("employee_salary"),
        F.col("d.level").alias("employee_level"),
        F.col("d.email").alias("employee_email"),
    )
)
