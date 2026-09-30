from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("e.hire_date").between("2023-09-18", "2025-07-29"))
    .select(
        F.col("e.id"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("e.email").alias("employee_email"),
        F.col("m.is_remote").alias("manager_is_remote"),
        F.col("m.job_title").alias("manager_job_title"),
        F.col("m.dept_id").alias("manager_dept_id"),
    )
    .orderBy(F.col("id").desc(), "employee_email")
)
