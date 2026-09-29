from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("m.dept_id") == 13)
    .select(
        F.col("e.job_title").alias("employee_job_title"),
        F.col("e.email").alias("employee_email"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("m.level").alias("manager_level"),
        F.col("m.full_name").alias("manager_full_name"),
    )
)
