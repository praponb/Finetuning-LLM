from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .select(
        F.col("e.email").alias("employee_email"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("e.job_title").alias("employee_job_title"),
        F.col("m.hire_date").alias("manager_hire_date"),
        F.lower(F.col("m.level")).alias("level_lower"),
    )
)
