from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("e.is_remote"))
    .select(
        F.col("e.id"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("e.email").alias("employee_email"),
        F.col("m.manager_id").alias("manager_manager_id"),
        F.col("m.hire_date").alias("manager_hire_date"),
        F.col("m.job_title").alias("manager_job_title"),
    )
)
