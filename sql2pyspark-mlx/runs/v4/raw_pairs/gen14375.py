from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .select(
        F.col("e.id"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("m.full_name").alias("manager_full_name"),
        F.col("m.dept_id").alias("manager_dept_id"),
        F.col("m.job_title").alias("manager_job_title"),
    )
)
