from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .select(
        F.col("emp.job_title").alias("employee_job_title"),
        F.col("emp.hire_date").alias("employee_hire_date"),
        F.col("emp.dept_id").alias("employee_dept_id"),
        F.col("mgr.hire_date").alias("manager_hire_date"),
        F.col("mgr.full_name").alias("manager_full_name"),
    )
)
