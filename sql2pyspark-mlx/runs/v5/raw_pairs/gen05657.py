from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter((F.col("emp.job_title") == "Analyst") & F.col("emp.is_remote"))
    .select(
        F.col("emp.id"),
        F.col("emp.dept_id").alias("employee_dept_id"),
        F.col("emp.hire_date").alias("employee_hire_date"),
        F.col("mgr.dept_id").alias("manager_dept_id"),
    )
)
