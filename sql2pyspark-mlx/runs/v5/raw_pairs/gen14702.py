from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .filter(F.col("mgr.is_remote") | F.col("emp.salary").between(105000, 112000))
    .select(
        F.col("emp.manager_id").alias("employee_manager_id"),
        F.col("emp.hire_date").alias("employee_hire_date"),
        F.col("emp.dept_id").alias("employee_dept_id"),
        F.col("mgr.salary").alias("manager_salary"),
        F.col("mgr.dept_id").alias("manager_dept_id"),
        F.col("mgr.full_name").alias("manager_full_name"),
        F.upper(F.col("mgr.job_title")).alias("job_title_upper"),
    )
)
