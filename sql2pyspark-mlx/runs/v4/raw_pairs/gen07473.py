from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .select(
        F.col("emp.level").alias("employee_level"),
        F.col("emp.hire_date").alias("employee_hire_date"),
        F.col("mgr.job_title").alias("manager_job_title"),
        F.col("mgr.is_remote").alias("manager_is_remote"),
        F.col("mgr.dept_id").alias("manager_dept_id"),
        F.trim(F.col("emp.level")).alias("clean_level"),
    )
    .orderBy(F.col("manager_dept_id").desc())
)
