from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.col("emp.job_title").isNotNull())
    .select(
        F.col("emp.dept_id").alias("employee_dept_id"),
        F.col("mgr.dept_id").alias("manager_dept_id"),
        F.col("mgr.level").alias("manager_level"),
    )
)
