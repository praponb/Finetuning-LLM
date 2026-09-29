from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.col("emp.level").isin(["L5", "L2", "L4"]))
    .select(
        F.col("emp.id"),
        F.col("emp.job_title").alias("employee_job_title"),
        F.col("mgr.manager_id").alias("manager_manager_id"),
        F.col("mgr.dept_id").alias("manager_dept_id"),
        F.col("mgr.level").alias("manager_level"),
    )
)
