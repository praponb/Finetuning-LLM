from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.col("mgr.full_name").isNotNull())
    .select(
        F.col("emp.full_name").alias("employee_full_name"),
        F.col("mgr.dept_id").alias("manager_dept_id"),
        F.col("mgr.is_remote").alias("manager_is_remote"),
        F.col("mgr.full_name").alias("manager_full_name"),
    )
)
