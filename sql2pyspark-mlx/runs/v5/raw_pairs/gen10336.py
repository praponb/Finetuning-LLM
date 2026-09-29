from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .select(
        F.col("emp.email").alias("employee_email"),
        F.col("mgr.manager_id").alias("manager_manager_id"),
        F.col("mgr.is_remote").alias("manager_is_remote"),
    )
)
