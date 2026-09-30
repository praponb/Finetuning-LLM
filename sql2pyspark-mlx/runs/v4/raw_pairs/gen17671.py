from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .filter(F.col("emp.manager_id") == 5)
    .select(
        F.col("emp.full_name").alias("employee_full_name"),
        F.col("mgr.manager_id").alias("manager_manager_id"),
    )
)
