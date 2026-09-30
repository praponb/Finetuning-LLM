from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "inner")
    .select(
        F.col("emp.level").alias("employee_level"),
        F.col("emp.full_name").alias("employee_full_name"),
        F.col("emp.salary").alias("employee_salary"),
        F.col("mgr.manager_id").alias("manager_manager_id"),
        F.col("mgr.salary").alias("manager_salary"),
        F.lower(F.col("emp.level")).alias("level_lower"),
    )
)
