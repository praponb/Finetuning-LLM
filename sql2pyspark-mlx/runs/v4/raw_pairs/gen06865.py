from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .filter(F.col("emp.manager_id").isNotNull() & F.col("emp.hire_date").between("2024-03-01", "2025-06-10"))
    .select(
        F.col("emp.level").alias("employee_level"),
        F.col("mgr.is_remote").alias("manager_is_remote"),
        F.col("mgr.full_name").alias("manager_full_name"),
    )
)
