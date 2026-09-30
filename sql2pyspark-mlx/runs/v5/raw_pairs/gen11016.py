from pyspark.sql import functions as F

result = (
    df_employees.alias("emp")
    .join(df_employees.alias("mgr"), F.col("emp.manager_id") == F.col("mgr.id"), "left")
    .select(
        F.col("emp.full_name").alias("employee_full_name"),
        F.col("emp.hire_date").alias("employee_hire_date"),
        F.col("mgr.is_remote").alias("manager_is_remote"),
        F.col("mgr.level").alias("manager_level"),
        F.length(F.col("emp.full_name")).alias("full_name_length"),
    )
    .orderBy(F.col("employee_hire_date").desc())
)
