from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.length(F.col("m.email")) < 8)
    .select(
        F.col("e.id"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("e.dept_id").alias("employee_dept_id"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("m.manager_id").alias("manager_manager_id"),
        F.col("m.hire_date").alias("manager_hire_date"),
        F.col("m.level").alias("manager_level"),
    )
)
