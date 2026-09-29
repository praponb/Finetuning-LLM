from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.level") != "L1")
    .select(
        F.col("e.id"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("m.full_name").alias("manager_full_name"),
    )
)
