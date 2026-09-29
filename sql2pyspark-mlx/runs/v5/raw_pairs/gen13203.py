from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("m.level").isin(["L5", "L2"]))
    .select(
        F.col("e.id"),
        F.col("e.email").alias("employee_email"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("m.manager_id").alias("manager_manager_id"),
        F.col("m.email").alias("manager_email"),
    )
)
