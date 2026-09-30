from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.level").isin(["L2", "L1", "L4"]))
    .select(
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("m.full_name").alias("manager_full_name"),
        F.col("m.level").alias("manager_level"),
    )
)
