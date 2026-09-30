from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .select(
        F.col("e.id"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("m.full_name").alias("manager_full_name"),
        F.col("m.level").alias("manager_level"),
    )
    .orderBy(F.col("manager_full_name").desc(), "manager_level")
)
