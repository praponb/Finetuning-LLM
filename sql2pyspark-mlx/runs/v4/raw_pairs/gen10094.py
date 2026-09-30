from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .filter(F.col("e.email").like("%@outlook.com"))
    .select(
        F.col("e.id"),
        F.col("e.level").alias("employee_level"),
        F.col("m.is_remote").alias("manager_is_remote"),
        F.col("m.full_name").alias("manager_full_name"),
    )
)
