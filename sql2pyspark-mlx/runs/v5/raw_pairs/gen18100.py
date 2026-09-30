from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("e.is_remote"))
    .select(
        F.col("e.id"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("e.salary").alias("employee_salary"),
        F.col("m.full_name").alias("manager_full_name"),
    )
)
