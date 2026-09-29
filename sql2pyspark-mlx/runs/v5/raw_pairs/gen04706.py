from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .select(
        F.col("e.id"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("m.hire_date").alias("manager_hire_date"),
        F.col("m.is_remote").alias("manager_is_remote"),
    )
)
