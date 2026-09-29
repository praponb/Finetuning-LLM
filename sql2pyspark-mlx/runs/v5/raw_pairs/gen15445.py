from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "left")
    .select(
        F.col("e.id"),
        F.col("e.manager_id").alias("employee_manager_id"),
        F.col("m.full_name").alias("manager_full_name"),
        F.col("m.salary").alias("manager_salary"),
        F.col("m.is_remote").alias("manager_is_remote"),
    )
    .orderBy(F.col("manager_salary").desc(), F.col("employee_manager_id").desc())
)
