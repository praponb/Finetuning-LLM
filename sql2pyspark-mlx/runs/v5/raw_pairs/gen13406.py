from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_employees.alias("m"), F.col("e.manager_id") == F.col("m.id"), "inner")
    .filter(F.col("e.salary") <= 102000)
    .select(
        F.col("e.id"),
        F.col("e.salary").alias("employee_salary"),
        F.col("e.is_remote").alias("employee_is_remote"),
        F.col("m.manager_id").alias("manager_manager_id"),
        F.col("m.is_remote").alias("manager_is_remote"),
        F.col("m.hire_date").alias("manager_hire_date"),
    )
)
